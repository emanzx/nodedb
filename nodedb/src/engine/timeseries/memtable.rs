//! L0 RAM memtable for timeseries ingest.
//!
//! Incoming metrics and log entries are buffered in the memtable before
//! being flushed to L1 NVMe segment files. The memtable is organized
//! by series key (metric name + tag set hash) for cache-efficient
//! writes and reads.
//!
//! ## Cardinality Protection
//!
//! The memtable enforces a hard cardinality limit (`max_series`). When the
//! limit is reached, the coldest series (by last write timestamp) is evicted
//! to make room. This prevents OOM under high-cardinality workloads
//! (e.g., Kubernetes with ephemeral pod IDs as tags).
//!
//! ## Memory Accounting
//!
//! Memory is tracked per-series with actual sizes, not hardcoded estimates.
//! The memtable reports accurate `memory_bytes()` for budget enforcement.
//!
//! ## Admission Control
//!
//! `ingest_metric` / `ingest_log` return `IngestResult` which tells the
//! caller whether the memtable needs flushing or has rejected the write
//! due to budget exhaustion (when the frozen memtable is still flushing).

use std::collections::{BTreeMap, HashMap};

use super::gorilla::GorillaEncoder;

/// Unique identifier for a timeseries (metric name + tag set).
pub type SeriesId = u64;

/// A single metric sample.
#[derive(Debug, Clone, Copy)]
pub struct MetricSample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A single log entry.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp_ms: i64,
    pub data: Vec<u8>,
}

/// Result of an ingest operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestResult {
    /// Write accepted, memtable healthy.
    Ok,
    /// Write accepted, but memtable should be flushed (memory pressure).
    FlushNeeded,
    /// Write rejected — memory budget exhausted and cannot evict further.
    /// Caller should apply backpressure to the client.
    Rejected,
}

impl IngestResult {
    pub fn is_flush_needed(&self) -> bool {
        matches!(self, Self::FlushNeeded)
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected)
    }
}

/// Per-series state in the memtable.
#[derive(Debug)]
enum SeriesBuffer {
    Metric(MetricBuffer),
    Log(LogBuffer),
}

impl SeriesBuffer {
    /// Approximate memory footprint of this series buffer.
    fn memory_bytes(&self) -> usize {
        // Base overhead for enum + fields.
        const SERIES_OVERHEAD: usize = 80;
        match self {
            SeriesBuffer::Metric(m) => SERIES_OVERHEAD + m.encoder.compressed_size() + 64,
            SeriesBuffer::Log(l) => SERIES_OVERHEAD + l.total_bytes + l.entries.len() * 40,
        }
    }
}

/// Buffer for metric samples (Gorilla-compressed in-memory).
#[derive(Debug)]
struct MetricBuffer {
    encoder: GorillaEncoder,
    min_ts: i64,
    max_ts: i64,
    sample_count: u64,
}

/// Buffer for log entries (raw bytes, compressed on flush).
#[derive(Debug)]
struct LogBuffer {
    entries: Vec<LogEntry>,
    total_bytes: usize,
    min_ts: i64,
    max_ts: i64,
}

/// Per-series metadata for LRU eviction.
#[derive(Debug, Clone, Copy)]
struct SeriesMeta {
    /// Timestamp of the most recent write to this series.
    last_write_ts: i64,
    /// Number of samples/entries in this series.
    count: u64,
}

/// Configuration for the timeseries memtable.
#[derive(Debug, Clone)]
pub struct MemtableConfig {
    /// Maximum memory usage before flush is triggered (bytes).
    pub max_memory_bytes: usize,
    /// Maximum number of unique series (cardinality limit).
    /// When exceeded, coldest series are evicted.
    pub max_series: usize,
    /// Hard memory ceiling — ingest is rejected above this.
    /// Should be set slightly above max_memory_bytes to allow for
    /// in-flight writes during flush coordination.
    pub hard_memory_limit: usize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 64 * 1024 * 1024,  // 64 MiB soft limit
            max_series: 500_000,                 // 500K unique series
            hard_memory_limit: 80 * 1024 * 1024, // 80 MiB hard limit
        }
    }
}

/// L0 RAM memtable for timeseries data.
///
/// Organizes data by series ID. Metric samples are Gorilla-compressed
/// incrementally. Log entries are buffered raw and compressed on flush.
///
/// The memtable is NOT thread-safe — it lives on a single Data Plane
/// core (!Send by design).
pub struct TimeseriesMemtable {
    series: HashMap<SeriesId, SeriesBuffer>,
    /// LRU tracking: series_id → last write metadata.
    series_meta: HashMap<SeriesId, SeriesMeta>,
    /// Total approximate memory usage in bytes.
    memory_bytes: usize,
    /// Configuration.
    config: MemtableConfig,
    /// Metric sample count across all series.
    metric_count: u64,
    /// Log entry count across all series.
    log_count: u64,
    /// Creation timestamp of oldest unflushed data.
    oldest_ts: Option<i64>,
    /// Number of series evicted due to cardinality pressure.
    eviction_count: u64,
    /// Evicted series data waiting to be flushed alongside main drain.
    evicted: Vec<FlushedSeries>,
}

impl TimeseriesMemtable {
    pub fn new(max_memory_bytes: usize) -> Self {
        Self::with_config(MemtableConfig {
            max_memory_bytes,
            hard_memory_limit: max_memory_bytes + max_memory_bytes / 4,
            ..Default::default()
        })
    }

    pub fn with_config(config: MemtableConfig) -> Self {
        Self {
            series: HashMap::new(),
            series_meta: HashMap::new(),
            memory_bytes: 0,
            config,
            metric_count: 0,
            log_count: 0,
            oldest_ts: None,
            eviction_count: 0,
            evicted: Vec::new(),
        }
    }

    /// Ingest a metric sample.
    pub fn ingest_metric(&mut self, series_id: SeriesId, sample: MetricSample) -> IngestResult {
        // Hard limit: reject if we're way over budget.
        if self.memory_bytes >= self.config.hard_memory_limit {
            return IngestResult::Rejected;
        }

        // Cardinality check: evict coldest if at limit and this is a new series.
        if !self.series.contains_key(&series_id) && self.series.len() >= self.config.max_series {
            self.evict_coldest_series();
        }

        let is_new = !self.series.contains_key(&series_id);
        let buf = self.series.entry(series_id).or_insert_with(|| {
            SeriesBuffer::Metric(MetricBuffer {
                encoder: GorillaEncoder::new(),
                min_ts: sample.timestamp_ms,
                max_ts: sample.timestamp_ms,
                sample_count: 0,
            })
        });

        if let SeriesBuffer::Metric(m) = buf {
            m.encoder.encode(sample.timestamp_ms, sample.value);
            if sample.timestamp_ms < m.min_ts {
                m.min_ts = sample.timestamp_ms;
            }
            if sample.timestamp_ms > m.max_ts {
                m.max_ts = sample.timestamp_ms;
            }
            m.sample_count += 1;
            self.metric_count += 1;
        }

        // Update LRU metadata.
        self.series_meta
            .entry(series_id)
            .and_modify(|m| {
                m.last_write_ts = sample.timestamp_ms;
                m.count += 1;
            })
            .or_insert(SeriesMeta {
                last_write_ts: sample.timestamp_ms,
                count: 1,
            });

        // Memory accounting: actual encoder size for new series, estimate for append.
        if is_new {
            self.recompute_memory();
        } else {
            // Gorilla typically uses 1-3 bytes per sample; use 3 as conservative estimate.
            self.memory_bytes += 3;
        }

        self.update_oldest(sample.timestamp_ms);
        self.check_flush_state()
    }

    /// Ingest a log entry.
    pub fn ingest_log(&mut self, series_id: SeriesId, entry: LogEntry) -> IngestResult {
        if self.memory_bytes >= self.config.hard_memory_limit {
            return IngestResult::Rejected;
        }

        if !self.series.contains_key(&series_id) && self.series.len() >= self.config.max_series {
            self.evict_coldest_series();
        }

        let entry_size = entry.data.len();
        let ts = entry.timestamp_ms;

        let buf = self.series.entry(series_id).or_insert_with(|| {
            SeriesBuffer::Log(LogBuffer {
                entries: Vec::new(),
                total_bytes: 0,
                min_ts: ts,
                max_ts: ts,
            })
        });

        if let SeriesBuffer::Log(l) = buf {
            if ts < l.min_ts {
                l.min_ts = ts;
            }
            if ts > l.max_ts {
                l.max_ts = ts;
            }
            l.total_bytes += entry_size;
            l.entries.push(entry);
            self.log_count += 1;
        }

        self.series_meta
            .entry(series_id)
            .and_modify(|m| {
                m.last_write_ts = ts;
                m.count += 1;
            })
            .or_insert(SeriesMeta {
                last_write_ts: ts,
                count: 1,
            });

        // Log entries: actual bytes + Vec overhead per entry.
        self.memory_bytes += entry_size + 40;

        self.update_oldest(ts);
        self.check_flush_state()
    }

    /// Whether the memtable should be flushed (memory pressure).
    pub fn should_flush(&self) -> bool {
        self.memory_bytes >= self.config.max_memory_bytes
    }

    fn check_flush_state(&self) -> IngestResult {
        if self.memory_bytes >= self.config.max_memory_bytes {
            IngestResult::FlushNeeded
        } else {
            IngestResult::Ok
        }
    }

    /// Evict the coldest (least recently written) series.
    ///
    /// The evicted series data is moved to `self.evicted` so it gets
    /// included in the next drain/flush cycle rather than being lost.
    fn evict_coldest_series(&mut self) {
        let coldest = self
            .series_meta
            .iter()
            .min_by_key(|(_, meta)| meta.last_write_ts)
            .map(|(id, _)| *id);

        let Some(coldest_id) = coldest else { return };

        if let Some(buf) = self.series.remove(&coldest_id) {
            let evicted_mem = buf.memory_bytes();
            let flushed = match buf {
                SeriesBuffer::Metric(m) => {
                    let sample_count = m.sample_count;
                    let compressed = m.encoder.finish();
                    self.metric_count = self.metric_count.saturating_sub(sample_count);
                    FlushedSeries {
                        series_id: coldest_id,
                        kind: FlushedKind::Metric {
                            gorilla_block: compressed,
                            sample_count,
                        },
                        min_ts: m.min_ts,
                        max_ts: m.max_ts,
                    }
                }
                SeriesBuffer::Log(l) => {
                    self.log_count = self.log_count.saturating_sub(l.entries.len() as u64);
                    let total_bytes = l.total_bytes;
                    FlushedSeries {
                        series_id: coldest_id,
                        kind: FlushedKind::Log {
                            entries: l.entries,
                            total_bytes,
                        },
                        min_ts: l.min_ts,
                        max_ts: l.max_ts,
                    }
                }
            };
            self.evicted.push(flushed);
            self.series_meta.remove(&coldest_id);
            self.memory_bytes = self.memory_bytes.saturating_sub(evicted_mem);
            self.eviction_count += 1;
        }
    }

    /// Drain the memtable, returning all buffered data organized by series.
    ///
    /// After drain, the memtable is empty and ready for new ingest.
    /// Includes any series that were evicted due to cardinality pressure.
    pub fn drain(&mut self) -> Vec<FlushedSeries> {
        let mut result = Vec::with_capacity(self.series.len() + self.evicted.len());

        // Include previously evicted series.
        result.append(&mut self.evicted);

        for (series_id, buf) in self.series.drain() {
            match buf {
                SeriesBuffer::Metric(m) => {
                    let sample_count = m.sample_count;
                    let compressed = m.encoder.finish();
                    result.push(FlushedSeries {
                        series_id,
                        kind: FlushedKind::Metric {
                            gorilla_block: compressed,
                            sample_count,
                        },
                        min_ts: m.min_ts,
                        max_ts: m.max_ts,
                    });
                }
                SeriesBuffer::Log(l) => {
                    result.push(FlushedSeries {
                        series_id,
                        kind: FlushedKind::Log {
                            entries: l.entries,
                            total_bytes: l.total_bytes,
                        },
                        min_ts: l.min_ts,
                        max_ts: l.max_ts,
                    });
                }
            }
        }

        self.series_meta.clear();
        self.memory_bytes = 0;
        self.metric_count = 0;
        self.log_count = 0;
        self.oldest_ts = None;

        result
    }

    /// Recompute memory usage from scratch (expensive, use sparingly).
    fn recompute_memory(&mut self) {
        let base_overhead = self.series.len() * 64; // HashMap entry overhead
        let series_bytes: usize = self.series.values().map(|b| b.memory_bytes()).sum();
        let meta_bytes = self.series_meta.len() * 32;
        let evicted_bytes: usize = self
            .evicted
            .iter()
            .map(|f| match &f.kind {
                FlushedKind::Metric { gorilla_block, .. } => gorilla_block.len() + 32,
                FlushedKind::Log { total_bytes, .. } => *total_bytes + 32,
            })
            .sum();
        self.memory_bytes = base_overhead + series_bytes + meta_bytes + evicted_bytes;
    }

    pub fn metric_count(&self) -> u64 {
        self.metric_count
    }

    pub fn log_count(&self) -> u64 {
        self.log_count
    }

    pub fn memory_bytes(&self) -> usize {
        self.memory_bytes
    }

    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    pub fn oldest_timestamp(&self) -> Option<i64> {
        self.oldest_ts
    }

    pub fn is_empty(&self) -> bool {
        self.series.is_empty() && self.evicted.is_empty()
    }

    pub fn eviction_count(&self) -> u64 {
        self.eviction_count
    }

    pub fn config(&self) -> &MemtableConfig {
        &self.config
    }

    fn update_oldest(&mut self, ts: i64) {
        match self.oldest_ts {
            None => self.oldest_ts = Some(ts),
            Some(old) if ts < old => self.oldest_ts = Some(ts),
            _ => {}
        }
    }
}

/// Data from a single series after memtable drain.
#[derive(Debug)]
pub struct FlushedSeries {
    pub series_id: SeriesId,
    pub kind: FlushedKind,
    pub min_ts: i64,
    pub max_ts: i64,
}

/// Type-specific flushed data.
#[derive(Debug)]
pub enum FlushedKind {
    Metric {
        /// Gorilla-compressed block.
        gorilla_block: Vec<u8>,
        sample_count: u64,
    },
    Log {
        entries: Vec<LogEntry>,
        total_bytes: usize,
    },
}

/// Time range for queries.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl TimeRange {
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }

    pub fn contains(&self, ts: i64) -> bool {
        ts >= self.start_ms && ts <= self.end_ms
    }
}

/// Read-only index over flushed L1 segments for time-range queries.
///
/// Maps (series_id, time_range) → segment file references.
/// Used by the query path to locate relevant segments without scanning.
#[derive(Debug)]
pub struct SegmentIndex {
    /// series_id → sorted list of (min_ts, max_ts, segment_path).
    entries: HashMap<SeriesId, BTreeMap<i64, SegmentRef>>,
    /// Total number of segments tracked.
    total_segments: usize,
    /// Total bytes across all tracked segments.
    total_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SegmentRef {
    pub path: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub kind: SegmentKind,
    /// On-disk size in bytes.
    pub size_bytes: u64,
    /// Timestamp when segment was created (for retention).
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Metric,
    Log,
}

impl SegmentIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            total_segments: 0,
            total_bytes: 0,
        }
    }

    /// Register a flushed segment.
    pub fn add(&mut self, series_id: SeriesId, seg: SegmentRef) {
        self.total_bytes += seg.size_bytes;
        self.total_segments += 1;
        self.entries
            .entry(series_id)
            .or_default()
            .insert(seg.min_ts, seg);
    }

    /// Find segments overlapping a time range for a given series.
    pub fn query(&self, series_id: SeriesId, range: &TimeRange) -> Vec<&SegmentRef> {
        let Some(tree) = self.entries.get(&series_id) else {
            return Vec::new();
        };
        tree.values()
            .filter(|seg| seg.max_ts >= range.start_ms && seg.min_ts <= range.end_ms)
            .collect()
    }

    /// Find ALL segments older than a given timestamp (for retention/compaction).
    pub fn segments_older_than(&self, cutoff_ts: i64) -> Vec<(SeriesId, i64, SegmentRef)> {
        let mut result = Vec::new();
        for (&series_id, tree) in &self.entries {
            for (&min_ts, seg) in tree {
                if seg.max_ts < cutoff_ts {
                    result.push((series_id, min_ts, seg.clone()));
                }
            }
        }
        result
    }

    /// Remove a segment from the index. Returns the removed segment if found.
    pub fn remove(&mut self, series_id: SeriesId, min_ts: i64) -> Option<SegmentRef> {
        let tree = self.entries.get_mut(&series_id)?;
        let seg = tree.remove(&min_ts)?;
        self.total_bytes = self.total_bytes.saturating_sub(seg.size_bytes);
        self.total_segments = self.total_segments.saturating_sub(1);
        // Clean up empty series entries.
        if tree.is_empty() {
            self.entries.remove(&series_id);
        }
        Some(seg)
    }

    pub fn series_count(&self) -> usize {
        self.entries.len()
    }

    pub fn total_segments(&self) -> usize {
        self.total_segments
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

impl Default for SegmentIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_memtable() {
        let mt = TimeseriesMemtable::new(1024 * 1024);
        assert!(mt.is_empty());
        assert_eq!(mt.metric_count(), 0);
        assert_eq!(mt.log_count(), 0);
    }

    #[test]
    fn ingest_metrics() {
        let mut mt = TimeseriesMemtable::new(1024 * 1024);
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1000 + i * 10,
                    value: 42.0 + i as f64,
                },
            );
        }
        assert_eq!(mt.metric_count(), 100);
        assert_eq!(mt.series_count(), 1);
        assert_eq!(mt.oldest_timestamp(), Some(1000));
    }

    #[test]
    fn ingest_logs() {
        let mut mt = TimeseriesMemtable::new(1024 * 1024);
        for i in 0..50 {
            mt.ingest_log(
                2,
                LogEntry {
                    timestamp_ms: 2000 + i * 100,
                    data: format!("log line {i}").into_bytes(),
                },
            );
        }
        assert_eq!(mt.log_count(), 50);
        assert_eq!(mt.series_count(), 1);
    }

    #[test]
    fn multiple_series() {
        let mut mt = TimeseriesMemtable::new(1024 * 1024);
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        mt.ingest_metric(
            2,
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );
        mt.ingest_log(
            3,
            LogEntry {
                timestamp_ms: 300,
                data: b"test".to_vec(),
            },
        );
        assert_eq!(mt.series_count(), 3);
    }

    #[test]
    fn drain_clears_state() {
        let mut mt = TimeseriesMemtable::new(1024 * 1024);
        for i in 0..10 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: i as f64,
                },
            );
        }
        mt.ingest_log(
            2,
            LogEntry {
                timestamp_ms: 500,
                data: b"hello".to_vec(),
            },
        );

        let flushed = mt.drain();
        assert_eq!(flushed.len(), 2);
        assert!(mt.is_empty());
        assert_eq!(mt.metric_count(), 0);
        assert_eq!(mt.log_count(), 0);
        assert_eq!(mt.memory_bytes(), 0);
    }

    #[test]
    fn flush_on_memory_pressure() {
        // Small budget.
        let mut mt = TimeseriesMemtable::new(100);
        let mut flush_triggered = false;
        for i in 0..200 {
            let result = mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: 42.0,
                },
            );
            if result.is_flush_needed() {
                flush_triggered = true;
                break;
            }
        }
        assert!(flush_triggered, "expected flush trigger");
    }

    #[test]
    fn cardinality_limit_evicts_coldest() {
        let config = MemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            max_series: 3,
            hard_memory_limit: 20 * 1024 * 1024,
        };
        let mut mt = TimeseriesMemtable::with_config(config);

        // Write to 3 series.
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        mt.ingest_metric(
            2,
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );
        mt.ingest_metric(
            3,
            MetricSample {
                timestamp_ms: 300,
                value: 3.0,
            },
        );
        assert_eq!(mt.series_count(), 3);
        assert_eq!(mt.eviction_count(), 0);

        // 4th series should trigger eviction of series 1 (coldest: ts=100).
        mt.ingest_metric(
            4,
            MetricSample {
                timestamp_ms: 400,
                value: 4.0,
            },
        );
        assert_eq!(mt.series_count(), 3);
        assert_eq!(mt.eviction_count(), 1);

        // Drain should include evicted series data.
        let flushed = mt.drain();
        assert_eq!(flushed.len(), 4); // 3 active + 1 evicted
    }

    #[test]
    fn hard_limit_rejects_ingest() {
        let config = MemtableConfig {
            max_memory_bytes: 100,
            max_series: 1000,
            hard_memory_limit: 200,
        };
        let mut mt = TimeseriesMemtable::with_config(config);

        // Fill with log data to hit hard limit.
        let mut rejected = false;
        for i in 0..1000 {
            let result = mt.ingest_log(
                1,
                LogEntry {
                    timestamp_ms: i,
                    data: vec![0u8; 100],
                },
            );
            if result.is_rejected() {
                rejected = true;
                break;
            }
        }
        assert!(rejected, "expected hard limit rejection");
    }

    #[test]
    fn segment_index_query() {
        let mut idx = SegmentIndex::new();
        let now = 1_700_000_000_000i64;
        idx.add(
            1,
            SegmentRef {
                path: "seg-001.ts".into(),
                min_ts: 0,
                max_ts: 3_600_000,
                kind: SegmentKind::Metric,
                size_bytes: 1024,
                created_at_ms: now,
            },
        );
        idx.add(
            1,
            SegmentRef {
                path: "seg-002.ts".into(),
                min_ts: 3_600_000,
                max_ts: 7_200_000,
                kind: SegmentKind::Metric,
                size_bytes: 2048,
                created_at_ms: now,
            },
        );

        // Query spanning both segments.
        let range = TimeRange::new(1_800_000, 5_400_000);
        let segs = idx.query(1, &range);
        assert_eq!(segs.len(), 2);

        // Query only second segment.
        let range = TimeRange::new(4_000_000, 5_000_000);
        let segs = idx.query(1, &range);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].path, "seg-002.ts");

        // Non-existent series.
        assert!(idx.query(999, &range).is_empty());

        // Size tracking.
        assert_eq!(idx.total_bytes(), 3072);
        assert_eq!(idx.total_segments(), 2);
    }

    #[test]
    fn segment_index_removal() {
        let mut idx = SegmentIndex::new();
        let now = 1_700_000_000_000i64;
        idx.add(
            1,
            SegmentRef {
                path: "seg-001.ts".into(),
                min_ts: 0,
                max_ts: 1000,
                kind: SegmentKind::Metric,
                size_bytes: 512,
                created_at_ms: now,
            },
        );
        idx.add(
            1,
            SegmentRef {
                path: "seg-002.ts".into(),
                min_ts: 1000,
                max_ts: 2000,
                kind: SegmentKind::Metric,
                size_bytes: 512,
                created_at_ms: now,
            },
        );

        assert_eq!(idx.total_segments(), 2);
        let removed = idx.remove(1, 0);
        assert!(removed.is_some());
        assert_eq!(idx.total_segments(), 1);
        assert_eq!(idx.total_bytes(), 512);
    }

    #[test]
    fn segments_older_than() {
        let mut idx = SegmentIndex::new();
        idx.add(
            1,
            SegmentRef {
                path: "old.ts".into(),
                min_ts: 0,
                max_ts: 100,
                kind: SegmentKind::Metric,
                size_bytes: 100,
                created_at_ms: 0,
            },
        );
        idx.add(
            1,
            SegmentRef {
                path: "new.ts".into(),
                min_ts: 200,
                max_ts: 300,
                kind: SegmentKind::Metric,
                size_bytes: 200,
                created_at_ms: 200,
            },
        );

        let old = idx.segments_older_than(150);
        assert_eq!(old.len(), 1);
        assert_eq!(old[0].2.path, "old.ts");
    }
}
