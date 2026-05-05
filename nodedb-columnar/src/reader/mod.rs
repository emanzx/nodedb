mod block_decode;
mod segment_reader;
mod types;

pub use segment_reader::SegmentReader;
pub use types::DecodedColumn;

#[cfg(feature = "encryption")]
pub use segment_reader::OwnedSegmentReader;
