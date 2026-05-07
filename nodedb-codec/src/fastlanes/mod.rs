// SPDX-License-Identifier: Apache-2.0

//! FastLanes-inspired FOR + bit-packing codec for integer columns.
//!
//! Frame-of-Reference (FOR): subtract the minimum value from all values,
//! reducing them to small unsigned residuals. Then bit-pack the residuals
//! using the minimum number of bits.
//!
//! The bit-packing loop is written as simple scalar operations on contiguous
//! arrays, which LLVM auto-vectorizes to AVX2/AVX-512/NEON/WASM-SIMD without
//! explicit intrinsics. This is the FastLanes insight: structured scalar code
//! that the compiler vectorizes, portable across all targets.
//!
//! Wire format:
//! ```text
//! [4 bytes] total value count (LE u32)
//! [2 bytes] block count (LE u16)
//! For each block:
//!   [2 bytes] values in this block (LE u16, max 1024)
//!   [1 byte]  bit width (0-64)
//!   [8 bytes] min value / reference (LE i64)
//!   [N bytes] bit-packed residuals
//! ```
//!
//! Block size: 1024 values. Last block may be smaller.

mod bits;
mod block;

pub use block::bit_width_for_range;

use crate::error::CodecError;
use block::{decode_block, encode_block, skip_block};

/// Block size for FastLanes processing. 1024 values aligns with SIMD
/// register widths across all targets (16 × 64-bit lanes on AVX-512,
/// 8 × 128-bit WASM v128 operations to cover 1024 elements).
const BLOCK_SIZE: usize = 1024;

/// Header: 4 bytes count + 2 bytes block_count.
const GLOBAL_HEADER_SIZE: usize = 6;

// ---------------------------------------------------------------------------
// Public encode / decode API
// ---------------------------------------------------------------------------

/// Encode a slice of i64 values using FOR + bit-packing.
pub fn encode(values: &[i64]) -> Vec<u8> {
    let total_count = values.len() as u32;
    let block_count = if values.is_empty() {
        0u16
    } else {
        values.len().div_ceil(BLOCK_SIZE) as u16
    };

    let mut out = Vec::with_capacity(GLOBAL_HEADER_SIZE + values.len() * 5);

    // Global header.
    out.extend_from_slice(&total_count.to_le_bytes());
    out.extend_from_slice(&block_count.to_le_bytes());

    // Encode each block.
    for chunk in values.chunks(BLOCK_SIZE) {
        encode_block(chunk, &mut out);
    }

    out
}

/// Decode FOR + bit-packed bytes back to i64 values.
pub fn decode(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }

    let total_count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let block_count = u16::from_le_bytes([data[4], data[5]]) as usize;

    if total_count == 0 {
        return Ok(Vec::new());
    }

    let mut values = Vec::with_capacity(total_count);
    let mut offset = GLOBAL_HEADER_SIZE;

    for block_idx in 0..block_count {
        offset = decode_block(data, offset, &mut values, block_idx)?;
    }

    if values.len() != total_count {
        return Err(CodecError::Corrupt {
            detail: format!(
                "value count mismatch: header says {total_count}, decoded {}",
                values.len()
            ),
        });
    }

    Ok(values)
}

/// Compute byte offsets for each block in an encoded stream.
///
/// Returns a Vec of byte offsets — `offsets[i]` is the start position of
/// block `i` within `data`. O(num_blocks) header scan, no decompression.
pub fn block_byte_offsets(data: &[u8]) -> Result<Vec<usize>, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }
    let num_blocks = u16::from_le_bytes([data[4], data[5]]) as usize;
    let mut offsets = Vec::with_capacity(num_blocks);
    let mut pos = GLOBAL_HEADER_SIZE;
    for i in 0..num_blocks {
        offsets.push(pos);
        pos = skip_block(data, pos, i)?;
    }
    Ok(offsets)
}

/// Decode a range of blocks [start_block..end_block) from encoded data.
///
/// More efficient than calling `decode_single_block` repeatedly — scans
/// headers once to find start_block, then decodes contiguously.
pub fn decode_block_range(
    data: &[u8],
    start_block: usize,
    end_block: usize,
) -> Result<Vec<i64>, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }
    let num_blocks = u16::from_le_bytes([data[4], data[5]]) as usize;
    if start_block >= num_blocks || end_block > num_blocks || start_block >= end_block {
        return Ok(Vec::new());
    }

    // Skip to start_block.
    let mut offset = GLOBAL_HEADER_SIZE;
    for i in 0..start_block {
        offset = skip_block(data, offset, i)?;
    }

    // Decode [start_block..end_block).
    let mut values = Vec::new();
    for i in start_block..end_block {
        offset = decode_block(data, offset, &mut values, i)?;
    }
    Ok(values)
}

/// Number of blocks in an encoded FastLanes stream.
pub fn block_count(data: &[u8]) -> Result<usize, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }
    Ok(u16::from_le_bytes([data[4], data[5]]) as usize)
}

/// Decode a single block by index without decoding the entire stream.
///
/// Iterates block headers to reach `block_idx`, then decodes only that
/// block. For sequential block-at-a-time processing, prefer
/// [`BlockIterator`] which tracks byte offsets without re-scanning.
pub fn decode_single_block(data: &[u8], block_idx: usize) -> Result<Vec<i64>, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }
    let num_blocks = u16::from_le_bytes([data[4], data[5]]) as usize;
    if block_idx >= num_blocks {
        return Err(CodecError::Corrupt {
            detail: format!("block_idx {block_idx} >= block_count {num_blocks}"),
        });
    }

    // Skip to the target block by iterating headers.
    let mut offset = GLOBAL_HEADER_SIZE;
    for i in 0..block_idx {
        offset = skip_block(data, offset, i)?;
    }

    let mut values = Vec::new();
    decode_block(data, offset, &mut values, block_idx)?;
    Ok(values)
}

/// Iterator that decodes one 1024-row block at a time, tracking byte
/// offsets internally. Avoids re-scanning headers for sequential access.
pub struct BlockIterator<'a> {
    data: &'a [u8],
    offset: usize,
    blocks_remaining: usize,
    current_block: usize,
}

impl<'a> BlockIterator<'a> {
    /// Create a block iterator over encoded FastLanes data.
    pub fn new(data: &'a [u8]) -> Result<Self, CodecError> {
        if data.len() < GLOBAL_HEADER_SIZE {
            return Err(CodecError::Truncated {
                expected: GLOBAL_HEADER_SIZE,
                actual: data.len(),
            });
        }
        let num_blocks = u16::from_le_bytes([data[4], data[5]]) as usize;
        Ok(Self {
            data,
            offset: GLOBAL_HEADER_SIZE,
            blocks_remaining: num_blocks,
            current_block: 0,
        })
    }

    /// Skip the next block without decoding it.
    pub fn skip_block(&mut self) -> Result<(), CodecError> {
        if self.blocks_remaining == 0 {
            return Ok(());
        }
        self.offset = skip_block(self.data, self.offset, self.current_block)?;
        self.current_block += 1;
        self.blocks_remaining -= 1;
        Ok(())
    }
}

impl Iterator for BlockIterator<'_> {
    type Item = Result<Vec<i64>, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.blocks_remaining == 0 {
            return None;
        }
        let mut values = Vec::new();
        match decode_block(self.data, self.offset, &mut values, self.current_block) {
            Ok(new_offset) => {
                self.offset = new_offset;
                self.current_block += 1;
                self.blocks_remaining -= 1;
                Some(Ok(values))
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.blocks_remaining, Some(self.blocks_remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn single_value() {
        let encoded = encode(&[42i64]);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, vec![42i64]);
    }

    #[test]
    fn identical_values_zero_bits() {
        let values = vec![999i64; 1024];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        // All identical → bit_width=0 → only headers, no packed data.
        // Global header(6) + block header(11) = 17 bytes for 1024 values.
        assert_eq!(encoded.len(), 17);
    }

    #[test]
    fn small_range_values() {
        // Values in range [100, 107] → 3 bits per value.
        let values: Vec<i64> = (0..1024).map(|i| 100 + (i % 8)).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        // 1024 values × 3 bits = 384 bytes packed + headers.
        let expected_packed = (1024usize * 3).div_ceil(8); // 384 bytes
        let expected_total = GLOBAL_HEADER_SIZE + block::BLOCK_HEADER_SIZE + expected_packed;
        assert_eq!(encoded.len(), expected_total);
    }

    #[test]
    fn constant_rate_timestamps() {
        let values: Vec<i64> = (0..10_000)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        let bytes_per_sample = encoded.len() as f64 / values.len() as f64;
        assert!(
            bytes_per_sample < 4.0,
            "timestamps should pack to <4 bytes/sample, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn pre_delta_timestamps() {
        let deltas: Vec<i64> = vec![10_000i64; 10_000];
        let encoded = encode(&deltas);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, deltas);

        let bytes_per_sample = encoded.len() as f64 / deltas.len() as f64;
        assert!(
            bytes_per_sample < 0.2,
            "constant deltas should pack to near-zero, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn pre_delta_timestamps_with_jitter() {
        let mut deltas = Vec::with_capacity(10_000);
        let mut rng: u64 = 42;
        for _ in 0..10_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let jitter = ((rng >> 33) as i64 % 101) - 50;
            deltas.push(10_000 + jitter);
        }
        let encoded = encode(&deltas);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, deltas);

        let bytes_per_sample = encoded.len() as f64 / deltas.len() as f64;
        assert!(
            bytes_per_sample < 1.5,
            "jittered deltas should pack to <1.5 bytes/sample, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn negative_values() {
        let values: Vec<i64> = (-500..500).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn boundary_values() {
        let values = vec![i64::MIN, 0, i64::MAX];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn multiple_blocks() {
        let values: Vec<i64> = (0..3000).map(|i| i * 7 + 100).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn partial_last_block() {
        let values: Vec<i64> = (0..1025).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn compression_vs_raw() {
        let values: Vec<i64> = (0..10_000)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let encoded = encode(&values);
        let raw_size = values.len() * 8;
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(ratio > 2.0, "expected >2x compression, got {ratio:.1}x");
    }

    #[test]
    fn bit_width_calculation() {
        assert_eq!(bit_width_for_range(0, 0), 0);
        assert_eq!(bit_width_for_range(100, 100), 0);
        assert_eq!(bit_width_for_range(0, 1), 1);
        assert_eq!(bit_width_for_range(0, 7), 3);
        assert_eq!(bit_width_for_range(0, 8), 4);
        assert_eq!(bit_width_for_range(0, 255), 8);
        assert_eq!(bit_width_for_range(0, 256), 9);
        assert_eq!(bit_width_for_range(i64::MIN, i64::MAX), 64);
    }

    #[test]
    fn pack_unpack_roundtrip() {
        for bw in 1..=64u8 {
            let max_val: u64 = if bw == 64 { u64::MAX } else { (1u64 << bw) - 1 };
            let test_vals = [0u64, 1, max_val / 2, max_val];
            for &val in &test_vals {
                let mut packed = vec![0u8; 16];
                bits::pack_bits(&mut packed, 0, val, bw);
                let unpacked = bits::unpack_bits(&packed, 0, bw);
                let mask = if bw == 64 { u64::MAX } else { (1u64 << bw) - 1 };
                assert_eq!(
                    unpacked & mask,
                    val & mask,
                    "pack/unpack failed for bw={bw}, val={val}"
                );
            }
        }
    }

    #[test]
    fn pack_unpack_at_offsets() {
        let mut packed = vec![0u8; 32];
        bits::pack_bits(&mut packed, 0, 0b101, 3);
        bits::pack_bits(&mut packed, 3, 0b110, 3);
        bits::pack_bits(&mut packed, 6, 0b011, 3);

        assert_eq!(bits::unpack_bits(&packed, 0, 3), 0b101);
        assert_eq!(bits::unpack_bits(&packed, 3, 3), 0b110);
        assert_eq!(bits::unpack_bits(&packed, 6, 3), 0b011);
    }

    #[test]
    fn truncated_input_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[1, 0, 0, 0, 1, 0]).is_err()); // count=1, blocks=1, no block data
    }

    #[test]
    fn large_dataset_roundtrip() {
        let mut values = Vec::with_capacity(100_000);
        let mut rng: u64 = 12345;
        for _ in 0..100_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            values.push((rng >> 1) as i64);
        }
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn decode_single_block_correctness() {
        let values: Vec<i64> = (0..3000).collect();
        let encoded = encode(&values);
        assert_eq!(block_count(&encoded).unwrap(), 3);

        let b0 = decode_single_block(&encoded, 0).unwrap();
        assert_eq!(b0.len(), 1024);
        assert_eq!(b0, &values[..1024]);

        let b1 = decode_single_block(&encoded, 1).unwrap();
        assert_eq!(b1.len(), 1024);
        assert_eq!(b1, &values[1024..2048]);

        let b2 = decode_single_block(&encoded, 2).unwrap();
        assert_eq!(b2.len(), 952);
        assert_eq!(b2, &values[2048..]);
    }

    #[test]
    fn block_iterator_matches_full_decode() {
        let values: Vec<i64> = (0..5000).map(|i| i * 7 - 2000).collect();
        let encoded = encode(&values);

        let mut all = Vec::new();
        let iter = BlockIterator::new(&encoded).unwrap();
        for blk in iter {
            all.extend(blk.unwrap());
        }
        assert_eq!(all, values);
    }

    #[test]
    fn block_iterator_skip() {
        let values: Vec<i64> = (0..3000).collect();
        let encoded = encode(&values);

        let mut iter = BlockIterator::new(&encoded).unwrap();
        iter.skip_block().unwrap(); // skip block 0
        let b1 = iter.next().unwrap().unwrap();
        assert_eq!(b1, &values[1024..2048]);
    }
}
