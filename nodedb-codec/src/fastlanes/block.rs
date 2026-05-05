//! Block-level encode and decode for the FastLanes FOR + bit-packing codec.

use super::bits::{pack_bits, unpack_bits};
use crate::error::CodecError;

/// Per-block header: 2 bytes count + 1 byte bit_width + 8 bytes min_value.
pub(super) const BLOCK_HEADER_SIZE: usize = 11;

/// Encode a single block (up to 1024 values).
pub(super) fn encode_block(values: &[i64], out: &mut Vec<u8>) {
    let count = values.len() as u16;

    // Find min/max for FOR.
    let mut min_val = values[0];
    let mut max_val = values[0];
    for &v in &values[1..] {
        if v < min_val {
            min_val = v;
        }
        if v > max_val {
            max_val = v;
        }
    }

    // Compute residuals and bit width.
    let range = (max_val as u128).wrapping_sub(min_val as u128) as u64;
    let bit_width = if range == 0 {
        0u8
    } else {
        64 - range.leading_zeros() as u8
    };

    // Block header.
    out.extend_from_slice(&count.to_le_bytes());
    out.push(bit_width);
    out.extend_from_slice(&min_val.to_le_bytes());

    if bit_width == 0 {
        // All values identical — no packed data needed.
        return;
    }

    // Bit-pack residuals.
    // This loop is structured for auto-vectorization: simple operations on
    // contiguous arrays, no branches in the inner loop, predictable access.
    let packed_bytes = (count as usize * bit_width as usize).div_ceil(8);
    let pack_start = out.len();
    out.resize(pack_start + packed_bytes, 0);
    let packed = &mut out[pack_start..];

    let bw = bit_width as u64;
    let mask = if bw == 64 { u64::MAX } else { (1u64 << bw) - 1 };

    // Pack values into the byte array, bit by bit.
    let mut bit_offset: usize = 0;
    for &val in values {
        let residual = (val.wrapping_sub(min_val) as u64) & mask;
        pack_bits(packed, bit_offset, residual, bit_width);
        bit_offset += bit_width as usize;
    }
}

/// Decode a single block from the byte stream.
///
/// Returns the new offset after this block.
pub(super) fn decode_block(
    data: &[u8],
    offset: usize,
    values: &mut Vec<i64>,
    block_idx: usize,
) -> Result<usize, CodecError> {
    if offset + BLOCK_HEADER_SIZE > data.len() {
        return Err(CodecError::Truncated {
            expected: offset + BLOCK_HEADER_SIZE,
            actual: data.len(),
        });
    }

    let count = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
    let bit_width = data[offset + 2];
    let min_val = i64::from_le_bytes([
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
        data[offset + 8],
        data[offset + 9],
        data[offset + 10],
    ]);

    let mut pos = offset + BLOCK_HEADER_SIZE;

    if bit_width == 0 {
        // All values are min_val.
        values.extend(std::iter::repeat_n(min_val, count));
        return Ok(pos);
    }

    if bit_width > 64 {
        return Err(CodecError::Corrupt {
            detail: format!("block {block_idx}: invalid bit_width {bit_width}"),
        });
    }

    let packed_bytes = (count * bit_width as usize).div_ceil(8);
    if pos + packed_bytes > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + packed_bytes,
            actual: data.len(),
        });
    }

    let packed = &data[pos..pos + packed_bytes];
    let mask: u64 = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };

    // Unpack residuals and add min_val.
    let mut bit_offset: usize = 0;
    for _ in 0..count {
        let residual = unpack_bits(packed, bit_offset, bit_width) & mask;
        values.push(min_val.wrapping_add(residual as i64));
        bit_offset += bit_width as usize;
    }

    pos += packed_bytes;
    Ok(pos)
}

/// Skip a block without decoding, returning the next byte offset.
pub(super) fn skip_block(
    data: &[u8],
    offset: usize,
    block_idx: usize,
) -> Result<usize, CodecError> {
    if offset + BLOCK_HEADER_SIZE > data.len() {
        return Err(CodecError::Truncated {
            expected: offset + BLOCK_HEADER_SIZE,
            actual: data.len(),
        });
    }
    let count = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
    let bit_width = data[offset + 2];
    if bit_width > 64 {
        return Err(CodecError::Corrupt {
            detail: format!("block {block_idx}: invalid bit_width {bit_width}"),
        });
    }
    let packed_bytes = if bit_width == 0 {
        0
    } else {
        (count * bit_width as usize).div_ceil(8)
    };
    Ok(offset + BLOCK_HEADER_SIZE + packed_bytes)
}

/// Compute the minimum number of bits needed to represent the range of values.
///
/// Useful for external callers that want to estimate compression ratio.
pub fn bit_width_for_range(min: i64, max: i64) -> u8 {
    let range = (max as u128).wrapping_sub(min as u128) as u64;
    if range == 0 {
        0
    } else {
        64 - range.leading_zeros() as u8
    }
}
