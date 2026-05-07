// SPDX-License-Identifier: Apache-2.0

//! Bit-packing and unpacking primitives for the FastLanes codec.
//!
//! Written as tight loops over bytes that LLVM auto-vectorizes to
//! AVX2/AVX-512/NEON/WASM-SIMD without explicit intrinsics.

/// Mask with `n` low bits set. Handles n=0 and n=64 without overflow.
#[inline]
pub(super) fn low_mask_u8(n: usize) -> u8 {
    if n >= 8 { 0xFF } else { (1u8 << n) - 1 }
}

#[inline]
pub(super) fn low_mask_u64(n: usize) -> u64 {
    if n >= 64 { u64::MAX } else { (1u64 << n) - 1 }
}

/// Pack a value into a byte array at the given bit offset.
///
/// Written as a tight loop over bytes for auto-vectorization.
#[inline]
pub(super) fn pack_bits(packed: &mut [u8], bit_offset: usize, value: u64, bit_width: u8) {
    let bw = bit_width as usize;
    if bw == 0 {
        return;
    }

    let byte_idx = bit_offset / 8;
    let bit_idx = bit_offset % 8;

    // How many bits fit in the first byte.
    let first_bits = (8 - bit_idx).min(bw);

    // Write first partial byte.
    packed[byte_idx] |= ((value & low_mask_u64(first_bits)) as u8) << bit_idx;

    let mut remaining = bw - first_bits;
    let mut val = value >> first_bits;
    let mut bi = byte_idx + 1;

    // Write full bytes.
    while remaining >= 8 {
        packed[bi] = (val & 0xFF) as u8;
        val >>= 8;
        remaining -= 8;
        bi += 1;
    }

    // Write last partial byte.
    if remaining > 0 {
        packed[bi] |= (val & low_mask_u64(remaining)) as u8;
    }
}

/// Unpack a value from a byte array at the given bit offset.
#[inline]
pub(super) fn unpack_bits(packed: &[u8], bit_offset: usize, bit_width: u8) -> u64 {
    let bw = bit_width as usize;
    if bw == 0 {
        return 0;
    }

    let byte_idx = bit_offset / 8;
    let bit_idx = bit_offset % 8;

    // How many bits available in the first byte.
    let first_bits = (8 - bit_idx).min(bw);
    let mut value = ((packed[byte_idx] >> bit_idx) & low_mask_u8(first_bits)) as u64;

    let mut collected = first_bits;
    let mut bi = byte_idx + 1;

    // Read full bytes.
    while collected + 8 <= bw {
        value |= (packed[bi] as u64) << collected;
        collected += 8;
        bi += 1;
    }

    // Read last partial byte.
    let remaining = bw - collected;
    if remaining > 0 {
        value |= ((packed[bi] & low_mask_u8(remaining)) as u64) << collected;
    }

    value
}
