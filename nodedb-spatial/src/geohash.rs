//! Geohash encoding/decoding for spatial indexing.
//!
//! Geohash is an interleaved Z-order curve over (longitude, latitude) that
//! maps 2D coordinates to a 1D string using base-32 encoding. Nearby points
//! share common prefixes, enabling fast proximity queries via string prefix
//! matching on standard B-tree indexes.
//!
//! Properties:
//! - Precision 1: ~5,000 km cells
//! - Precision 6: ~1.2 km × 0.6 km cells (default)
//! - Precision 8: ~19 m × 19 m cells
//! - Precision 12: ~3.7 cm × 1.9 cm cells (maximum practical)
//!
//! References:
//! - Gustavo Niemeyer, geohash.org (2008)
//! - Wikipedia: Geohash

use nodedb_types::BoundingBox;

/// Base-32 alphabet for geohash encoding (lowercase, no a/i/l/o to avoid
/// ambiguity with digits).
const BASE32: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Reverse lookup: ASCII byte → base32 index. Invalid chars map to 255.
const fn build_decode_table() -> [u8; 128] {
    let mut table = [255u8; 128];
    let chars = BASE32;
    let mut i = 0;
    while i < 32 {
        table[chars[i] as usize] = i as u8;
        i += 1;
    }
    table
}

static DECODE_TABLE: [u8; 128] = build_decode_table();

/// Encode a (longitude, latitude) coordinate to a geohash string.
///
/// - `precision`: number of characters (1–12). Default 6 gives ~1.2 km cells.
/// - Longitude range: [-180, 180]
/// - Latitude range: [-90, 90]
///
/// Returns an empty string if precision is 0.
pub fn geohash_encode(lng: f64, lat: f64, precision: u8) -> String {
    let precision = precision.min(12) as usize;
    if precision == 0 {
        return String::new();
    }

    let mut min_lng = -180.0_f64;
    let mut max_lng = 180.0_f64;
    let mut min_lat = -90.0_f64;
    let mut max_lat = 90.0_f64;

    let mut result = String::with_capacity(precision);
    let mut bits: u8 = 0;
    let mut bit_count: u8 = 0;
    let mut is_lng = true; // Longitude first (even bits).

    // Each character encodes 5 bits. Total bits = precision * 5.
    let total_bits = precision * 5;

    for _ in 0..total_bits {
        if is_lng {
            let mid = (min_lng + max_lng) / 2.0;
            if lng >= mid {
                bits = (bits << 1) | 1;
                min_lng = mid;
            } else {
                bits <<= 1;
                max_lng = mid;
            }
        } else {
            let mid = (min_lat + max_lat) / 2.0;
            if lat >= mid {
                bits = (bits << 1) | 1;
                min_lat = mid;
            } else {
                bits <<= 1;
                max_lat = mid;
            }
        }
        is_lng = !is_lng;
        bit_count += 1;

        if bit_count == 5 {
            result.push(BASE32[bits as usize] as char);
            bits = 0;
            bit_count = 0;
        }
    }

    result
}

/// Decode a geohash string to its bounding box.
///
/// Returns `None` if the geohash contains invalid characters or is empty.
pub fn geohash_decode(hash: &str) -> Option<BoundingBox> {
    if hash.is_empty() {
        return None;
    }

    let mut min_lng = -180.0_f64;
    let mut max_lng = 180.0_f64;
    let mut min_lat = -90.0_f64;
    let mut max_lat = 90.0_f64;
    let mut is_lng = true;

    for byte in hash.bytes() {
        if byte >= 128 {
            return None;
        }
        let idx = DECODE_TABLE[byte as usize];
        if idx == 255 {
            return None;
        }

        // Each character encodes 5 bits, MSB first.
        for bit in (0..5).rev() {
            let on = (idx >> bit) & 1 == 1;
            if is_lng {
                let mid = (min_lng + max_lng) / 2.0;
                if on {
                    min_lng = mid;
                } else {
                    max_lng = mid;
                }
            } else {
                let mid = (min_lat + max_lat) / 2.0;
                if on {
                    min_lat = mid;
                } else {
                    max_lat = mid;
                }
            }
            is_lng = !is_lng;
        }
    }

    Some(BoundingBox::new(min_lng, min_lat, max_lng, max_lat))
}

/// Decode a geohash to its center point (longitude, latitude).
pub fn geohash_decode_center(hash: &str) -> Option<(f64, f64)> {
    geohash_decode(hash).map(|bb| {
        (
            (bb.min_lng + bb.max_lng) / 2.0,
            (bb.min_lat + bb.max_lat) / 2.0,
        )
    })
}

/// Direction for neighbor computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    North,
    South,
    East,
    West,
    NorthEast,
    NorthWest,
    SouthEast,
    SouthWest,
}

impl Direction {
    /// All 8 directions.
    pub const ALL: [Direction; 8] = [
        Direction::North,
        Direction::NorthEast,
        Direction::East,
        Direction::SouthEast,
        Direction::South,
        Direction::SouthWest,
        Direction::West,
        Direction::NorthWest,
    ];
}

/// Compute the geohash of a neighbor cell in the given direction.
///
/// Returns `None` if the geohash is invalid or the neighbor would be
/// outside valid coordinate bounds (e.g., north of 90°).
pub fn geohash_neighbor(hash: &str, direction: Direction) -> Option<String> {
    let bb = geohash_decode(hash)?;
    let center_lng = (bb.min_lng + bb.max_lng) / 2.0;
    let center_lat = (bb.min_lat + bb.max_lat) / 2.0;
    let lng_span = bb.max_lng - bb.min_lng;
    let lat_span = bb.max_lat - bb.min_lat;

    let (dlng, dlat) = match direction {
        Direction::North => (0.0, lat_span),
        Direction::South => (0.0, -lat_span),
        Direction::East => (lng_span, 0.0),
        Direction::West => (-lng_span, 0.0),
        Direction::NorthEast => (lng_span, lat_span),
        Direction::NorthWest => (-lng_span, lat_span),
        Direction::SouthEast => (lng_span, -lat_span),
        Direction::SouthWest => (-lng_span, -lat_span),
    };

    let new_lng = center_lng + dlng;
    let new_lat = center_lat + dlat;

    // Clamp latitude. Wrap longitude.
    if !(-90.0..=90.0).contains(&new_lat) {
        return None;
    }
    let wrapped_lng = if new_lng > 180.0 {
        new_lng - 360.0
    } else if new_lng < -180.0 {
        new_lng + 360.0
    } else {
        new_lng
    };

    Some(geohash_encode(wrapped_lng, new_lat, hash.len() as u8))
}

/// Compute all 8 neighbor geohashes.
///
/// Returns a vec of `(Direction, geohash_string)` pairs. Neighbors that
/// would fall outside valid bounds (e.g., north of pole) are excluded.
pub fn geohash_neighbors(hash: &str) -> Vec<(Direction, String)> {
    Direction::ALL
        .iter()
        .filter_map(|&dir| geohash_neighbor(hash, dir).map(|h| (dir, h)))
        .collect()
}

/// Compute the set of geohash cells that cover a bounding box at the
/// given precision. Useful for range queries: "find all geohash prefixes
/// that overlap this region."
///
/// Returns up to `max_cells` geohash strings. If the bbox is too large
/// for the precision, returns fewer cells (the covering is approximate).
pub fn geohash_cover(bbox: &BoundingBox, precision: u8, max_cells: usize) -> Vec<String> {
    if precision == 0 || max_cells == 0 {
        return Vec::new();
    }

    let mut cells = Vec::new();

    // Decode a single geohash to find cell size at this precision.
    let sample = geohash_encode(bbox.min_lng, bbox.min_lat, precision);
    let sample_bb = match geohash_decode(&sample) {
        Some(bb) => bb,
        None => return Vec::new(),
    };
    let cell_lng = sample_bb.max_lng - sample_bb.min_lng;
    let cell_lat = sample_bb.max_lat - sample_bb.min_lat;

    if cell_lng <= 0.0 || cell_lat <= 0.0 {
        return Vec::new();
    }

    // Iterate over the bounding box in cell-sized steps.
    // Handle antimeridian crossing: when min_lng > max_lng, the bbox wraps
    // around ±180° (e.g., 170°E to 170°W). Split into two ranges.
    let lng_ranges: Vec<(f64, f64)> = if bbox.min_lng > bbox.max_lng {
        vec![(bbox.min_lng, 180.0), (-180.0, bbox.max_lng)]
    } else {
        vec![(bbox.min_lng, bbox.max_lng)]
    };

    let mut seen = std::collections::HashSet::with_capacity(max_cells);
    let mut lat = bbox.min_lat;
    while lat <= bbox.max_lat && cells.len() < max_cells {
        for &(lng_start, lng_end) in &lng_ranges {
            let mut lng = lng_start;
            while lng <= lng_end && cells.len() < max_cells {
                let hash = geohash_encode(lng, lat, precision);
                if seen.insert(hash.clone()) {
                    cells.push(hash);
                }
                lng += cell_lng;
            }
        }
        lat += cell_lat;
    }

    cells
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        // NYC: Times Square.
        let hash = geohash_encode(-73.9857, 40.7580, 9);
        assert_eq!(hash.len(), 9);

        let bb = geohash_decode(&hash).unwrap();
        // Center should be very close to original.
        let center_lng = (bb.min_lng + bb.max_lng) / 2.0;
        let center_lat = (bb.min_lat + bb.max_lat) / 2.0;
        assert!((center_lng - (-73.9857)).abs() < 0.001);
        assert!((center_lat - 40.7580).abs() < 0.001);
    }

    #[test]
    fn encode_precision_1() {
        let hash = geohash_encode(0.0, 0.0, 1);
        assert_eq!(hash.len(), 1);
        assert_eq!(hash, "s"); // origin maps to 's' cell
    }

    #[test]
    fn encode_precision_6_default() {
        let hash = geohash_encode(-73.9857, 40.7580, 6);
        assert_eq!(hash.len(), 6);
        // Should start with "dr5ru" (NYC area prefix).
        assert!(hash.starts_with("dr5ru"), "got {hash}");
    }

    #[test]
    fn encode_precision_12() {
        let hash = geohash_encode(139.6917, 35.6895, 12); // Tokyo
        assert_eq!(hash.len(), 12);
    }

    #[test]
    fn decode_invalid_chars() {
        assert!(geohash_decode("").is_none());
        assert!(geohash_decode("abc!").is_none()); // '!' invalid
        assert!(geohash_decode("ailo").is_none()); // 'a', 'i', 'l', 'o' not in base32
    }

    #[test]
    fn decode_center() {
        // Encode a known point, then decode and verify roundtrip.
        let hash = geohash_encode(13.405, 52.52, 9); // Berlin
        let (lng, lat) = geohash_decode_center(&hash).unwrap();
        assert!((lng - 13.405).abs() < 0.001, "lng: {lng}");
        assert!((lat - 52.52).abs() < 0.001, "lat: {lat}");
    }

    #[test]
    fn nearby_points_share_prefix() {
        // Two nearby points should have a common prefix.
        let h1 = geohash_encode(-73.985, 40.758, 8);
        let h2 = geohash_encode(-73.986, 40.759, 8);
        // At least first 5 chars should match (nearby in NYC).
        assert_eq!(&h1[..5], &h2[..5], "h1={h1}, h2={h2}");
    }

    #[test]
    fn neighbor_north() {
        let hash = geohash_encode(0.0, 0.0, 6);
        let north = geohash_neighbor(&hash, Direction::North).unwrap();
        let north_bb = geohash_decode(&north).unwrap();
        let orig_bb = geohash_decode(&hash).unwrap();
        // North neighbor's min_lat should be >= original's max_lat (approximately).
        assert!(
            north_bb.min_lat >= orig_bb.max_lat - 0.001,
            "north: {north_bb:?}, orig: {orig_bb:?}"
        );
    }

    #[test]
    fn neighbor_south() {
        let hash = geohash_encode(0.0, 0.0, 6);
        let south = geohash_neighbor(&hash, Direction::South).unwrap();
        let south_bb = geohash_decode(&south).unwrap();
        let orig_bb = geohash_decode(&hash).unwrap();
        assert!(south_bb.max_lat <= orig_bb.min_lat + 0.001);
    }

    #[test]
    fn all_8_neighbors() {
        let hash = geohash_encode(10.0, 50.0, 6);
        let neighbors = geohash_neighbors(&hash);
        // Should have 8 neighbors (well away from poles).
        assert_eq!(neighbors.len(), 8);
        // All should be different from the original.
        for (_, n) in &neighbors {
            assert_ne!(n, &hash);
        }
    }

    #[test]
    fn neighbor_at_pole_excluded() {
        // Near north pole — north neighbor should be None.
        let hash = geohash_encode(0.0, 89.99, 4);
        let north = geohash_neighbor(&hash, Direction::North);
        // Depending on cell size at precision 4, this may or may not be None.
        // At precision 4, lat span is ~5.6°, so 89.99 + 5.6 > 90 → None.
        assert!(north.is_none(), "expected None at pole, got {north:?}");
    }

    #[test]
    fn neighbor_wraps_longitude() {
        // Near date line — east neighbor should wrap.
        let hash = geohash_encode(179.99, 0.0, 6);
        let east = geohash_neighbor(&hash, Direction::East).unwrap();
        let east_bb = geohash_decode(&east).unwrap();
        // Should be in negative longitude (west side of date line).
        assert!(
            east_bb.min_lng < 0.0 || east_bb.max_lng < east_bb.min_lng,
            "east: {east_bb:?}"
        );
    }

    #[test]
    fn cover_small_bbox() {
        let bbox = BoundingBox::new(-0.01, -0.01, 0.01, 0.01);
        let cells = geohash_cover(&bbox, 6, 100);
        assert!(!cells.is_empty());
        // All cells should overlap the bbox.
        for cell in &cells {
            let cell_bb = geohash_decode(cell).unwrap();
            assert!(cell_bb.intersects(&bbox));
        }
    }

    #[test]
    fn cover_limits_max_cells() {
        let bbox = BoundingBox::new(-10.0, -10.0, 10.0, 10.0);
        let cells = geohash_cover(&bbox, 6, 5);
        assert!(cells.len() <= 5);
    }

    #[test]
    fn encode_extremes() {
        // Corners of the world.
        let ne = geohash_encode(180.0, 90.0, 6);
        let sw = geohash_encode(-180.0, -90.0, 6);
        assert!(!ne.is_empty());
        assert!(!sw.is_empty());
        assert_ne!(ne, sw);
    }

    #[test]
    fn base32_all_chars_valid() {
        // Every character in the base32 alphabet should decode successfully.
        for &ch in BASE32.iter() {
            let hash = String::from(ch as char);
            assert!(
                geohash_decode(&hash).is_some(),
                "failed for '{}'",
                ch as char
            );
        }
    }
}
