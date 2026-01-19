//! Compression and decompression functions using Zstandard with marker byte protocol.
//!
//! This module handles smart compression with a marker byte that indicates whether
//! data is stored raw or compressed. Small strings are stored raw to avoid compression
//! overhead.

/// Default compression level (zstd range is 1-22, 3 is default)
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 3;

/// Marker bytes for stored values
pub const MARKER_RAW: u8 = 0x00;
pub const MARKER_COMPRESSED: u8 = 0x01;

/// Minimum size threshold for compression (bytes). Strings smaller than this
/// are stored raw since compression overhead would outweigh benefits.
pub const MIN_COMPRESS_SIZE: usize = 64;

/// Compress text if beneficial, prepending marker byte.
/// Returns MARKER_RAW + raw bytes if compression isn't beneficial,
/// or MARKER_COMPRESSED + compressed bytes otherwise.
pub fn compress_with_marker(text: &str, level: i32) -> std::result::Result<Vec<u8>, String> {
    let bytes = text.as_bytes();

    // Skip compression for small strings
    if bytes.len() < MIN_COMPRESS_SIZE {
        let mut result = Vec::with_capacity(1 + bytes.len());
        result.push(MARKER_RAW);
        result.extend_from_slice(bytes);
        return Ok(result);
    }

    // Try compression
    let compressed =
        zstd::encode_all(bytes, level).map_err(|e| format!("zstd compression failed: {}", e))?;

    // Use compressed only if it's actually smaller (accounting for marker byte)
    if compressed.len() < bytes.len() {
        let mut result = Vec::with_capacity(1 + compressed.len());
        result.push(MARKER_COMPRESSED);
        result.extend_from_slice(&compressed);
        Ok(result)
    } else {
        let mut result = Vec::with_capacity(1 + bytes.len());
        result.push(MARKER_RAW);
        result.extend_from_slice(bytes);
        Ok(result)
    }
}

/// Decompress data with marker byte.
/// Handles both MARKER_RAW (returns as-is) and MARKER_COMPRESSED (decompresses).
pub fn decompress_with_marker(data: &[u8]) -> std::result::Result<String, String> {
    if data.is_empty() {
        return Err("empty data".to_string());
    }

    match data[0] {
        MARKER_RAW => String::from_utf8(data[1..].to_vec())
            .map_err(|e| format!("invalid UTF-8 in raw data: {}", e)),
        MARKER_COMPRESSED => {
            let decompressed = zstd::decode_all(&data[1..])
                .map_err(|e| format!("zstd decompression failed: {}", e))?;
            String::from_utf8(decompressed)
                .map_err(|e| format!("decompressed data is not valid UTF-8: {}", e))
        }
        marker => Err(format!("unknown marker byte: 0x{:02x}", marker)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_small_string() {
        let result = compress_with_marker("Hi", DEFAULT_COMPRESSION_LEVEL).unwrap();
        assert_eq!(result[0], MARKER_RAW);
        assert_eq!(&result[1..], b"Hi");
    }

    #[test]
    fn test_compress_large_string() {
        let large_text = "x".repeat(1000);
        let result = compress_with_marker(&large_text, DEFAULT_COMPRESSION_LEVEL).unwrap();
        assert_eq!(result[0], MARKER_COMPRESSED);
        assert!(result.len() < large_text.len());
    }

    #[test]
    fn test_roundtrip() {
        for text in &["Hi", "Hello, World!", &"x".repeat(1000)] {
            let compressed = compress_with_marker(text, DEFAULT_COMPRESSION_LEVEL).unwrap();
            let decompressed = decompress_with_marker(&compressed).unwrap();
            assert_eq!(&decompressed, *text);
        }
    }

    #[test]
    fn test_decompress_empty() {
        let result = decompress_with_marker(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_unknown_marker() {
        let result = decompress_with_marker(&[0xFF, 0x00, 0x00]);
        assert!(result.is_err());
    }
}
