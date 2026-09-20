//! PCM16 chunking helper.

/// Splits raw PCM16 byte slices into fixed-duration chunks.
pub struct AudioChunker {
    chunk_bytes: usize,
}

impl AudioChunker {
    pub fn new(sample_rate: u32, chunk_ms: u64) -> Self {
        let samples_per_chunk = (sample_rate as u64 * chunk_ms / 1000).max(1);
        let chunk_bytes = (samples_per_chunk as usize) * 2; // PCM16 = 2 bytes/sample
        Self { chunk_bytes }
    }

    /// Split `data` into a Vec of fixed-size chunks (last may be short).
    pub fn chunks<'a>(&self, data: &'a [u8]) -> Vec<&'a [u8]> {
        if data.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(data.len() / self.chunk_bytes + 1);
        let mut i = 0;
        while i < data.len() {
            let end = (i + self.chunk_bytes).min(data.len());
            out.push(&data[i..end]);
            i = end;
        }
        out
    }
}
