use std::{
    cmp::Ordering,
    fmt::{Debug, Write},
    io::{self, ErrorKind, Read},
    ops::Deref,
    sync::Arc,
};

/// A wrapper around a `Cow<'a, [u8]>` wrapper that implements Read, and has a
/// convenience method to take a slice of bytes as a `Cow<'a, [u8]>`.
#[derive(Clone)]
pub struct Buffer {
    buffer: Arc<Vec<u8>>,
    end: usize,
    position: usize,
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut slice = self.as_slice();
        write!(f, "Buffer {{ length: {}, bytes: [", slice.len())?;
        while !slice.is_empty() {
            let (chunk, remaining) = slice.split_at(4.min(slice.len()));
            slice = remaining;
            for byte in chunk {
                write!(f, "{:x}", byte)?;
            }
            if !slice.is_empty() {
                f.write_char(' ')?;
            }
        }
        f.write_str("] }}")
    }
}

impl Eq for Buffer {}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Ord for Buffer {
    fn cmp(&self, other: &Self) -> Ordering {
        if Arc::ptr_eq(&self.buffer, &other.buffer) {
            if self.position == other.position && self.end == other.end {
                Ordering::Equal
            } else {
                (&**self).cmp(&**other)
            }
        } else {
            (&**self).cmp(&**other)
        }
    }
}

impl PartialOrd for Buffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Buffer {
    /// Reads `count` bytes from the front of the buffer, returning a buffer
    /// that shares the same underlying buffer.
    pub fn read_bytes(&mut self, count: usize) -> Result<Self, std::io::Error> {
        let start = self.position;
        let end = self.position + count;
        if end > self.end {
            Err(std::io::Error::from(ErrorKind::UnexpectedEof))
        } else {
            self.position = end;
            Ok(Self {
                buffer: self.buffer.clone(),
                end,
                position: start,
            })
        }
    }

    /// Returns this buffer as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer[self.position..self.end]
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(buffer: Vec<u8>) -> Self {
        Self {
            end: buffer.len(),
            buffer: Arc::new(buffer),
            position: 0,
        }
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Read for Buffer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let end = self.buffer.len().min(self.position + buf.len());
        let bytes_read = end - self.position;

        if bytes_read == 0 {
            return Err(io::Error::from(ErrorKind::UnexpectedEof));
        }

        buf.copy_from_slice(&self.buffer[self.position..end]);
        self.position = end;

        Ok(bytes_read)
    }
}
