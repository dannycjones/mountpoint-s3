use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tracing::{debug, trace};

/// A simple buffer pool that allows reusing buffers to reduce memory fragmentation.
#[derive(Debug)]
pub struct BufferPool {
    buffers: Mutex<VecDeque<BytesMut>>,
    buffer_size: usize,
}

#[derive(Debug)]
pub struct LeasedBytesMut {
    buffer: Option<BytesMut>,
    pool: Arc<BufferPool>,
}

impl LeasedBytesMut {
    pub fn bytes_mut(&mut self) -> &mut BytesMut {
        self.buffer.as_mut().expect("always Some before drop")
    }

    pub fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

impl Drop for LeasedBytesMut {
    fn drop(&mut self) {
        let buffer = self.buffer.take().expect("always Some before drop");
        self.pool.return_buffer(buffer);
    }
}

impl AsRef<[u8]> for LeasedBytesMut {
    fn as_ref(&self) -> &[u8] {
        let buffer = self.buffer.as_ref().expect("always Some before drop");
        buffer.as_ref()
    }
}

impl BufferPool {
    /// Create a new buffer pool with the specified buffer size and optional maximum number of buffers.
    fn new(buffer_size: usize) -> Self {
        debug!("creating buffer pool with buffer_size={}", buffer_size);
        Self {
            buffers: Mutex::new(VecDeque::new()),
            buffer_size,
        }
    }

    /// Get a buffer from the pool, or create a new one if none are available.
    ///
    /// Buffer will always be empty.
    pub fn get_buffer(self: &Arc<Self>) -> LeasedBytesMut {
        let mut buffers = self.buffers.lock().unwrap();
        let buffer = match buffers.pop_front() {
            Some(mut buffer) => {
                // Reset the buffer for reuse
                buffer.clear();
                trace!("Reusing buffer from pool");
                buffer
            }
            None => {
                trace!("Creating new buffer");
                BytesMut::with_capacity(self.buffer_size)
            }
        };
        LeasedBytesMut {
            buffer: Some(buffer),
            pool: Arc::clone(&self),
        }
    }

    /// Return a buffer to the pool for reuse.
    fn return_buffer(&self, buffer: BytesMut) {
        let mut buffers = self.buffers.lock().unwrap();
        trace!("Returning buffer to pool");
        buffers.push_back(buffer);
    }
}

/// Create a new shared buffer pool with no maximum buffer limit.
pub fn new_unbounded_buffer_pool(buffer_size: usize) -> Arc<BufferPool> {
    Arc::new(BufferPool::new(buffer_size))
}
