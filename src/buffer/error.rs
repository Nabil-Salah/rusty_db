use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BufferError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Page {0} not found in buffer pool")]
    PageNotFound(u64),
    
    #[error("Frame {0} not found")]
    FrameNotFound(usize),
    
    #[error("No free frames available in buffer pool")]
    NoFreeFrames,
    
    #[error("Page {0} is pinned and cannot be evicted")]
    PagePinned(u64),
    
    #[error("Failed to lock page: {0}")]
    LockError(String),
    
    #[error("Invalid page ID: {0}")]
    InvalidPageId(u64),
    
    #[error("Buffer pool is full")]
    BufferPoolFull,
    
    #[error("Internal error: {0}")]
    Internal(String),
}

pub type BufferResult<T> = std::result::Result<T, BufferError>;