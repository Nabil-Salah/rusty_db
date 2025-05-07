use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use serde_bencode::{de, Error as BencodeError};
use log::{debug, error, info, trace, warn};
use anyhow::{Result, Context, anyhow};

use crate::block::block_iterator::BlockIterator;
use crate::buffer::BufferPoolManager;
use crate::shared::page::PageId;
use crate::sstable::table_builder::{BlockMetadata, SstableMetadata};

/// Error types for SSTable operations
#[derive(Debug)]
pub enum TableError {
    IoError(io::Error),
    BencodeError(BencodeError),
    InvalidFormat(String),
    KeyNotFound,
    BlockNotFound,
    BufferPoolError(String),
    InvalidState(String),
}

impl From<io::Error> for TableError {
    fn from(err: io::Error) -> Self {
        TableError::IoError(err)
    }
}

impl From<BencodeError> for TableError {
    fn from(err: BencodeError) -> Self {
        TableError::BencodeError(err)
    }
}

impl std::fmt::Display for TableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableError::IoError(e) => write!(f, "IO error: {}", e),
            TableError::BencodeError(e) => write!(f, "Bencode error: {}", e),
            TableError::InvalidFormat(s) => write!(f, "Invalid format: {}", s),
            TableError::KeyNotFound => write!(f, "Key not found"),
            TableError::BlockNotFound => write!(f, "Block not found"),
            TableError::BufferPoolError(s) => write!(f, "Buffer pool error: {}", s),
            TableError::InvalidState(s) => write!(f, "Invalid state: {}", s),
        }
    }
}

impl std::error::Error for TableError {}

/// TableIterator allows iterating over and searching within an SSTable file
pub struct TableIterator {
    file: File,
    buffer_pool: Arc<Mutex<BufferPoolManager>>,
    block_metas: Vec<BlockMetadata>,
    current_block_iter: Option<BlockIterator>,
    current_block_idx: Option<usize>,
    file_path: PathBuf,
    sst_id: u64,
    current_page_id: Option<PageId>,
    table_metadata: SstableMetadata,
}

impl TableIterator {
    /// Opens an SSTable file and returns a TableIterator
    pub fn open<P: AsRef<Path>>(
        path: P, 
        sst_id: u64,
        buffer_pool: Arc<Mutex<BufferPoolManager>>
    ) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        info!("Opening SSTable: {}", file_path.display());
        
        let mut file = File::open(&file_path)
            .context(format!("Failed to open SSTable file: {}", file_path.display()))?;
        
        let file_size = file.metadata()
            .context("Failed to get file metadata")?
            .len();
            
        if file_size < 4 {
            return Err(anyhow!("File too small to be a valid SSTable"));
        }
        
        debug!("Reading footer from SSTable file of size {}", file_size);
        file.seek(SeekFrom::End(-4))
            .context("Failed to seek to footer")?;
            
        let mut footer_buf = [0u8; 4];
        file.read_exact(&mut footer_buf)
            .context("Failed to read footer")?;
            
        let metadata_block_offset = LittleEndian::read_u32(&footer_buf) as u64;
        
        if metadata_block_offset >= file_size - 4 {
            return Err(anyhow!("Invalid metadata block offset: {} in file of size {}", 
                           metadata_block_offset, file_size));
        }
        
        debug!("Reading metadata block at offset {}", metadata_block_offset);
        let metadata_block = Self::read_metadata_block(&mut file, metadata_block_offset, file_size - metadata_block_offset - 4)
            .context("Failed to read metadata block")?;
            
        file.seek(SeekFrom::End(0))
            .context("Failed to reset file position")?;
            
        let block_metas: Vec<BlockMetadata> = de::from_bytes(&metadata_block)
            .context("Failed to deserialize block metadata")?;
            
        debug!("Loaded {} block metadata entries", block_metas.len());
        
        let first_key = block_metas.first().map_or(Vec::new(), |meta| meta.first_key.clone());
        let last_key = block_metas.last().map_or(Vec::new(), |meta| meta.last_key.clone());
        
        let table_metadata = SstableMetadata {
            file_path: file_path.clone(),
            total_size: file_size,
            first_key,
            last_key,
            metadata_block_offset: metadata_block_offset as u32,
            block_count: block_metas.len(),
        };
        
        info!("Successfully opened SSTable with {} blocks", block_metas.len());
        
        Ok(TableIterator {
            file,
            buffer_pool,
            block_metas,
            current_block_iter: None,
            current_block_idx: None,
            file_path,
            sst_id,
            current_page_id: None,
            table_metadata,
        })
    }
    
    /// Reads the metadata block from the file
    fn read_metadata_block(file: &mut File, offset: u64, size: u64) -> Result<Vec<u8>> {
        trace!("Reading metadata block of size {} from offset {}", size, offset);
        let mut buffer = vec![0u8; size as usize];
        file.seek(SeekFrom::Start(offset))
            .context("Failed to seek to metadata block")?;
        file.read_exact(&mut buffer)
            .context("Failed to read metadata block")?;
        Ok(buffer)
    }
    
    /// Returns true if the iterator is currently pointing to a valid entry
    pub fn is_valid(&self) -> bool {
        self.current_block_iter
            .as_ref()
            .map_or(false, |iter| iter.is_valid())
    }
    
    /// Returns the current key if the iterator is valid
    pub fn key(&self) -> Option<&[u8]> {
        self.current_block_iter.as_ref().and_then(|iter| iter.key())
    }
    
    /// Returns the current value if the iterator is valid
    /// A zero-length slice represents a tombstone
    pub fn value(&self) -> Option<&[u8]> {
        self.current_block_iter.as_ref().and_then(|iter| iter.value())
    }
    
    /// Returns true if the current entry is a tombstone (empty value)
    pub fn is_tombstone(&self) -> bool {
        match self.value() {
            Some(v) if v.is_empty() => true,
            Some(_) => false,
            None => false, // Not pointing to a valid entry
        }
    }
    
    /// Moves to the next key-value pair
    pub fn next(&mut self) -> Result<()> {
        if let Some(ref mut iter) = self.current_block_iter {
            iter.next();
            
            // If the current block iterator is exhausted, move to the next block
            if !iter.is_valid() {
                trace!("Current block iterator exhausted, moving to next block");
                if let Some(current_idx) = self.current_block_idx {
                    if current_idx + 1 < self.block_metas.len() {
                        return self.load_block(current_idx + 1);
                    }
                }
                // No more blocks, iterator becomes invalid
                debug!("No more blocks available, iterator becomes invalid");
                self.current_block_idx = None;
            }
        }
        
        Ok(())
    }

    /// Calculate a unique page ID for a block within this file
    fn calculate_page_id(&self, block_offset: u64) -> PageId {
        // Combine the SSTable ID and block offset to create a unique page ID
        // Use bit manipulation to fit both values in a u64
        (self.sst_id << 32) | (block_offset & 0xFFFFFFFF)
    }
    
    /// Seeks to the first key-value pair with a key >= target
    pub fn seek_to_key(&mut self, target: &[u8]) -> Result<bool> {
        if self.block_metas.is_empty() {
            debug!("Cannot seek in empty SSTable");
            return Ok(false);
        }

        if target < &self.block_metas[0].first_key[..] {
            trace!("Target key is before first key in SSTable, seeking to first entry");
            return self.seek_to_first();
        }
        
        let block_idx = self.find_block_for_key(target);
        if block_idx >= self.block_metas.len() {
            debug!("Target key is after last key in SSTable");
            self.current_block_iter = None;
            self.current_block_idx = None;
            self.current_page_id = None;
            return Ok(false);
        }
        
        trace!("Found potential block {} for key", block_idx);
        self.load_block(block_idx)?;
        
        if let Some(ref mut iter) = self.current_block_iter {
            if iter.seek_to_key(target) {
                trace!("Found key in block {}", block_idx);
                self.current_block_idx = Some(block_idx);
                return Ok(true);
            }
        }
        
        debug!("Key not found in block {}", block_idx);
        Ok(false)
    }
    
    /// Binary search to find the block that may contain the target key
    fn find_block_for_key(&self, target: &[u8]) -> usize {
        if self.block_metas.is_empty() {
            return 0;
        }
        
        let mut left = 0;
        let mut right = self.block_metas.len() - 1;
        
        while left <= right {
            let mid = (left + right) / 2;
            let meta = &self.block_metas[mid];
            
            if target <= &meta.last_key[..] {
                if mid == 0 || target > &self.block_metas[mid - 1].last_key[..] {
                    trace!("Found block {} for key (key <= block.last_key)", mid);
                    return mid;
                }
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        
        // If the key is greater than all blocks' last keys, return the index past the last block
        trace!("Key is beyond all blocks, returning index {}", self.block_metas.len());
        self.block_metas.len()
    }
    
    /// Loads a block using the buffer pool
    fn load_block(&mut self, block_idx: usize) -> Result<()> {
        if block_idx >= self.block_metas.len() {
            return Err(anyhow!("Block index {} out of bounds (max: {})", 
                           block_idx, self.block_metas.len() - 1));
        }
        
        let meta = &self.block_metas[block_idx];
        debug!("Loading block {} with offset {} and size {}", block_idx, meta.offset, meta.size);
        
        // Unpin the current page if there is one
        if let Some(page_id) = self.current_page_id {
            trace!("Unpinning current page {}", page_id);
            let mut pool = self.buffer_pool.lock()
                .map_err(|_| anyhow!("Failed to lock buffer pool"))?;
                
            if let Err(e) = pool.unpin_page(page_id, false) {
                warn!("Failed to unpin page {}: {:?}", page_id, e);
                // Continue even if unpinning fails
            }
            self.current_page_id = None;
        }
        
        let page_id = self.calculate_page_id(meta.offset);
        trace!("Calculated page ID {} for block {}", page_id, block_idx);
        
        let mut pool = self.buffer_pool.lock()
            .map_err(|_| anyhow!("Failed to lock buffer pool"))?;
        
        let page_arc = match pool.fetch_page(page_id) {
            Ok(page_arc) => {
                trace!("Page {} found in buffer pool", page_id);
                page_arc
            },
            Err(_) => {
                debug!("Page {} not in buffer pool, loading from disk", page_id);
                // Page not in buffer pool - we need to allocate a new page and load from disk
                let mut data = vec![0u8; meta.size as usize];
                self.file.seek(SeekFrom::Start(meta.offset))
                    .context("Failed to seek to block data")?;
                self.file.read_exact(&mut data)
                    .context("Failed to read block data")?;
                
                // Create a new page in the buffer pool
                let (_, page_arc) = pool.new_page(page_id)
                    .context("Failed to create new page in buffer pool")?;
                
                // Write data to the page and set its ID
                {
                    let mut page = page_arc.write()
                        .map_err(|_| anyhow!("Failed to write to page in buffer pool"))?;
                    
                    let page_data = page.get_data_mut();
                    // Copy data to page (up to page size)
                    let copy_size = std::cmp::min(meta.size as usize, page_data.len());
                    page_data[..copy_size].copy_from_slice(&data[..copy_size]);
                    
                    page.set_page_id(page_id);
                    trace!("Data copied to page {}", page_id);
                }
                
                page_arc
            }
        };
        
        // Read data from the page
        let block_data = {
            let page = page_arc.read()
                .map_err(|_| anyhow!("Failed to read page from buffer pool"))?;
            
            Bytes::copy_from_slice(&page.get_data()[..meta.size as usize])
        };
        
        // Create a block iterator over the data
        let block_iter = BlockIterator::new(block_data)
            .context("Failed to create block iterator")?;
            
        self.current_block_iter = Some(block_iter);
        self.current_block_idx = Some(block_idx);
        self.current_page_id = Some(page_id);
        
        debug!("Successfully loaded block {}", block_idx);
        Ok(())
    }
    
    /// Positions the iterator at the first key in the table
    pub fn seek_to_first(&mut self) -> Result<bool> {
        if self.block_metas.is_empty() {
            debug!("Cannot seek to first in empty SSTable");
            return Ok(false);
        }
        
        // Load the first block
        debug!("Seeking to first entry in SSTable");
        self.load_block(0)?;
        
        if let Some(ref mut iter) = self.current_block_iter {
            iter.seek_to_first();
            if iter.is_valid() {
                trace!("Found first valid entry");
                return Ok(true);
            }
        }
        
        // First block might be empty, try next blocks
        trace!("First block is empty, trying next blocks");
        self.next()?;
        Ok(self.is_valid())
    }
    
    /// Returns the table metadata
    pub fn metadata(&self) -> &SstableMetadata {
        &self.table_metadata
    }
}

impl Drop for TableIterator {
    fn drop(&mut self) {
        // Ensure we unpin the current page when the iterator is dropped
        if let Some(page_id) = self.current_page_id {
            if let Ok(mut pool) = self.buffer_pool.lock() {
                if let Err(e) = pool.unpin_page(page_id, false) {
                    warn!("Failed to unpin page {} during drop: {:?}", page_id, e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::table_builder::{TableBuilder, TableOptions};
    use crate::storage::DiskManager;
    use tempfile::tempdir;
    
    // Helper to create a test SSTable with specified entries
    fn create_test_sstable(entries: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> (tempfile::TempDir, PathBuf, SstableMetadata) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        
        let mut builder = TableBuilder::new(&path).unwrap();
        
        for (key, value) in entries {
            builder.add(&key, value.as_deref()).unwrap();
        }
        
        let metadata = builder.finish().unwrap();
        (dir, path, metadata)
    }
    
    // Helper to create a buffer pool manager for testing
    fn create_test_buffer_pool() -> Arc<Mutex<BufferPoolManager>> {
        let temp_file = format!("/tmp/rusty_db_test_buffer_{}.db", std::process::id());
        let dm = DiskManager::new(&temp_file).unwrap();
        Arc::new(Mutex::new(BufferPoolManager::new(10, dm)))
    }
    
    #[test]
    fn test_table_iterator_open() {
        let entries = vec![
            (b"key1".to_vec(), Some(b"value1".to_vec())),
            (b"key2".to_vec(), Some(b"value2".to_vec())),
            (b"key3".to_vec(), Some(b"value3".to_vec())),
        ];
        
        let (_dir, path, _) = create_test_sstable(entries);
        let buffer_pool = create_test_buffer_pool();
        
        let iter = TableIterator::open(&path, 1, buffer_pool).unwrap();
        
        // Should have at least one block
        assert!(!iter.block_metas.is_empty());
    }
    
    #[test]
    fn test_table_iterator_seek_to_first() {
        let entries = vec![
            (b"key1".to_vec(), Some(b"value1".to_vec())),
            (b"key2".to_vec(), Some(b"value2".to_vec())),
            (b"key3".to_vec(), Some(b"value3".to_vec())),
        ];
        
        let (_dir, path, _) = create_test_sstable(entries);
        let buffer_pool = create_test_buffer_pool();
        
        let mut iter = TableIterator::open(&path, 1, buffer_pool).unwrap();
        
        assert!(iter.seek_to_first().unwrap());
        assert!(iter.is_valid());
        
        // First key should be "key1"
        assert_eq!(iter.key(), Some(b"key1" as &[u8]));
        assert_eq!(iter.value(), Some(b"value1" as &[u8]));
    }
    
    #[test]
    fn test_table_iterator_seek() {
        let entries = vec![
            (b"key1".to_vec(), Some(b"value1".to_vec())),
            (b"key2".to_vec(), Some(b"value2".to_vec())),
            (b"key3".to_vec(), Some(b"value3".to_vec())),
            (b"key5".to_vec(), Some(b"value5".to_vec())),
            (b"key7".to_vec(), Some(b"value7".to_vec())),
        ];
        
        let (_dir, path, _) = create_test_sstable(entries);
        let buffer_pool = create_test_buffer_pool();
        
        let mut iter = TableIterator::open(&path, 1, buffer_pool).unwrap();
        
        // Seek to an existing key
        assert!(iter.seek_to_key(b"key3").unwrap());
        assert!(iter.is_valid());
        assert_eq!(iter.key(), Some(b"key3" as &[u8]));
        
        // Seek to a key that doesn't exist - should find next key
        assert!(iter.seek_to_key(b"key4").unwrap());
        assert!(iter.is_valid());
        assert_eq!(iter.key(), Some(b"key5" as &[u8]));
        
        // Seek to a key before all keys
        assert!(iter.seek_to_key(b"key0").unwrap());
        assert!(iter.is_valid());
        assert_eq!(iter.key(), Some(b"key1" as &[u8]));
        
        // Seek beyond the last key
        assert!(!iter.seek_to_key(b"key8").unwrap());
        assert!(!iter.is_valid());
    }
    
    #[test]
    fn test_table_iterator_next() {
        let entries = vec![
            (b"key1".to_vec(), Some(b"value1".to_vec())),
            (b"key2".to_vec(), Some(b"value2".to_vec())),
            (b"key3".to_vec(), Some(b"value3".to_vec())),
        ];
        
        let (_dir, path, _) = create_test_sstable(entries);
        let buffer_pool = create_test_buffer_pool();
        
        let mut iter = TableIterator::open(&path, 1, buffer_pool).unwrap();
        
        assert!(iter.seek_to_first().unwrap());
        
        // Check the sequence of keys
        assert_eq!(iter.key(), Some(b"key1" as &[u8]));
        iter.next().unwrap();
        assert_eq!(iter.key(), Some(b"key2" as &[u8]));
        iter.next().unwrap();
        assert_eq!(iter.key(), Some(b"key3" as &[u8]));
        iter.next().unwrap();
        assert!(!iter.is_valid()); // Past the end
    }
    
    #[test]
    fn test_table_iterator_with_tombstones() {
        let entries = vec![
            (b"key1".to_vec(), Some(b"value1".to_vec())),
            (b"key2".to_vec(), None), // Tombstone
            (b"key3".to_vec(), Some(b"value3".to_vec())),
        ];
        
        let (_dir, path, _) = create_test_sstable(entries);
        let buffer_pool = create_test_buffer_pool();
        
        let mut iter = TableIterator::open(&path, 1, buffer_pool).unwrap();
        
        // Seek to the tombstone
        assert!(iter.seek_to_key(b"key2").unwrap());
        assert!(iter.is_valid());
        assert_eq!(iter.key(), Some(b"key2" as &[u8]));
        
        // Value should be empty (tombstone)
        assert!(iter.is_tombstone());
        assert_eq!(iter.value(), Some(b"" as &[u8]));
    }
    
    #[test]
    fn test_table_iterator_multi_block() {
        // Create entries to force multiple blocks
        let mut entries = Vec::new();
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            entries.push((key.as_bytes().to_vec(), Some(value.as_bytes().to_vec())));
        }
        
        // Use small block size to force multiple blocks
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi_block.sst");
        
        let options = TableOptions {
            block_size: 128, // Small block size to force multiple blocks
        };
        
        let mut builder = TableBuilder::with_options(&path, options).unwrap();
        
        for (key, value) in &entries {
            builder.add(key, value.as_deref()).unwrap();
        }
        
        let metadata = builder.finish().unwrap();
        
        // Verify we have multiple blocks
        assert!(metadata.block_count > 1);
        
        // Test the iterator
        let buffer_pool = create_test_buffer_pool();
        let mut iter = TableIterator::open(&path, 1, buffer_pool).unwrap();
        
        // Test seek_to_first
        assert!(iter.seek_to_first().unwrap());
        assert_eq!(iter.key(), Some(b"key000" as &[u8]));
        
        // Test seeking to a key in a later block
        assert!(iter.seek_to_key(b"key050").unwrap());
        assert_eq!(iter.key().unwrap(), b"key050" as &[u8]);
        
        // Test moving through blocks with next()
        assert!(iter.seek_to_key(b"key099").unwrap());
        assert_eq!(iter.key().unwrap(), b"key099" as &[u8]);
        
        // Test going beyond the last key
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }
    
    #[test]
    fn test_buffer_pool_integration() {
        // Create a small buffer pool to test eviction
        let temp_file = format!("/tmp/rusty_db_test_buffer_int_{}.db", std::process::id());
        let dm = DiskManager::new(&temp_file).unwrap();
        let buffer_pool = Arc::new(Mutex::new(BufferPoolManager::new(3, dm)));
        
        // Create entries to force multiple blocks
        let mut entries = Vec::new();
        for i in 0..50 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            entries.push((key.as_bytes().to_vec(), Some(value.as_bytes().to_vec())));
        }
        
        // Use small block size to force multiple blocks
        let dir = tempdir().unwrap();
        let path = dir.path().join("buffer_test.sst");
        
        let options = TableOptions {
            block_size: 128, // Small block size to force multiple blocks
        };
        
        let mut builder = TableBuilder::with_options(&path, options).unwrap();
        
        for (key, value) in &entries {
            builder.add(key, value.as_deref()).unwrap();
        }
        
        let metadata = builder.finish().unwrap();
        assert!(metadata.block_count > 3); // Make sure we have more blocks than buffer pool capacity
        
        // Create iterator and seek to key in first block
        let mut iter = TableIterator::open(&path, 1, Arc::clone(&buffer_pool)).unwrap();
        assert!(iter.seek_to_key(b"key000").unwrap());
        
        // Seek to various keys that would require loading different blocks
        // This should exercise the buffer pool eviction logic
        for target in &["key010", "key020", "key030", "key040", "key020", "key005"] {
            assert!(iter.seek_to_key(target.as_bytes()).unwrap());
            // Extract the number from the key to construct the expected value
            let num = target[3..].parse::<usize>().unwrap();
            let expected_value = format!("value{}", num);
            assert_eq!(iter.value().unwrap(), expected_value.as_bytes());
        }
    }
}