use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{self, Write, Seek, SeekFrom, BufWriter};
use std::path::{Path, PathBuf};
use byteorder::{LittleEndian, WriteBytesExt};
use serde_bencode::{ser, Error as BencodeError};
use bytes::Bytes;
use serde::{Serialize, Deserialize};

use crate::block::block_builder::{BlockBuilder, DEFAULT_BLOCK_SIZE};
use crate::memtable::MemTableValidIterator;

/// Options for creating SSTables
#[derive(Debug, Clone)]
pub struct TableOptions {
    /// Target size for data blocks (~4KB by default)
    pub block_size: usize,
}

impl Default for TableOptions {
    fn default() -> Self {
        TableOptions {
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}

/// Metadata about an individual data block in the SSTable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMetadata {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub offset: u64,
    pub size: u64,
}

/// Metadata about the entire SSTable
#[derive(Debug, Clone)]
pub struct SstableMetadata {
    pub file_path: PathBuf,
    pub total_size: u64,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub metadata_block_offset: u32,
    pub block_count: usize,
}

/// TableBuilder builds an SSTable file from key-value pairs
/// The file format consists of:
/// [Data Blocks][Metadata Block][Footer]
pub struct TableBuilder {
    writer: BufWriter<File>,
    offset: u64,
    block_builder: BlockBuilder,
    block_metas: Vec<BlockMetadata>,
    options: TableOptions,
    current_block_first_key: Option<Vec<u8>>,
    current_block_last_key: Option<Vec<u8>>,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    file_path: PathBuf,
}

impl TableBuilder {
    /// Creates a new TableBuilder that writes to the specified file path with default options
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::with_options(path, TableOptions::default())
    }

    /// Creates a new TableBuilder with specified options
    pub fn with_options<P: AsRef<Path>>(path: P, options: TableOptions) -> io::Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)?;

        let writer = BufWriter::new(file);
        
        Ok(TableBuilder {
            writer,
            offset: 0,
            block_builder: BlockBuilder::with_capacity(options.block_size),
            block_metas: Vec::new(),
            options,
            current_block_first_key: None,
            current_block_last_key: None,
            first_key: None,
            last_key: None,
            file_path,
        })
    }

    /// Adds a key-value pair to the SSTable
    /// The key-value pairs must be added in sorted order
    /// A None value represents a tombstone
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        
        self.last_key = Some(key.to_vec());
        
        if self.current_block_first_key.is_none() {
            self.current_block_first_key = Some(key.to_vec());
        }
        
        self.current_block_last_key = Some(key.to_vec());
        
        // Use empty value for tombstone (None)
        let value_bytes = value.unwrap_or(&[]);
        
        // If the block builder is full, finish the current block and start a new one
        if !self.block_builder.add(key, value_bytes) {
            self.finish_block()?;
            
            // Start a new block with this key-value pair
            self.current_block_first_key = Some(key.to_vec());
            self.current_block_last_key = Some(key.to_vec());
            assert!(self.block_builder.add(key, value_bytes), 
                   "Key-value pair too large for block");
        }
        
        Ok(())
    }

    /// Finishes the current block and writes it to disk
    fn finish_block(&mut self) -> io::Result<()> {
        if self.block_builder.is_empty() {
            return Ok(());
        }
        
        let first_key = self.current_block_first_key.take()
            .expect("Block cannot be non-empty without a first key");
        let last_key = self.current_block_last_key.take()
            .expect("Block cannot be non-empty without a last key");
        
        let block_data = self.block_builder.finish();
        let block_size = block_data.len() as u64;
        let block_offset = self.offset;
        
        self.writer.write_all(&block_data)?;
        self.offset += block_size;
        
        self.block_metas.push(BlockMetadata {
            first_key,
            last_key,
            offset: block_offset,
            size: block_size,
        });
        
        // Reset the block builder for the next block
        self.block_builder.reset();
        
        Ok(())
    }

    /// Builds the Metadata Block containing index information
    fn build_metadata_block(&self) -> Result<Vec<u8>, BencodeError> {
        // Serialize the block metadata using serde_bencode
        ser::to_bytes(&self.block_metas)
    }

    /// Finalizes the SSTable by writing all data blocks, metadata block, and footer
    pub fn finish(mut self) -> io::Result<SstableMetadata> {
        self.finish_block()?;
        
        let first_key = self.first_key.clone().unwrap_or_default();
        let last_key = self.last_key.clone().unwrap_or_default();
        
        let metadata_block = self.build_metadata_block()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        
        let metadata_block_offset = self.offset as u32;
        self.writer.write_all(&metadata_block)?;
        
        self.offset += metadata_block.len() as u64;
        
        self.writer.write_u32::<LittleEndian>(metadata_block_offset)?;
        
        self.offset += 4; // Footer size (u32)
        
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        
        Ok(SstableMetadata {
            file_path: self.file_path,
            total_size: self.offset,
            first_key,
            last_key,
            metadata_block_offset,
            block_count: self.block_metas.len(),
        })
    }

    /// Builds an SSTable from an iterator over key-value pairs
    pub fn build_from_iterator<'a>(
        path: impl AsRef<Path>,
        iter: &mut MemTableValidIterator<'a>,
        options: Option<TableOptions>,
    ) -> io::Result<SstableMetadata> {
        let mut builder = match options {
            Some(opts) => TableBuilder::with_options(path, opts)?,
            None => TableBuilder::new(path)?,
        };

        for (key, value) in iter {
            builder.add(&key, Some(value.as_ref()))?;
        }

        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::memtable::MemTable;
    use std::sync::Arc;
    use std::fs;

    // Helper to create a temporary SSTable for testing
     fn create_test_sstable(entries: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> (PathBuf, SstableMetadata, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        
        let mut builder = TableBuilder::new(&path).unwrap();
        
        for (key, value) in entries {
            builder.add(&key, value.as_deref()).unwrap();
        }
        
        let metadata = builder.finish().unwrap();
        (path, metadata, dir)  // Return the TempDir to keep it alive
    }

    #[test]
    fn test_table_builder_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");
        
        let builder = TableBuilder::new(&path).unwrap();
        let metadata = builder.finish().unwrap();
        
        // Check that the file was created
        assert!(path.exists());
        
        // Verify metadata
        assert_eq!(metadata.block_count, 0);
        assert_eq!(metadata.first_key, Vec::<u8>::new());
        assert_eq!(metadata.last_key, Vec::<u8>::new());
        
        // Check file size (should at least have the footer)
        let file_size = fs::metadata(&path).unwrap().len();
        assert!(file_size >= 4); // Footer is at least 4 bytes
    }
    
    #[test]
    fn test_table_builder_add_one_pair() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("one_pair.sst");
        
        let mut builder = TableBuilder::new(&path).unwrap();
        
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();
        
        builder.add(&key, Some(&value)).unwrap();
        let metadata = builder.finish().unwrap();
        
        // Verify metadata
        assert_eq!(metadata.block_count, 1);
        assert_eq!(metadata.first_key, key);
        assert_eq!(metadata.last_key, key);
        
        // Check file size
        let file_size = fs::metadata(&path).unwrap().len();
        assert!(file_size > 0); // File should have some data
    }
    
    #[test]
    fn test_table_builder_multiple_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi_block.sst");
        
        // Use a small block size to force multiple blocks
        let options = TableOptions {
            block_size: 128, // Small block size for testing
        };
        
        let mut builder = TableBuilder::with_options(&path, options).unwrap();
        
        // Add enough key-value pairs to create multiple blocks
        for i in 0..20 {
            let key = format!("key{:03}", i);
            let value = format!("value for key {}", i);
            builder.add(key.as_bytes(), Some(value.as_bytes())).unwrap();
        }
        
        let metadata = builder.finish().unwrap();
        
        // Verify we have multiple blocks
        assert!(metadata.block_count > 1);
        
        // Verify first and last keys
        assert_eq!(metadata.first_key, b"key000".to_vec());
        assert_eq!(metadata.last_key, b"key019".to_vec());
    }
    
    #[test]
    fn test_build_from_iterator() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("from_iterator.sst");
        
        // Create a memtable and add some data
        let memtable = MemTable::new(None);
        for i in 0..10 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            memtable.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
        }
        
        // Add a tombstone
        memtable.delete(b"key005".to_vec());
        
        // Create iterator over valid entries
        let mut iter = memtable.iter_valid();
        
        // Build SSTable from iterator
        let metadata = TableBuilder::build_from_iterator(&path, &mut iter, None).unwrap();
        
        // Verify metadata
        assert!(metadata.block_count >= 1);
        assert!(path.exists());
    }
    
    #[test]
    fn test_tombstone_representation() {
        let entries = vec![
            (b"key1".to_vec(), Some(b"value1".to_vec())),
            (b"key2".to_vec(), None), // Tombstone
            (b"key3".to_vec(), Some(b"value3".to_vec())),
        ];
        
        let (path, metadata, _temp_dir) = create_test_sstable(entries);
        
        // Verify metadata
        assert_eq!(metadata.block_count, 1);
        assert_eq!(metadata.first_key, b"key1".to_vec());
        assert_eq!(metadata.last_key, b"key3".to_vec());
        
        // Verify the file exists
        assert!(path.exists());
    }
}