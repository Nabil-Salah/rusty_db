use std::convert::TryInto;
use byteorder::{LittleEndian, WriteBytesExt};

pub const DEFAULT_BLOCK_SIZE: usize = 4096; // ~4KB

/// BlockBuilder builds a block with the following format:
/// [Entry #1][Entry #2]...[Entry #N][Offset #1]...[Offset #N][Num Entries]
/// 
/// Entry format:
/// [key_len (u16)][key][value_len (u16)][value]
/// 
/// The offsets are u16 values pointing to the start of each entry.
/// The last 2 bytes store the number of entries as a u16.
pub struct BlockBuilder {
    buffer: Vec<u8>,       // Contains the block data
    offsets: Vec<u16>,     // Keeps track of entry offsets
    target_size: usize,    // Target block size (~4KB by default)
}

impl BlockBuilder {
    /// Creates a new BlockBuilder with the default target size
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BLOCK_SIZE)
    }

    /// Creates a new BlockBuilder with a specific target size
    pub fn with_capacity(target_size: usize) -> Self {
        BlockBuilder {
            buffer: Vec::with_capacity(target_size),
            offsets: Vec::new(),
            target_size,
        }
    }

    /// Returns the current estimated size of the block in bytes
    pub fn estimated_size(&self) -> usize {
        if self.is_empty() {
            return 0;
        }
        // Current data size + offsets (2 bytes each) + count (2 bytes)
        self.buffer.len() + (self.offsets.len() * 2) + 2
    }

    /// Adds a key-value pair to the block
    /// Returns false if adding would exceed the target size
    /// Assumes keys are added in sorted order
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool { 
        let entry_size = 2 + key.len() + 2 + value.len();
        let new_offsets_size = (self.offsets.len() + 1) * 2;
        let count_size = 2;
        let total_size = self.buffer.len() + entry_size + new_offsets_size + count_size;
        
        if total_size > self.target_size {
            return false;
        }
        
        let offset = self.buffer.len().try_into().unwrap();
        self.offsets.push(offset);
        self.buffer.write_u16::<LittleEndian>(key.len() as u16).unwrap();
        self.buffer.extend_from_slice(key);
        self.buffer.write_u16::<LittleEndian>(value.len() as u16).unwrap();
        self.buffer.extend_from_slice(value);
        
        true
    }

    /// Returns true if the block is empty
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalizes the block, appending offsets and count
    /// Returns the block as bytes with the following layout:
    /// [Entry #1][Entry #2]...[Entry #N][Padding][Offset #1]...[Offset #N][Num Entries]
    /// The result will always be exactly `target_size` bytes, with padding as needed
    pub fn finish(&self) -> Vec<u8> {
        if self.is_empty() {
            return vec![0; self.target_size];
        }
        
        // Calculate size of actual data (entries + offsets + count)
        let data_size = self.buffer.len() + (self.offsets.len() * 2) + 2;
        let padding_size = self.target_size - data_size;
        let mut result = Vec::with_capacity(self.target_size);
        result.extend_from_slice(&self.buffer);
        result.resize(self.buffer.len() + padding_size, 0);
        
        for &offset in &self.offsets {
            result.write_u16::<LittleEndian>(offset).unwrap();
        }

        result.write_u16::<LittleEndian>(self.offsets.len() as u16).unwrap();
        
        debug_assert_eq!(result.len(), self.target_size, 
                        "Block size should be exactly target_size");
        
        result
    }

    /// Resets the block builder to empty state
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.offsets.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{ByteOrder, LittleEndian};

    #[test]
    fn test_empty_block() {
        let builder = BlockBuilder::with_capacity(100); // Use a smaller size for testing
        assert!(builder.is_empty());
        assert_eq!(builder.estimated_size(), 0);
        
        let block = builder.finish();
        // An empty block should still be target_size bytes but filled with zeros
        assert_eq!(block.len(), 100);
        assert!(block.iter().all(|&b| b == 0));
    }
    
    #[test]
    fn test_add_single_entry() {
        // Use a smaller capacity for testing
        let mut builder = BlockBuilder::with_capacity(100);
        
        let key = b"key1";
        let value = b"value1";
        
        assert!(builder.add(key, value));
        assert!(!builder.is_empty());
        
        let block = builder.finish();
        
        // Block should be exactly target_size bytes
        assert_eq!(block.len(), 100);
        
        // Get the count from the last 2 bytes
        let count = LittleEndian::read_u16(&block[block.len() - 2..]);
        assert_eq!(count, 1);
        
        // Get the offset from the 2 bytes before count
        let offset_pos = block.len() - 4;
        let offset = LittleEndian::read_u16(&block[offset_pos..offset_pos + 2]);
        // Convert offset from u16 to usize for indexing
        let offset = offset as usize;
        
        // Verify key length at the offset
        let key_len = LittleEndian::read_u16(&block[offset..offset + 2]);
        assert_eq!(key_len, key.len() as u16);
        
        // Verify key
        assert_eq!(&block[offset + 2..offset + 2 + key.len()], key);
        
        // Verify value length
        let value_len_pos = offset + 2 + key.len();
        let value_len = LittleEndian::read_u16(&block[value_len_pos..value_len_pos + 2]);
        assert_eq!(value_len, value.len() as u16);
        
        // Verify value
        let value_pos = value_len_pos + 2;
        assert_eq!(&block[value_pos..value_pos + value.len()], value);
    }
    
    #[test]
    fn test_multiple_entries() {
        // Use a smaller capacity for testing
        let mut builder = BlockBuilder::with_capacity(200);
        
        // Add entries in sorted order
        assert!(builder.add(b"key1", b"value1"));
        assert!(builder.add(b"key2", b"value2"));
        assert!(builder.add(b"key3", b"value3"));
        
        let block = builder.finish();
        
        // Block should be exactly target_size bytes
        assert_eq!(block.len(), 200);
        
        // Verify count at the end
        let count = LittleEndian::read_u16(&block[block.len() - 2..]);
        assert_eq!(count, 3);
        
        // Verify we can find all three entries using their offsets
        let offsets_start = block.len() - 2 - (count as usize * 2);
        
        // Check the first entry
        let offset1 = LittleEndian::read_u16(&block[offsets_start..offsets_start + 2]) as usize;
        let key_len1 = LittleEndian::read_u16(&block[offset1..offset1 + 2]);
        assert_eq!(key_len1, 4); // "key1" length
        assert_eq!(&block[offset1 + 2..offset1 + 2 + key_len1 as usize], b"key1");
        
        // Check the second entry
        let offset2 = LittleEndian::read_u16(&block[offsets_start + 2..offsets_start + 4]) as usize;
        let key_len2 = LittleEndian::read_u16(&block[offset2..offset2 + 2]);
        assert_eq!(key_len2, 4); // "key2" length
        assert_eq!(&block[offset2 + 2..offset2 + 2 + key_len2 as usize], b"key2");
        
        // Check the third entry
        let offset3 = LittleEndian::read_u16(&block[offsets_start + 4..offsets_start + 6]) as usize;
        let key_len3 = LittleEndian::read_u16(&block[offset3..offset3 + 2]);
        assert_eq!(key_len3, 4); // "key3" length
        assert_eq!(&block[offset3 + 2..offset3 + 2 + key_len3 as usize], b"key3");
    }
    
    #[test]
    fn test_reset() {
        let mut builder = BlockBuilder::with_capacity(100);
        
        assert!(builder.add(b"key1", b"value1"));
        assert!(!builder.is_empty());
        
        builder.reset();
        assert!(builder.is_empty());
        assert_eq!(builder.estimated_size(), 0);
        
        // After reset, should be able to add again
        assert!(builder.add(b"key2", b"value2"));
        let block = builder.finish();
        assert_eq!(block.len(), 100); // Should always be target_size
    }
    
    #[test]
    fn test_size_limit() {
        // Create a block builder with a target size that can fit exactly one entry
        let key1 = b"k1";
        let value1 = b"v1";
        let key2 = b"k2";
        let value2 = b"v2";
        
        // Calculate exact size needed for first entry
        let first_entry_size = 2 + key1.len() + 2 + value1.len(); // key_len + key + value_len + value
        let offsets_size = 2; // One offset (2 bytes)
        let count_size = 2; // Count at end (2 bytes)
        let min_size = first_entry_size + offsets_size + count_size;
        
        // Make target size just enough for first entry and metadata
        let target_size = min_size;
        let mut builder = BlockBuilder::with_capacity(target_size);
        
        // First entry should succeed
        assert!(builder.add(key1, value1), "First entry should fit exactly");
        
        // Second entry should fail - even the smallest additional entry won't fit
        assert!(!builder.add(key2, value2), "Second entry should not fit");
        
        // Verify block size
        let block = builder.finish();
        assert_eq!(block.len(), target_size, "Block should be exactly target_size bytes");
    }
    
    #[test]
    fn test_padding() {
        // Create a block builder with specific capacity for testing padding
        let mut builder = BlockBuilder::with_capacity(100);
        
        // Add a small entry
        builder.add(b"key", b"value");
        
        // The actual data size would be:
        // key_len(2) + key(3) + value_len(2) + value(5) + offset(2) + count(2) = 16 bytes
        // So we should have 100 - 16 = 84 bytes of padding
        
        let block = builder.finish();
        assert_eq!(block.len(), 100); // Full target size
        
        // Verify the count is at the end
        let count = LittleEndian::read_u16(&block[98..100]);
        assert_eq!(count, 1);
        
        // Verify the offset is before the count
        let offset = LittleEndian::read_u16(&block[96..98]) as usize;
        assert_eq!(offset, 0);
        
        // Verify there's padding (zeros) between the entry data and offsets
        let entry_end = 2 + 3 + 2 + 5; // key_len + key + value_len + value = 12
        let padding_start = entry_end;
        let padding_end = 96; // Where the offsets start
        
        for i in padding_start..padding_end {
            assert_eq!(block[i], 0, "Padding byte at position {} should be 0", i);
        }
    }
}