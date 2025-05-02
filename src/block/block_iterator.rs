use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;

/// BlockIterator parses and iterates through a block created by the BlockBuilder
pub struct BlockIterator {
    block_data: Bytes,        // The raw bytes of the entire block
    offsets: Vec<u16>,        // Parsed offset list
    num_entries: usize,       // Number of entries in the block
    current_index: usize,     // Current position for iteration/seek result
}

impl BlockIterator {
    /// Creates a new BlockIterator from raw block data
    pub fn new(block_data: Bytes) -> Self {
        if block_data.is_empty() {
            return Self {
                block_data,
                offsets: Vec::new(),
                num_entries: 0,
                current_index: 0,
            };
        }

        let num_entries = LittleEndian::read_u16(&block_data[block_data.len() - 2..]) as usize;
        let mut offsets = Vec::with_capacity(num_entries);
        let offsets_start = block_data.len() - 2 - (num_entries * 2);
        
        for i in 0..num_entries {
            let offset_pos = offsets_start + (i * 2);
            let offset = LittleEndian::read_u16(&block_data[offset_pos..offset_pos + 2]);
            offsets.push(offset);
        }

        Self {
            block_data,
            offsets,
            num_entries,
            current_index: 0,
        }
    }

    /// Returns true if the iterator is pointing to a valid entry
    pub fn is_valid(&self) -> bool {
        !self.offsets.is_empty() && self.current_index < self.num_entries
    }

    /// Helper method to get the offset of the current entry
    fn get_current_entry_offset(&self) -> Option<usize> {
        if !self.is_valid() {
            return None;
        }
        Some(self.offsets[self.current_index] as usize)
    }

    /// Helper method to get key length and start position for an entry
    fn get_key_info(&self, entry_offset: usize) -> (usize, usize) {
        let key_len = LittleEndian::read_u16(&self.block_data[entry_offset..entry_offset + 2]) as usize;
        let key_start = entry_offset + 2;
        (key_len, key_start)
    }

    /// Helper method to get value length and start position for an entry
    fn get_value_info(&self, entry_offset: usize) -> (usize, usize) {
        let (key_len, key_start) = self.get_key_info(entry_offset);
        let value_len_pos = key_start + key_len;
        let value_len = LittleEndian::read_u16(&self.block_data[value_len_pos..value_len_pos + 2]) as usize;
        let value_start = value_len_pos + 2;
        (value_len, value_start)
    }

    /// Returns the key at the current position
    pub fn key(&self) -> Option<&[u8]> {
        let entry_offset = self.get_current_entry_offset()?;
        let (key_len, key_start) = self.get_key_info(entry_offset);
        let key_end = key_start + key_len;
        Some(&self.block_data[key_start..key_end])
    }

    /// Returns the value at the current position
    pub fn value(&self) -> Option<&[u8]> {
        let entry_offset = self.get_current_entry_offset()?;
        let (value_len, value_start) = self.get_value_info(entry_offset);
        let value_end = value_start + value_len;
        Some(&self.block_data[value_start..value_end])
    }

    /// Moves the iterator to the next entry
    pub fn next(&mut self) {
        if self.is_valid() {
            self.current_index += 1;
        }
    }

    /// Seeks to the first entry whose key is >= the target key
    /// Uses binary search over the offsets
    pub fn seek_to_key(&mut self, target: &[u8]) -> bool {
        if self.num_entries == 0 {
            return false;
        }

        // Binary search to find the first key >= target
        let mut low = 0;
        let mut high = self.num_entries - 1;

        while low <= high {
            let mid = (low + high) / 2;
            let entry_offset = self.offsets[mid] as usize;
            
            // Extract the key at mid using our helper method
            let (key_len, key_start) = self.get_key_info(entry_offset);
            let key_end = key_start + key_len;
            let current_key = &self.block_data[key_start..key_end];

            match current_key.cmp(target) {
                std::cmp::Ordering::Less => {
                    // Current key is smaller, look in the right half
                    if mid == self.num_entries - 1 {
                        self.current_index = self.num_entries;
                        return false;
                    }
                    low = mid + 1;
                }
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
                    // Current key is greater than or equal to target
                    self.current_index = mid;
                    if mid == 0 || high == 0 {
                        return true;
                    }
                    high = mid - 1;
                }
            }
        }

        // If we've fallen through, position at the low entry
        self.current_index = low;
        self.is_valid()
    }

    /// Positions the iterator at the first entry of the block
    pub fn seek_to_first(&mut self) {
        self.current_index = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::block_builder::BlockBuilder;

    fn create_test_block() -> Bytes {
        let mut builder = BlockBuilder::new();
        
        // Add entries in sorted order
        builder.add(b"key1", b"value1");
        builder.add(b"key3", b"value3");
        builder.add(b"key5", b"value5");
        builder.add(b"key7", b"value7");
        builder.add(b"key9", b"value9");
        
        Bytes::from(builder.finish())
    }

    #[test]
    fn test_empty_block_iterator() {
        let iter = BlockIterator::new(Bytes::new());
        assert!(!iter.is_valid());
        assert_eq!(iter.key(), None);
        assert_eq!(iter.value(), None);
    }

    #[test]
    fn test_basic_iteration() {
        let block_data = create_test_block();
        let mut iter = BlockIterator::new(block_data);
        
        // Iterator should start at the first entry
        assert!(iter.is_valid());
        assert_eq!(iter.key(), Some(b"key1" as &[u8]));
        assert_eq!(iter.value(), Some(b"value1" as &[u8]));
        
        // Move to the next entry
        iter.next();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), Some(b"key3" as &[u8]));
        assert_eq!(iter.value(), Some(b"value3" as &[u8]));
        
        // Iterate through all entries
        iter.next();
        assert_eq!(iter.key(), Some(b"key5" as &[u8]));
        
        iter.next();
        assert_eq!(iter.key(), Some(b"key7" as &[u8]));
        
        iter.next();
        assert_eq!(iter.key(), Some(b"key9" as &[u8]));
        
        // Move past the last entry
        iter.next();
        assert!(!iter.is_valid());
        assert_eq!(iter.key(), None);
        assert_eq!(iter.value(), None);
    }

    #[test]
    fn test_seek() {
        let block_data = create_test_block();
        let mut iter = BlockIterator::new(block_data);
        
        // Seek to an existing key
        assert!(iter.seek_to_key(b"key5"));
        assert_eq!(iter.key(), Some(b"key5" as &[u8]));
        
        // Seek to a key that doesn't exist but falls between entries
        assert!(iter.seek_to_key(b"key4"));
        assert_eq!(iter.key(), Some(b"key5" as &[u8]));
        
        // Seek to a key before the first entry
        assert!(iter.seek_to_key(b"key0"));
        assert_eq!(iter.key(), Some(b"key1" as &[u8]));
        
        // Seek to a key after the last entry
        assert!(!iter.seek_to_key(b"keyz"));
        assert!(!iter.is_valid());
    }

    #[test]
    fn test_seek_to_first() {
        let block_data = create_test_block();
        let mut iter = BlockIterator::new(block_data);
        
        // Seek to the middle of the block
        iter.seek_to_key(b"key5");
        assert_eq!(iter.key(), Some(b"key5" as &[u8]));
        
        // Move forward
        iter.next();
        assert_eq!(iter.key(), Some(b"key7" as &[u8]));
        
        // Seek back to the first entry
        iter.seek_to_first();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), Some(b"key1" as &[u8]));
        assert_eq!(iter.value(), Some(b"value1" as &[u8]));
    }

    #[test]
    fn test_seek_to_first_after_end() {
        let block_data = create_test_block();
        let mut iter = BlockIterator::new(block_data);
        
        // Iterate past the end
        for _ in 0..6 {
            iter.next();
        }
        assert!(!iter.is_valid());
        
        // Seek to first should restore validity
        iter.seek_to_first();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), Some(b"key1" as &[u8]));
    }
}