use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use log::{debug, info, trace, warn};
use anyhow::{Result, Context, anyhow};
//use std::ops::Bound;

pub const DEFAULT_SIZE_THRESHOLD: usize = 256 * 1024 * 1024; //256MB

/// MemTable is an in-memory key-value store backed by a concurrent skip list
/// It supports concurrent read and write operations
pub struct MemTable {
    data: SkipMap<Vec<u8>, Option<Arc<Vec<u8>>>>,
    size: AtomicUsize,
    size_threshold: usize,
}

/// Iterator over all entries in the memtable, including tombstones
pub struct MemTableIterator<'a> {
    inner: crossbeam_skiplist::map::Iter<'a, Vec<u8>, Option<Arc<Vec<u8>>>>,
}

impl<'a> Iterator for MemTableIterator<'a> {
    type Item = (Vec<u8>, Option<Arc<Vec<u8>>>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| {
            let key = entry.key().clone();
            let value = entry.value().as_ref().map(Arc::clone);
            trace!("Iterator yielding key of size {}", key.len());
            (key, value)
        })
    }
}

/// Iterator over valid entries in the memtable (excluding tombstones)
pub struct MemTableValidIterator<'a> {
    inner: crossbeam_skiplist::map::Iter<'a, Vec<u8>, Option<Arc<Vec<u8>>>>,
}

impl<'a> Iterator for MemTableValidIterator<'a> {
    type Item = (Vec<u8>, Arc<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.inner.next() {
            let key = entry.key().clone();
            if let Some(value) = entry.value().as_ref() {
                trace!("Valid iterator yielding key of size {}", key.len());
                return Some((key, Arc::clone(value)));
            }
            trace!("Skipping tombstone entry");
        }
        None
    }
}

// /// Iterator over entries in a specific key range (excluding tombstones)
// pub struct MemTableRangeIterator<'a> {
//     inner: crossbeam_skiplist::map::Range<'a, Vec<u8>, Option<Arc<Vec<u8>>>>,
// }

// impl<'a> Iterator for MemTableRangeIterator<'a> {
//     type Item = (Vec<u8>, Arc<Vec<u8>>);

//     fn next(&mut self) -> Option<Self::Item> {
//         while let Some(entry) = self.inner.next() {
//             let key = entry.key().clone();
//             if let Some(value) = entry.value().as_ref() {
//                 return Some((key, Arc::clone(value)));
//             }
//         }
//         None
//     }
// }

impl MemTable {
    /// Creates a new MemTable with the specified size threshold
    pub fn new(size_threshold: Option<usize>) -> Self {
        let threshold = size_threshold.unwrap_or(DEFAULT_SIZE_THRESHOLD);
        info!("Creating MemTable with size threshold of {} bytes", threshold);
        
        MemTable {
            data: SkipMap::new(),
            size: AtomicUsize::new(0),
            size_threshold: threshold,
        }
    }

    /// Inserts a key-value pair into the memtable
    /// Returns a tuple containing (previous_value, success_flag)
    /// - previous_value: The previous value if the key existed
    /// - success_flag: true if the insert was successful, false if it would exceed the size threshold
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> (Option<Arc<Vec<u8>>>, bool) {
        let entry_size = key.len() + value.len();
        debug!("Adding entry with key size {} and value size {}", key.len(), value.len());
        
        let (old_value, old_size) = match self.data.get(&key) {
            Some(entry) => {
                let old_size = key.len() + entry.value().as_ref().map_or(0, |v| v.len());
                (entry.value().as_ref().map(Arc::clone), old_size)
            },
            None => (None, 0)
        };
        
        let current_size = self.size.load(Ordering::Relaxed);
        let new_size = current_size + entry_size - old_size;
        
        if old_size == 0 && new_size > self.size_threshold {
            warn!("Adding entry would exceed size threshold: current: {}, entry: {}, threshold: {}",
                  current_size, entry_size, self.size_threshold);
            return (None, false);
        }
        
        if old_size > 0 {
            if entry_size > old_size {
                self.size.fetch_add(entry_size - old_size, Ordering::Relaxed);
            } else {
                self.size.fetch_sub(old_size - entry_size, Ordering::Relaxed);
            }
        } else {
            self.size.fetch_add(entry_size, Ordering::Relaxed);
        }
        
        let arc_value = Some(Arc::new(value));
        
        self.data.insert(key, arc_value);
        
        if old_size > 0 {
            trace!("Updated existing key, old size: {}", old_size);
        } else {
            trace!("Inserted new key");
        }
        
        (old_value, true)
    }

    /// Marks a key as deleted by inserting a tombstone value (None)
    pub fn delete(&self, key: Vec<u8>) -> Option<Arc<Vec<u8>>> {
        let entry_size = key.len() + 1; // 1 byte for the None variant
        debug!("Adding tombstone for key of size {}", key.len());
        
        // Check for existing entry to get the old size
        match self.data.get(&key) {
            Some(entry) => {
                let old_size = key.len() + entry.value().as_ref().map_or(0, |v| v.len());
                if entry_size > old_size {
                    self.size.fetch_add(entry_size - old_size, Ordering::Relaxed);
                } else {
                    self.size.fetch_sub(old_size - entry_size, Ordering::Relaxed);
                }
                self.data.insert(key, None);
                entry.value().as_ref().map(Arc::clone)
            },
            None => {
                None
            }
        }
    }

    /// Gets the value for a key
    /// Returns None if the key doesn't exist or has been deleted
    pub fn get(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        trace!("Getting value for key of size {}", key.len());
        match self.data.get(key) {
            Some(entry) => {
                match entry.value() {
                    Some(arc_value) => {
                        trace!("Found value of size {}", arc_value.len());
                        Some(Arc::clone(arc_value))
                    },
                    None => {
                        trace!("Found tombstone for key");
                        None // Tombstone, return None
                    }
                }
            }
            None => {
                trace!("Key not found");
                None
            }
        }
    }

    // /// Returns an iterator over key-value pairs within the specified range
    // /// Both lower_bound and upper_bound are optional, allowing for open-ended ranges
    // /// Tombstones (deleted entries) are excluded from the results
    // pub fn scan<K>(&self, lower_bound: impl Into<Bound<K>>, upper_bound: impl Into<Bound<K>>) -> MemTableRangeIterator<'_>
    // where
    //     K: AsRef<[u8]>,
    // {
    //     // Convert bounds to the appropriate type
    //     let lower: Bound<Vec<u8>> = match lower_bound.into() {
    //         Bound::Included(k) => Bound::Included(k.as_ref().to_vec()),
    //         Bound::Excluded(k) => Bound::Excluded(k.as_ref().to_vec()),
    //         Bound::Unbounded => Bound::Unbounded,
    //     };
        
    //     let upper: Bound<Vec<u8>> = match upper_bound.into() {
    //         Bound::Included(k) => Bound::Included(k.as_ref().to_vec()),
    //         Bound::Excluded(k) => Bound::Excluded(k.as_ref().to_vec()),
    //         Bound::Unbounded => Bound::Unbounded,
    //     };

    //     MemTableRangeIterator {
    //         inner: self.data.range((lower, upper))
    //     }
    // }

    /// Returns an iterator over all key-value pairs in the memtable
    /// Tombstones (deleted entries) are included as (key, None)
    pub fn iter(&self) -> MemTableIterator<'_> {
        debug!("Creating full iterator over memtable with {} entries", self.data.len());
        MemTableIterator {
            inner: self.data.iter()
        }
    }

    /// Returns an iterator over all key-value pairs, filtering out tombstones
    pub fn iter_valid(&self) -> MemTableValidIterator<'_> {
        debug!("Creating valid-only iterator over memtable");
        MemTableValidIterator {
            inner: self.data.iter()
        }
    }

    /// Returns the current approximate size in bytes
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Checks if the memtable has reached its size threshold
    pub fn should_flush(&self) -> bool {
        let current_size = self.size();
        let should_flush = current_size >= self.size_threshold;
        
        if should_flush {
            info!("MemTable reached flush threshold: current size {} >= threshold {}", 
                  current_size, self.size_threshold);
        }
        
        should_flush
    }

    /// Returns the number of entries in the memtable
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Checks if the memtable is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clears all entries in the memtable
    pub fn clear(&self) {
        info!("Clearing memtable with {} entries and {} bytes", 
              self.data.len(), self.size.load(Ordering::Relaxed));
        self.data.clear();
        self.size.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use std::thread;
    // use std::sync::Arc as StdArc;
    // use std::ops::Bound;

    #[test]
    fn test_put_and_get() {
        let memtable = MemTable::new(Some(1024));
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();
        let expected_value = Arc::new(value.clone());
        
        assert_eq!(memtable.put(key.clone(), value.clone()), (None, true));
        assert_eq!(memtable.get(&key).as_ref().map(|v| v.as_ref()), Some(expected_value.as_ref()));
    }

    #[test]
    fn test_delete() {
        let memtable = MemTable::new(Some(1024));
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();
        let expected_value = Arc::new(value.clone());
        
        memtable.put(key.clone(), value.clone());
        
        // Delete should return the previous value
        let deleted_value = memtable.delete(key.clone());
        assert_eq!(deleted_value.as_ref().map(|v| v.as_ref()), Some(expected_value.as_ref()));
        
        // Get after delete should return None
        assert_eq!(memtable.get(&key), None);
    }

    #[test]
    fn test_size_tracking() {
        let memtable = MemTable::new(Some(1024));
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();
        let expected_size = key.len() + value.len();
        
        memtable.put(key, value);
        assert_eq!(memtable.size(), expected_size);
    }

    #[test]
    fn test_should_flush() {
        let threshold = 10;
        let memtable = MemTable::new(Some(threshold));
        
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();
        let entry_size = key.len() + value.len();
        
        // Add entries until we reach the threshold
        if entry_size < threshold {
            memtable.put(key, value);
            assert!(!memtable.should_flush());
        }
        
        // Add an entry that will exceed the threshold
        let key2 = b"key2".to_vec();
        let value2 = b"value_that_exceeds".to_vec();
        memtable.put(key2, value2);
        assert!(memtable.should_flush());
    }

    #[test]
    fn test_iter() {
        let memtable = MemTable::new(Some(1024));
        
        // Insert some regular entries
        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        memtable.put(b"key2".to_vec(), b"value2".to_vec());
        
        // Insert a tombstone
        memtable.delete(b"key3".to_vec());
        
        // Test regular iterator (includes tombstones)
        let mut count = 0;
        for (_, value) in memtable.iter() {
            if value.is_none() {
                count += 1; // Count tombstones
            }
        }
        assert_eq!(count, 1); // We should have 1 tombstone
        
        // Test valid iterator (excludes tombstones)
        let mut valid_count = 0;
        for _ in memtable.iter_valid() {
            valid_count += 1;
        }
        assert_eq!(valid_count, 2); // We should have 2 valid entries
    }

    #[test]
    fn test_default_size_threshold() {
        let memtable = MemTable::new(None);
        assert_eq!(memtable.size_threshold, DEFAULT_SIZE_THRESHOLD);
    }

    #[test]
    fn test_tombstone_operations() {
        let memtable = MemTable::new(Some(1024));
        
        // Put a value
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();
        memtable.put(key.clone(), value);
        
        // Delete the value (creates a tombstone)
        memtable.delete(key.clone());
        
        // Get should return None for a tombstone
        assert_eq!(memtable.get(&key), None);
        
        // Overwrite the tombstone with a new value
        let new_value = b"new_value".to_vec();
        memtable.put(key.clone(), new_value.clone());
        
        // Get should now return the new value
        assert_eq!(memtable.get(&key).as_ref().map(|v| v.as_ref()), 
                  Some(Arc::new(new_value.clone()).as_ref()));
    }

    // #[test]
    // fn test_scan_function() {
    //     let memtable = MemTable::new(Some(1024));
        
    //     // Insert data with keys in alphabetical order
    //     memtable.put(b"apple".to_vec(), b"red".to_vec());
    //     memtable.put(b"banana".to_vec(), b"yellow".to_vec());
    //     memtable.put(b"cherry".to_vec(), b"red".to_vec());
    //     memtable.put(b"date".to_vec(), b"brown".to_vec());
    //     memtable.put(b"elderberry".to_vec(), b"purple".to_vec());
        
    //     // Add a tombstone
    //     memtable.delete(b"cherry".to_vec());
        
    //     // Test inclusive range scan
    //     let results: Vec<_> = memtable.scan(
    //         Bound::Included(b"banana".as_ref()), 
    //         Bound::Included(b"date".as_ref())
    //     ).collect();
        
    //     // Should include banana and date, but not cherry (tombstone)
    //     assert_eq!(results.len(), 2);
    //     assert_eq!(results[0].0, b"banana");
    //     assert_eq!(results[1].0, b"date");
        
    //     // Test exclusive lower bound
    //     let results: Vec<_> = memtable.scan(
    //         Bound::Excluded(b"banana".as_ref()), 
    //         Bound::Included(b"elderberry".as_ref())
    //     ).collect();
        
    //     // Should include date and elderberry only
    //     assert_eq!(results.len(), 2);
    //     assert_eq!(results[0].0, b"date");
    //     assert_eq!(results[1].0, b"elderberry");
        
    //     // Test unbounded lower
    //     let results: Vec<_> = memtable.scan(
    //         Bound::Unbounded, 
    //         Bound::Excluded(b"cherry".as_ref())
    //     ).collect();
        
    //     // Should include apple and banana
    //     assert_eq!(results.len(), 2);
    //     assert_eq!(results[0].0, b"apple");
    //     assert_eq!(results[1].0, b"banana");
        
    //     // Test unbounded upper
    //     let results: Vec<_> = memtable.scan(
    //         Bound::Included(b"date".as_ref()), 
    //         Bound::Unbounded
    //     ).collect();
        
    //     // Should include date and elderberry
    //     assert_eq!(results.len(), 2);
    //     assert_eq!(results[0].0, b"date");
    //     assert_eq!(results[1].0, b"elderberry");
    // }
}