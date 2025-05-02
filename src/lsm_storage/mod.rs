use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};
use std::path::PathBuf;

use crate::buffer::BufferPoolManager;
use crate::memtable::MemTable;
use crate::wal::WriteAheadLog;

/// LSMStorage is the main storage engine using a Log-Structured Merge Tree design
/// It uses a memtable for recent writes and persists data in SSTables
pub struct LSMStorage {
    active_memtable: Arc<RwLock<MemTable>>,
    immutable_memtables: Mutex<VecDeque<Arc<MemTable>>>,
    buffer_pool: Arc<Mutex<BufferPoolManager>>,
    data_dir: PathBuf,
    max_immutable_memtables: usize,
    wal: Option<Arc<Mutex<WriteAheadLog>>>,
}

impl LSMStorage {
    /// Create a new LSM storage engine
    pub fn new(
        buffer_pool: Arc<Mutex<BufferPoolManager>>,
        data_dir: PathBuf,
        memtable_size_threshold: Option<usize>,
        max_immutable_memtables: usize,
        enable_wal: bool,
    ) -> std::io::Result<Self> {
        let wal = if enable_wal {
            let wal_path = data_dir.join("wal.log");
            let write_ahead_log = WriteAheadLog::new(wal_path)?;
            Some(Arc::new(Mutex::new(write_ahead_log)))
        } else {
            None
        };
        
        let memtable = MemTable::new(memtable_size_threshold);
        
        Ok(Self {
            active_memtable: Arc::new(RwLock::new(memtable)),
            immutable_memtables: Mutex::new(VecDeque::new()),
            buffer_pool,
            data_dir,
            max_immutable_memtables,
            wal,
        })
    }
    
    /// Put a key-value pair into the database
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        if let Some(ref wal) = self.wal {
            let mut wal_guard = wal.lock().unwrap();
            if let Err(e) = wal_guard.put(&key, &value) {
                return Err(format!("Failed to write to WAL: {}", e));
            }
        }
        
        let mut memtable_locked = self.active_memtable.write().unwrap();
        memtable_locked.put(key, value);
        
        if memtable_locked.should_flush() {
            drop(memtable_locked); // Release the lock before freezing
            self.freeze_memtable()?;
        }
        
        Ok(())
    }
    
    /// Delete a key from the database (by writing a tombstone)
    pub fn delete(&self, key: Vec<u8>) -> Result<(), String> {
        // First log to WAL if enabled
        if let Some(ref wal) = self.wal {
            let mut wal_guard = wal.lock().unwrap();
            if let Err(e) = wal_guard.delete(&key) {
                return Err(format!("Failed to write to WAL: {}", e));
            }
        }
        
        let mut memtable_locked = self.active_memtable.write().unwrap();
        
        memtable_locked.delete(key);
        
        if memtable_locked.should_flush() {
            drop(memtable_locked); // Release the lock before freezing
            self.freeze_memtable()?;
        }
        
        Ok(())
    }
    
    /// Get a value by key from the database
    pub fn get(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        if let Some(value) = self.active_memtable.read().unwrap().get(key) {
            return Some(value);
        }
        
        let immutable_tables = self.immutable_memtables.lock().unwrap();
        for memtable in immutable_tables.iter().rev() {
            if let Some(value) = memtable.get(key) {
                return Some(value);
            }
        }
        
        // If not found in any memtable, check the SSTables
        // This would involve searching through SSTables on disk
        // TODO We'll implement this in a future iteration
        None
    }
    
    // /// Range scan for key-value pairs within a range
    // pub fn scan<K>(&self, from_key: Option<K>, to_key: Option<K>) -> Vec<(Vec<u8>, Arc<Vec<u8>>)>
    // where
    //     K: AsRef<[u8]>,
    // {
    //     use std::ops::Bound;
        
    //     // Convert the optional keys to bounds
    //     let from_bound = match from_key {
    //         Some(key) => Bound::Included(key.as_ref().to_vec()),
    //         None => Bound::Unbounded,
    //     };
        
    //     let to_bound = match to_key {
    //         Some(key) => Bound::Included(key.as_ref().to_vec()),
    //         None => Bound::Unbounded,
    //     };
        
    //     // First get results from the active memtable
    //     let mut results = self.active_memtable
    //         .read()
    //         .unwrap()
    //         .scan(from_bound.clone(), to_bound.clone())
    //         .map(|(k, v)| (k, v))
    //         .collect::<Vec<_>>();
        
    //     // Then check the immutable memtables
    //     let immutable_tables = self.immutable_memtables.lock().unwrap();
    //     for memtable in immutable_tables.iter() {
    //         let memtable_results = memtable
    //             .scan(from_bound.clone(), to_bound.clone())
    //             .collect::<Vec<_>>();
            
    //         results.extend(memtable_results);
    //     }
        
    //     // TODO: Merge results from SSTables
    //     // For now, just return the results from memtables
        
    //     // Remove duplicates, keeping the newest version of each key
    //     // This is a simple approach - in a real LSM-tree we would use a more efficient merge algorithm
    //     let mut unique_results = std::collections::HashMap::new();
    //     for (key, value) in results {
    //         unique_results.insert(key, value);
    //     }
        
    //     unique_results.into_iter().collect()
    // }
    
    /// Freeze the current active memtable and create a new one
    fn freeze_memtable(&self) -> Result<(), String> {
        let mut immutable_tables = self.immutable_memtables.lock().unwrap();
        
        if immutable_tables.len() >= self.max_immutable_memtables {
            return Err("Too many immutable memtables, cannot freeze more until some are flushed".to_string());
        }
        
        let new_memtable = MemTable::new(None);
        
        // If WAL is enabled and we're freezing a memtable, we should create a new WAL segment
        // This can help in flushing the memtable to disk later
        if self.wal.is_some() {
            if let Some(ref wal) = self.wal {
                if let Err(e) = wal.lock().unwrap().flush() {
                    return Err(format!("Failed to flush WAL before freezing memtable: {}", e));
                }
            }
            
            let new_wal_path = self.data_dir.join(format!("wal_{}.log", immutable_tables.len() + 1));
            let new_wal = match WriteAheadLog::new(new_wal_path) {
                Ok(wal) => wal,
                Err(e) => return Err(format!("Failed to create new WAL: {}", e)),
            };
            
            if let Some(ref wal) = self.wal {
                let mut wal_guard = wal.lock().unwrap();
                *wal_guard = new_wal;
            }
        }
        
        let old_memtable = {
            let mut active = self.active_memtable.write().unwrap();
            std::mem::replace(&mut *active, new_memtable)
        };
        
        immutable_tables.push_back(Arc::new(old_memtable));
        self.maybe_schedule_compaction();
        Ok(())
    }
    
    /// Schedule a compaction if necessary
    fn maybe_schedule_compaction(&self) {
        // This would trigger a background task to flush immutable memtables to disk
        // We'll implement this in a future iteration
        
        // For now, we'll just log that a compaction might be needed
        let immutable_count = self.immutable_memtables.lock().unwrap().len();
        if immutable_count > 0 {
            println!("Compaction may be needed: {} immutable memtables", immutable_count);
        }
    }
    
    /// Manually trigger a flush of immutable memtables to disk
    pub fn flush(&self) -> Result<(), String> {
        // This would flush all immutable memtables to disk as SSTables
        // We'll implement this in a future iteration
        
        // For now, just acknowledge the request
        println!("Manual flush requested, but not yet implemented");
        Ok(())
    }
    
    /// Force a compaction of the LSM tree
    pub fn compact(&self) -> Result<(), String> {
        // This would trigger a full compaction of the LSM tree
        // We'll implement this in a future iteration
        
        // For now, just acknowledge the request
        println!("Compaction requested, but not yet implemented");
        Ok(())
    }
    
    /// Shutdown the storage engine, ensuring all data is flushed
    pub fn shutdown(&self) -> Result<(), String> {
        // Flush the WAL if enabled
        if let Some(ref wal) = self.wal {
            if let Err(e) = wal.lock().unwrap().flush() {
                return Err(format!("Failed to flush WAL during shutdown: {}", e));
            }
        }
        
        // Flush all memtables to disk
        // This is a placeholder for actual implementation
        println!("Shutdown requested, flushing all memtables");
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::storage::DiskManager;
    
    fn setup_test_env() -> (Arc<Mutex<BufferPoolManager>>, PathBuf) {
        // Create a temporary directory for the test
        let temp_dir = tempdir().unwrap();
        let data_dir = temp_dir.path().to_path_buf();
        
        // Create a disk manager
        let db_path = data_dir.join("test.db");
        let disk_manager = DiskManager::new(&db_path).unwrap();
        
        // Create a buffer pool manager
        let buffer_pool = BufferPoolManager::new(10, disk_manager);
        let buffer_pool = Arc::new(Mutex::new(buffer_pool));
        
        (buffer_pool, data_dir)
    }
    
    #[test]
    fn test_basic_operations() {
        let (buffer_pool, data_dir) = setup_test_env();
        
        // Create an LSM storage engine
        let storage = LSMStorage::new(
            buffer_pool,
            data_dir,
            Some(1024 * 1024), // 1MB memtable size
            5,                  // Max 5 immutable memtables
            true,               // Enable WAL for this test
        ).unwrap();
        
        // Put some data
        storage.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        storage.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        storage.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();
        
        // Get the data back
        assert_eq!(
            storage.get(b"key1").unwrap().as_ref(),
            b"value1"
        );
        assert_eq!(
            storage.get(b"key2").unwrap().as_ref(),
            b"value2"
        );
        assert_eq!(
            storage.get(b"key3").unwrap().as_ref(),
            b"value3"
        );
        
        // Delete a key
        storage.delete(b"key2".to_vec()).unwrap();
        
        // Verify it's gone
        assert!(storage.get(b"key2").is_none());
        
        // Update a key
        storage.put(b"key1".to_vec(), b"new_value1".to_vec()).unwrap();
        
        // Verify the update
        assert_eq!(
            storage.get(b"key1").unwrap().as_ref(),
            b"new_value1"
        );
        
        // // Test range scan
        // let scan_results = storage.scan(Some(b"key1".as_ref()), Some(b"key3".as_ref()));
        
        // // Should have 2 results (key1 and key3, since key2 was deleted)
        // assert_eq!(scan_results.len(), 2);
        
        // // Check that the results are as expected
        // let mut has_key1 = false;
        // let mut has_key3 = false;
        
        // for (key, value) in scan_results {
        //     if key == b"key1" {
        //         assert_eq!(value.as_ref(), b"new_value1");
        //         has_key1 = true;
        //     } else if key == b"key3" {
        //         assert_eq!(value.as_ref(), b"value3");
        //         has_key3 = true;
        //     }
        // }
        
        // assert!(has_key1);
        // assert!(has_key3);
    }
    
    #[test]
    fn test_memtable_freezing() {
        let (buffer_pool, data_dir) = setup_test_env();
        
        // Create an LSM storage engine with a very small memtable size
        let storage = LSMStorage::new(
            buffer_pool,
            data_dir,
            Some(100), // Very small memtable size to force freezing
            5,         // Max 5 immutable memtables
            true,      // Enable WAL for this test
        ).unwrap();
        
        // Put enough data to trigger a memtable freeze
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            storage.put(key, value).unwrap();
        }
        
        // Check that we have some immutable memtables
        let immutable_count = storage.immutable_memtables.lock().unwrap().len();
        assert!(immutable_count > 0, "Expected at least one immutable memtable");
        
        // Put more data and verify we can still retrieve all values
        for i in 10..20 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            storage.put(key, value).unwrap();
        }
        
        // Verify all keys are accessible
        for i in 0..20 {
            let key_string = format!("key{}", i);
            let key = key_string.as_bytes();
            let expected_value_string = format!("value{}", i);
            let expected_value = expected_value_string.as_bytes();
            
            let result = storage.get(key).unwrap();
            assert_eq!(result.as_ref(), expected_value);
        }
    }
}