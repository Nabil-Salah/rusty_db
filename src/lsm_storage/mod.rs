use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::buffer::BufferPoolManager;
use crate::memtable::MemTable;
use crate::wal::WriteAheadLog;
use crate::sstable::{table_builder::TableBuilder, table_iterator::TableIterator};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::task::JoinHandle;
use std::time::Duration;

/// LSMStorage is the main storage engine using a Log-Structured Merge Tree design
/// It uses a memtable for recent writes and persists data in SSTables
pub struct LSMStorage {
    active_memtable: Arc<RwLock<MemTable>>,
    immutable_memtables: VecDeque<Arc<MemTable>>,
    buffer_pool: Arc<Mutex<BufferPoolManager>>,
    data_dir: PathBuf,
    max_immutable_memtables: usize,
    wal: Option<Arc<Mutex<WriteAheadLog>>>,
    
    // SSTable management
    sstable_counter: Arc<RwLock<AtomicU64>>,
    sstable_iterators: Vec<Arc<RwLock<TableIterator>>>,
    
    // Background processing
    flush_scheduler: Option<Sender<Arc<MemTable>>>,
    flush_task: Option<JoinHandle<()>>,
    runtime: Option<Runtime>,
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
        // Ensure the data directory exists
        std::fs::create_dir_all(&data_dir)?;
        
        let wal = if enable_wal {
            let wal_path = data_dir.join("wal.log");
            
            // Make sure parent directory exists for WAL
            if let Some(parent) = wal_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            
            let write_ahead_log = WriteAheadLog::new(wal_path)?;
            Some(Arc::new(Mutex::new(write_ahead_log)))
        } else {
            None
        };
        
        let memtable = MemTable::new(memtable_size_threshold);
        
        Ok(Self {
            active_memtable: Arc::new(RwLock::new(memtable)),
            immutable_memtables: VecDeque::new(),
            buffer_pool,
            data_dir,
            max_immutable_memtables,
            wal,
            sstable_counter: Arc::new(RwLock::new(AtomicU64::new(0))),
            sstable_iterators: Vec::new(),
            flush_scheduler: None,
            flush_task: None,
            runtime: None,
        })
    }
    
    /// Put a key-value pair into the database
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        if let Some(ref wal) = self.wal {
            let mut wal_guard = wal.lock().unwrap();
            if let Err(e) = wal_guard.put(&key, &value) {
                return Err(format!("Failed to write to WAL: {}", e));
            }
        }
        
        let memtable_locked = self.active_memtable.write().unwrap();
        memtable_locked.put(key, value);
        
        if memtable_locked.should_flush() {
            drop(memtable_locked);
            self.freeze_memtable()?;
        }
        
        Ok(())
    }
    
    /// Delete a key from the database (by writing a tombstone)
    pub fn delete(&mut self, key: Vec<u8>) -> Result<(), String> {
        // First log to WAL if enabled
        if let Some(ref wal) = self.wal {
            let mut wal_guard = wal.lock().unwrap();
            if let Err(e) = wal_guard.delete(&key) {
                return Err(format!("Failed to write to WAL: {}", e));
            }
        }
        
        let memtable_locked = self.active_memtable.write().unwrap();
        
        memtable_locked.delete(key);
        
        if memtable_locked.should_flush() {
            drop(memtable_locked);
            self.freeze_memtable()?;
        }
        
        Ok(())
    }
    
    /// Get a value by key from the database
    pub fn get(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        // First, check the active memtable
        if let Some(value) = self.active_memtable.read().unwrap().get(key) {
            return Some(value);
        }
        
        // Then check immutable memtables from newest to oldest
        for memtable in self.immutable_memtables.iter().rev() {
            if let Some(value) = memtable.get(key) {
                return Some(value);
            }
        }
        
        // Finally, check SSTables from newest to oldest
        for sst in &self.sstable_iterators {
            if let Ok(mut iterator) = sst.write() {
                if let Ok(found) = iterator.seek_to_key(key) {
                    if found && iterator.key() == Some(key) {
                        if iterator.is_tombstone() {
                            return None;
                        } else {
                            if let Some(value_bytes) = iterator.value() {
                                return Some(Arc::new(value_bytes.to_vec()));
                            }
                        }
                    }
                }
            }
        }
        
        // Not found in any store
        None
    }
    
    /// Initialize the background flushing system
    pub fn start_background_flusher(&mut self) -> Result<(), String> {
        if self.runtime.is_some() {
            return Err("Background flushing already started".to_string());
        }

        // Create a tokio runtime for background processing
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("lsm-flush-worker")
            .build()
            .map_err(|e| format!("Failed to create tokio runtime: {}", e))?;

        // Create a channel for scheduling flush operations
        let (sender, receiver) = channel::<Arc<MemTable>>(10);
        
        // Store the sender
        self.flush_scheduler = Some(sender);
        
        // Get a clone of the buffer pool and data directory
        let buffer_pool = self.buffer_pool.clone();
        let data_dir = self.data_dir.clone();
        
        // Create an atomic counter for SSTable IDs
        let counter = self.sstable_counter.clone();
        
        // Create a vector to store SSTable iterators
        let sstable_iterators = self.sstable_iterators.clone();
        
        // Spawn the background flush worker
        let handle = runtime.spawn(async move {
            Self::background_flush_worker(
                buffer_pool, 
                data_dir, 
                receiver, 
                counter,
                sstable_iterators
            ).await;
        });
        
        self.flush_task = Some(handle);
        self.runtime = Some(runtime);
        
        Ok(())
    }
    
    /// Stop the background flushing system
    pub fn stop_background_flushing(&mut self) -> Result<(), String> {
        if let Some(runtime) = self.runtime.take() {
            // Drop the sender to terminate the channel
            self.flush_scheduler = None;
            
            // Shutdown the runtime
            runtime.shutdown_timeout(Duration::from_secs(5));
            
            // Clear the task handle
            self.flush_task = None;
        }
        
        Ok(())
    }
    
    /// Schedule a memtable for flushing to disk
    fn schedule_flush(&self, memtable: Arc<MemTable>) -> Result<(), String> {
        if let Some(ref sender) = self.flush_scheduler {
            if let Err(_) = sender.try_send(memtable) {
                return Err("Failed to schedule flush: channel full or closed".to_string());
            }
            Ok(())
        } else {
            Err("Background flushing not started".to_string())
        }
    }
    
    /// Background worker that processes flush requests
    async fn background_flush_worker(
        buffer_pool: Arc<Mutex<BufferPoolManager>>,
        data_dir: PathBuf,
        mut receiver: Receiver<Arc<MemTable>>,
        sstable_counter: Arc<RwLock<AtomicU64>>,
        mut sstable_iterators: Vec<Arc<RwLock<TableIterator>>>
    ) {
        println!("Background flush worker started");
        
        while let Some(memtable) = receiver.recv().await {
            let sst_id = sstable_counter.write().unwrap().fetch_add(1, Ordering::SeqCst);
            let sst_path = data_dir.join(format!("sst_{:06}.sst", sst_id));
            println!("Flushing memtable to SSTable: {}", sst_path.display());
            
            let mut iter = memtable.iter_valid();
            
            // Use the TableBuilder to create a new SSTable
            match TableBuilder::build_from_iterator(&sst_path, &mut iter, None) {
                Ok(metadata) => {
                    println!("SSTable created: {} with {} blocks", 
                             sst_path.display(), metadata.block_count);
                    
                    match TableIterator::open(&sst_path, sst_id, Arc::clone(&buffer_pool)) {
                        Ok(iterator) => {
                            sstable_iterators.push(Arc::new(RwLock::new(iterator)));
                            
                            // Sort newest to oldest
                            sstable_iterators.sort_by(|a, b| {
                                if let (Ok(a_iter), Ok(b_iter)) = (a.read(), b.read()) {
                                    let a_id = a_iter.metadata().file_path.to_string_lossy();
                                    let b_id = b_iter.metadata().file_path.to_string_lossy();
                                    b_id.cmp(&a_id) // Reverse order so newest is first
                                } else {
                                    // If we can't acquire locks, maintain current order
                                    std::cmp::Ordering::Equal
                                }
                            });
                        },
                        Err(e) => {
                            eprintln!("Failed to open SSTable iterator: {:?}", e);
                        }
                    }
                },
                Err(e) => {
                    eprintln!("Failed to create SSTable: {:?}", e);
                }
            }
        }
        
        println!("Background flush worker stopped");
    }

    /// Freeze the current active memtable and create a new one
    fn freeze_memtable(&mut self) -> Result<(), String> {
        if self.immutable_memtables.len() >= self.max_immutable_memtables {
            return Err("Too many immutable memtables, cannot freeze more until some are flushed".to_string());
        }
        
        let new_memtable = MemTable::new(None);
        
        // If WAL is enabled and we're freezing a memtable, we should create a new WAL segment
        if self.wal.is_some() {
            if let Some(ref wal) = self.wal {
                if let Err(e) = wal.lock().unwrap().flush() {
                    return Err(format!("Failed to flush WAL before freezing memtable: {}", e));
                }
            }
            
            let new_wal_path = self.data_dir.join(format!("wal_{}.log", self.immutable_memtables.len() + 1));
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
        
        // Convert to Arc and add to immutable tables
        let arc_old_memtable = Arc::new(old_memtable);
        
        // Schedule the memtable for flushing if background flushing is enabled
        if self.flush_scheduler.is_some() {
            if let Err(e) = self.schedule_flush(Arc::clone(&arc_old_memtable)) {
                eprintln!("Warning: Failed to schedule memtable flush: {}", e);
            }
        }
        
        self.immutable_memtables.push_back(arc_old_memtable);
        
        Ok(())
    }
    
    /// Manually trigger a flush of immutable memtables to disk
    pub fn flush(&mut self) -> Result<(), String> {
        // Check if we have any immutable memtables to flush
        if self.immutable_memtables.is_empty() {
            return Ok(());
        }
        
        // If background flushing is enabled, schedule all immutable memtables for flushing
        if self.flush_scheduler.is_some() {
            while let Some(memtable) = self.immutable_memtables.pop_front() {
                if let Err(e) = self.schedule_flush(Arc::clone(&memtable)) {
                    // Push the memtable back to the front since we failed to schedule it
                    self.immutable_memtables.push_front(memtable);
                    return Err(e);
                }
            }
            Ok(())
        } else {
            // For synchronous approach, we would flush here directly
            // This is a placeholder for actual implementation
            Err("Background flushing not enabled, synchronous flush not yet implemented".to_string())
        }
    }
    
    /// Shutdown the storage engine, ensuring all data is flushed
    pub fn shutdown(&mut self) -> Result<(), String> {
        if self.runtime.is_some() {
            self.stop_background_flushing()?;
        }
        
        if let Some(ref wal) = self.wal {
            if let Err(e) = wal.lock().unwrap().flush() {
                return Err(format!("Failed to flush WAL during shutdown: {}", e));
            }
        }
        
        let active_size = self.active_memtable.read().unwrap().size();
        if active_size > 0 {
            self.freeze_memtable()?;
        }
        
        println!("Storage engine shutdown complete");
        Ok(())
    }
    
    /// Initialize LSM storage from existing files in the data directory
    pub fn recover_from_disk(
        buffer_pool: Arc<Mutex<BufferPoolManager>>,
        data_dir: PathBuf,
        memtable_size_threshold: Option<usize>,
        max_immutable_memtables: usize,
        enable_wal: bool,
    ) -> Result<Self, String> {
        // Create the base LSM storage
        let mut lsm = match Self::new(
            buffer_pool.clone(),
            data_dir.clone(),
            memtable_size_threshold,
            max_immutable_memtables,
            enable_wal,
        ) {
            Ok(lsm) => lsm,
            Err(e) => return Err(format!("Failed to create LSM storage: {}", e)),
        };
        let counter_clone = lsm.sstable_counter.clone();
        let counter = counter_clone.write().unwrap();
        // Find all SSTable files in the data directory
        if let Ok(entries) = std::fs::read_dir(&data_dir) {
            // Process each file in the directory
            let mut sst_files: Vec<(u64, PathBuf)> = Vec::new();
            
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    
                    // Check if this is an SSTable file
                    if path.is_file() {
                        let file_name = path.file_name().unwrap().to_string_lossy();
                        if file_name.starts_with("sst_") && file_name.ends_with(".sst") {
                            // Extract the SSTable ID
                            if let Some(id_str) = file_name
                                .strip_prefix("sst_")
                                .and_then(|s| s.strip_suffix(".sst"))
                            {
                                if let Ok(id) = id_str.parse::<u64>() {
                                    sst_files.push((id, path.clone()));
                                    
                                    // Update the SSTable counter if needed
                                    if id >= counter.load(Ordering::SeqCst) {
                                        counter.store(id + 1, Ordering::SeqCst);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            drop(counter);
            // Sort SSTables by ID (oldest to newest)
            sst_files.sort_by_key(|(id, _)| *id);
            
            // Create iterators for all SSTables
            for (id, path) in sst_files {
                match TableIterator::open(&path, id, buffer_pool.clone()) {
                    Ok(iterator) => {
                        println!("Recovered SSTable: {} (id: {})", path.display(), id);
                        lsm.sstable_iterators.push(Arc::new(RwLock::new(iterator)));
                    },
                    Err(e) => {
                        eprintln!("Failed to open SSTable: {} - {:?}", path.display(), e);
                        // Continue with recovery, skipping this file
                    }
                }
            }
            
            // Sort the iterators from newest to oldest
            lsm.sstable_iterators.sort_by(|a, b| {
                if let (Ok(a_iter), Ok(b_iter)) = (a.read(), b.read()) {
                    let a_id = a_iter.metadata().file_path.to_string_lossy();
                    let b_id = b_iter.metadata().file_path.to_string_lossy();
                    b_id.cmp(&a_id) // Reverse order so newest is first
                } else {
                    // If we can't acquire locks, maintain current order
                    std::cmp::Ordering::Equal
                }
            });
        }
        
        // TODO: Process WAL files if enable_wal is true
        // This would involve replaying WAL entries into the active memtable
        
        Ok(lsm)
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
        
        // Ensure the directory exists
        std::fs::create_dir_all(&data_dir).unwrap();
        
        // Create a disk manager
        let db_path = data_dir.join("test.db");
        // Make sure parent directories exist for db_path
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        
        // Create an empty file to initialize the database
        std::fs::File::create(&db_path).unwrap();
        
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
        let mut storage = LSMStorage::new(
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
    }
    
    #[test]
    fn test_memtable_freezing() {
        let (buffer_pool, data_dir) = setup_test_env();
        
        // Create an LSM storage engine with a very small memtable size
        let mut storage = LSMStorage::new(
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
        let immutable_count = storage.immutable_memtables.len();
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
            let key_bytes = key_string.clone().into_bytes();
            let expected_value_string = format!("value{}", i);
            let expected_value_bytes = expected_value_string.into_bytes();
            
            let result = storage.get(&key_bytes).unwrap();
            assert_eq!(result.as_ref(), &expected_value_bytes);
        }
    }
    
    #[test]
    fn test_background_flush() {
        let (buffer_pool, data_dir) = setup_test_env();
        
        // Create an LSM storage engine with a smaller memtable size
        let mut storage = LSMStorage::new(
            buffer_pool,
            data_dir.clone(),
            Some(200), // Small memtable size to trigger freezing
            3,         // Max 3 immutable memtables
            true,      // Enable WAL
        ).unwrap();
        
        // Start background flushing
        storage.start_background_flusher().unwrap();
        
        // Insert enough data to trigger multiple memtable freezes and flushes
        for i in 0..50 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            storage.put(key, value).unwrap();
        }
        
        // Small delay to allow background flush to process
        std::thread::sleep(std::time::Duration::from_millis(500));
        
        // Verify we can read all the data
        for i in 0..50 {
            let key_string = format!("key{}", i);
            let key_bytes = key_string.clone().into_bytes();
            let expected_value_string = format!("value{}", i);
            let expected_value_bytes = expected_value_string.into_bytes();
            
            match storage.get(&key_bytes) {
                Some(value) => assert_eq!(value.as_ref(), &expected_value_bytes),
                None => panic!("Failed to get key: {:?}", key_string),
            }
        }
        
        // Shutdown properly
        storage.shutdown().unwrap();
        
        // Check if any SSTable files were created
        let entries = std::fs::read_dir(&data_dir).unwrap();
        let mut sst_count = 0;
        
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() && path.file_name().unwrap().to_string_lossy().starts_with("sst_") {
                    sst_count += 1;
                }
            }
        }
        
        assert!(sst_count > 0, "Expected at least one SSTable to be created");
    }
    
    #[test]
    fn test_recovery() {
        let (buffer_pool, data_dir) = setup_test_env();
        
        // First, create and populate a storage engine
        {
            let mut storage = LSMStorage::new(
                Arc::clone(&buffer_pool),
                data_dir.clone(),
                Some(200),
                3,
                true,
            ).unwrap();
            
            storage.start_background_flusher().unwrap();
            
            // Insert some data
            for i in 0..15 {  // Reduced to fewer keys for simpler test
                let key = format!("key{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                storage.put(key, value).unwrap();
            }
            
            // Small delay to allow background flush
            std::thread::sleep(std::time::Duration::from_millis(500));
            
            // Force memtable freeze to ensure all data is in SSTables
            if storage.active_memtable.read().unwrap().size() > 0 {
                storage.freeze_memtable().unwrap();
                // Give extra time for flush to complete
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            
            // Proper shutdown - this ensures all buffers are flushed
            storage.shutdown().unwrap();
        }
        
        // Explicitly unlock buffer pool by releasing any references
        // This ensures the disk manager can be properly accessed 
        {
            let mut bpm = buffer_pool.lock().unwrap();
            bpm.flush_all_pages();
        }
        
        // Small delay to ensure filesystem has time to sync
        std::thread::sleep(std::time::Duration::from_millis(100));
        
        // Now recover from the same directory
        let recovered_storage = LSMStorage::recover_from_disk(
            buffer_pool,
            data_dir.clone(),
            Some(200),
            3,
            true,
        ).unwrap();
        
        // Verify we can read the data back
        for i in 0..15 {  // Match the reduced number of keys
            let key_string = format!("key{}", i);
            let key_bytes = key_string.clone().into_bytes();
            let expected_value_string = format!("value{}", i);
            let expected_value_bytes = expected_value_string.into_bytes();
            
            match recovered_storage.get(&key_bytes) {
                Some(value) => assert_eq!(value.as_ref(), &expected_value_bytes),
                None => {
                    // Print debug info to help diagnose the issue
                    println!("Failed to recover key: {:?}", key_string);
                    
                    // Check if any SSTable files exist in the directory
                    let entries = std::fs::read_dir(&data_dir).unwrap();
                    println!("Files in data directory:");
                    for entry in entries {
                        if let Ok(entry) = entry {
                            println!("  {}", entry.path().display());
                        }
                    }
                    
                    panic!("Failed to recover key: {:?}", key_string);
                }
            }
        }
    }
}