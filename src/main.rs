mod buffer;
mod shared;
mod storage;
mod tests;
mod wal;
mod memtable;
mod lsm_storage;
mod block;
mod sstable;
mod logger;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use buffer::BufferPoolManager;
use storage::DiskManager;
use lsm_storage::LSMStorage;

fn main() {
    // Initialize logger - check for debug mode flag or environment variable
    let debug_mode = std::env::var("RUSTY_DB_DEBUG")
        .map(|val| val == "1" || val.to_lowercase() == "true")
        .unwrap_or(false);
    
    // Initialize the logger
    logger::init(debug_mode).expect("Failed to initialize logger");
    
    logger::info!("Rusty DB Demo");

    // First: LSM Storage Demo
    logger::info!("=== Rusty DB LSM Storage Demo ===");
    
    // Set up the LSM storage
    let data_dir = PathBuf::from("./data");
    
    // Create data directory if it doesn't exist
    if !data_dir.exists() {
        std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");
        logger::debug!("Created data directory at: {}", data_dir.display());
    }
    
    // Put the db file inside the data directory
    let db_path = data_dir.join("test_db.rdb");
    logger::debug!("Database file path: {}", db_path.display());
    
    // Create a disk manager for the buffer pool
    let disk_manager = match DiskManager::new(&db_path) {
        Ok(dm) => {
            logger::debug!("Disk manager created successfully");
            dm
        },
        Err(e) => {
            logger::error!("Failed to create disk manager: {}", e);
            panic!("Failed to create disk manager: {}", e);
        }
    };
    
    // Create a buffer pool manager with 100 pages
    let buffer_pool = BufferPoolManager::new(100, disk_manager);
    let buffer_pool = Arc::new(Mutex::new(buffer_pool));
    logger::debug!("Buffer pool manager created with 100 pages");
    
    // Create an LSM storage engine
    let mut lsm_storage = match LSMStorage::new(
        buffer_pool,
        data_dir,
        Some(1024 * 1024), // 1MB memtable size
        5,                 // Max 5 immutable memtables
        true,              // Enable WAL
    ) {
        Ok(storage) => {
            logger::debug!("LSM storage created successfully");
            storage
        },
        Err(e) => {
            logger::error!("Failed to create LSM storage: {}", e);
            panic!("Failed to create LSM storage: {}", e);
        }
    };
    
    // Start the background flusher
    if let Err(e) = lsm_storage.start_background_flusher() {
        logger::error!("Failed to start background flusher: {}", e);
        panic!("Failed to start background flusher: {}", e);
    }
    logger::debug!("Background flusher started successfully");
    
    // Perform some naive put operations
    logger::info!("Putting key-value pairs into LSM storage...");
    let test_pairs = [
        ("user:1001", "John Doe"),
        ("user:1002", "Jane Smith"),
        ("user:1003", "Bob Johnson"),
        ("config:theme", "dark"),
        ("config:language", "en-US")
    ];
    
    for (key, value) in test_pairs {
        match lsm_storage.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()) {
            Ok(_) => logger::info!("  Put: {} = {}", key, value),
            Err(e) => logger::error!("  Failed to put {}: {}", key, e)
        }
    }
    
    // Perform some naive get operations
    logger::info!("\nGetting values from LSM storage...");
    for key in ["user:1001", "user:1002", "user:1003", "config:theme", "config:language", "non-existent-key"] {
        match lsm_storage.get(key.as_bytes()) {
            Ok(Some(value)) => {
                let value_str = String::from_utf8_lossy(&value);
                logger::info!("  Get: {} = {}", key, value_str);
            },
            Ok(None) => logger::info!("  Key not found: {}", key),
            Err(e) => logger::error!("  Error retrieving key {}: {}", key, e)
        }
    }
    
    // Delete a key
    logger::info!("\nDeleting a key from LSM storage...");
    match lsm_storage.delete("user:1002".as_bytes().to_vec()) {
        Ok(_) => logger::info!("  Deleted: user:1002"),
        Err(e) => logger::error!("  Failed to delete user:1002: {}", e)
    }
    
    // Try to get the deleted key
    match lsm_storage.get("user:1002".as_bytes()) {
        Ok(Some(value)) => logger::warn!("  Unexpectedly found deleted key: user:1002 = {}", String::from_utf8_lossy(&value)),
        Ok(None) => logger::info!("  Verified key was deleted: user:1002 is not found"),
        Err(e) => logger::error!("  Error checking deleted key: {}", e)
    }
    
    // Shutdown the LSM storage engine properly
    logger::info!("\nShutting down LSM storage...");
    if let Err(e) = lsm_storage.shutdown() {
        logger::error!("Error during LSM storage shutdown: {}", e);
    } else {
        logger::info!("LSM storage shut down successfully");
    }
}

