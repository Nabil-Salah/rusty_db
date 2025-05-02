mod buffer;
mod shared;
mod storage;
mod tests;
mod wal;
mod memtable;
mod lsm_storage;
mod block;
mod sstable;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use buffer::BufferPoolManager;
use storage::DiskManager;
use lsm_storage::LSMStorage;

fn main() {
    println!("Rusty DB Demo");

    // First: LSM Storage Demo
    println!("\n=== Rusty DB LSM Storage Demo ===");
    
    // Set up the LSM storage
    let data_dir = PathBuf::from("./data");
    
    // Create data directory if it doesn't exist
    if !data_dir.exists() {
        std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");
    }
    
    // Put the db file inside the data directory
    let db_path = data_dir.join("test_db.rdb");
    
    // Create a disk manager for the buffer pool
    let disk_manager = DiskManager::new(&db_path).expect("Failed to create disk manager");
    
    // Create a buffer pool manager with 100 pages
    let buffer_pool = BufferPoolManager::new(100, disk_manager);
    let buffer_pool = Arc::new(Mutex::new(buffer_pool));
    
    // Create an LSM storage engine
    let mut lsm_storage = LSMStorage::new(
        buffer_pool,
        data_dir,
        Some(1024 * 1024), // 1MB memtable size
        5,                 // Max 5 immutable memtables
        true,              // Enable WAL
    ).expect("Failed to create LSM storage");
    
    // Start the background flusher
    lsm_storage.start_background_flusher().expect("Failed to start background flusher");
    
    // Perform some naive put operations
    println!("Putting key-value pairs into LSM storage...");
    let test_pairs = [
        ("user:1001", "John Doe"),
        ("user:1002", "Jane Smith"),
        ("user:1003", "Bob Johnson"),
        ("config:theme", "dark"),
        ("config:language", "en-US")
    ];
    
    for (key, value) in test_pairs {
        match lsm_storage.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()) {
            Ok(_) => println!("  Put: {} = {}", key, value),
            Err(e) => println!("  Failed to put {}: {}", key, e)
        }
    }
    
    // Perform some naive get operations
    println!("\nGetting values from LSM storage...");
    for key in ["user:1001", "user:1002", "user:1003", "config:theme", "config:language", "non-existent-key"] {
        match lsm_storage.get(key.as_bytes()) {
            Some(value) => {
                let value_str = String::from_utf8_lossy(&value);
                println!("  Get: {} = {}", key, value_str);
            },
            None => println!("  Key not found: {}", key)
        }
    }
    
    // Delete a key
    println!("\nDeleting a key from LSM storage...");
    match lsm_storage.delete("user:1002".as_bytes().to_vec()) {
        Ok(_) => println!("  Deleted: user:1002"),
        Err(e) => println!("  Failed to delete user:1002: {}", e)
    }
    
    // Try to get the deleted key
    match lsm_storage.get("user:1002".as_bytes()) {
        Some(value) => println!("  Unexpectedly found deleted key: user:1002 = {}", String::from_utf8_lossy(&value)),
        None => println!("  Verified key was deleted: user:1002 is not found")
    }
    
    // Shutdown the LSM storage engine properly
    println!("\nShutting down LSM storage...");
    if let Err(e) = lsm_storage.shutdown() {
        println!("Error during LSM storage shutdown: {}", e);
    } else {
        println!("LSM storage shut down successfully");
    }
    
}

