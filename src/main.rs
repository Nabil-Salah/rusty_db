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
use std::io::{self, Write};

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
    
    logger::info!("Rusty DB Starting");

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
    
    // Run the interactive CLI
    run_interactive_cli(&mut lsm_storage);
    
    // Shutdown the LSM storage engine properly
    logger::info!("Shutting down LSM storage...");
    if let Err(e) = lsm_storage.shutdown() {
        logger::error!("Error during LSM storage shutdown: {}", e);
    } else {
        logger::info!("LSM storage shut down successfully");
    }
}

fn run_interactive_cli(lsm_storage: &mut LSMStorage) {
    println!("Welcome to Rusty DB interactive CLI!");
    println!("Available commands:");
    println!("  get <key>         - Retrieve a value by key");
    println!("  put <key> <value> - Store a key-value pair");
    println!("  delete <key>      - Delete a key");
    println!("  exit              - Exit the program");
    println!();
    
    let mut input = String::new();
    loop {
        print!("> ");
        io::stdout().flush().unwrap();
        
        input.clear();
        if io::stdin().read_line(&mut input).is_err() {
            println!("Error reading input. Please try again.");
            continue;
        }
        
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        
        match parts[0].to_lowercase().as_str() {
            "get" => {
                if parts.len() != 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                
                let key = parts[1].as_bytes();
                match lsm_storage.get(key) {
                    Ok(Some(value)) => {
                        match String::from_utf8(value.as_ref().to_vec()) {
                            Ok(value_str) => println!("Value: {}", value_str),
                            Err(_) => println!("Value (binary): {:?}", value),
                        }
                    },
                    Ok(None) => println!("Key not found: {}", parts[1]),
                    Err(e) => println!("Error retrieving key: {}", e),
                }
            },
            "put" => {
                if parts.len() < 3 {
                    println!("Usage: put <key> <value>");
                    continue;
                }
                
                let key = parts[1].as_bytes().to_vec();
                // Join the remaining parts to support values with spaces
                let value = parts[2..].join(" ").as_bytes().to_vec();
                
                match lsm_storage.put(key, value) {
                    Ok(_) => println!("Successfully stored the key-value pair"),
                    Err(e) => println!("Error storing key-value pair: {}", e),
                }
            },
            "delete" => {
                if parts.len() != 2 {
                    println!("Usage: delete <key>");
                    continue;
                }
                
                let key = parts[1].as_bytes().to_vec();
                match lsm_storage.delete(key) {
                    Ok(_) => println!("Successfully deleted the key"),
                    Err(e) => println!("Error deleting key: {}", e),
                }
            },
            "exit" => {
                println!("Exiting Rusty DB...");
                break;
            },
            _ => {
                println!("Unknown command: {}", parts[0]);
                println!("Available commands: get, put, delete, exit");
            }
        }
    }
}

