use crate::buffer::BufferPoolManager;
use crate::storage::DiskManager;
use crate::shared::page::{Page, PageId, PAGE_SIZE};

use std::path::Path;
use std::fs::{self, File};
use std::io::Write;

fn create_temp_db_file() -> (String, DiskManager) {
    let temp_file = format!("/tmp/rusty_db_integration_test_{}.db", std::process::id());
    
    if Path::new(&temp_file).exists() {
        fs::remove_file(&temp_file).unwrap();
    }
    
    // Initialize with some empty pages
    let file = File::create(&temp_file).unwrap();
    file.set_len(PAGE_SIZE as u64 * 10).unwrap(); // 10 pages of 4KB each
    
    let disk_manager = DiskManager::new(&temp_file).unwrap();
    
    (temp_file, disk_manager)
}

fn cleanup_temp_file(file_path: &str) {
    if Path::new(file_path).exists() {
        fs::remove_file(file_path).unwrap();
    }
}

#[test]
fn test_buffer_pool_with_disk_manager() {
    let (temp_file, disk_manager) = create_temp_db_file();
    
    let pool_size = 5;
    let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
    
    // Create new pages and write data to them
    let (page_id1, page1) = buffer_pool.new_page(1).unwrap();
    let (page_id2, page2) = buffer_pool.new_page(2).unwrap();
    
    // Write data to pages
    {
        let mut p1 = page1.write().unwrap();
        p1.get_data_mut()[0] = 0x12;
        p1.get_data_mut()[1] = 0x34;
        p1.set_dirty(true);
        
        let mut p2 = page2.write().unwrap();
        p2.get_data_mut()[0] = 0x56;
        p2.get_data_mut()[1] = 0x78;
        p2.set_dirty(true);
    }
    
    // Unpin pages so they can be evicted
    assert!(buffer_pool.unpin_page(page_id1, true));
    assert!(buffer_pool.unpin_page(page_id2, true));
    
    // Flush pages to disk
    assert!(buffer_pool.flush_all_pages());
    
    // Create more pages to evict the previous ones
    for i in 2..pool_size+2 {
        let (page_id, _) = buffer_pool.new_page((i+1) as u64).unwrap();
        // Immediately unpin to ensure our previous pages can be evicted
        buffer_pool.unpin_page(page_id, false);
    }
    
    // Now fetch the original pages (should be read from disk)
    let fetched_page1 = buffer_pool.fetch_page(page_id1).unwrap();
    let fetched_page2 = buffer_pool.fetch_page(page_id2).unwrap();
    
    // Verify the data
    {
        let p1 = fetched_page1.read().unwrap();
        let p2 = fetched_page2.read().unwrap();
        
        assert_eq!(p1.get_data()[0], 0x12);
        assert_eq!(p1.get_data()[1], 0x34);
        
        assert_eq!(p2.get_data()[0], 0x56);
        assert_eq!(p2.get_data()[1], 0x78);
    }
    
    cleanup_temp_file(&temp_file);
}

#[test]
fn test_buffer_pool_eviction_mechanism() {
    let (temp_file, disk_manager) = create_temp_db_file();
    
    // Create a buffer pool with a small capacity
    let pool_size = 3;
    let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
    
    // Create and fill the buffer pool
    let mut page_ids = Vec::new();
    
    for i in 0..pool_size {
        let (page_id, page) = buffer_pool.new_page((i+1) as u64).unwrap();
        page_ids.push(page_id);
        
        // Write unique data
        {
            let mut p = page.write().unwrap();
            p.get_data_mut()[0] = i as u8;
            p.get_data_mut()[1] = (i * 2) as u8;
            p.set_dirty(true);
        }
        
        // Unpin the page so it can be evicted
        buffer_pool.unpin_page(page_id, true);
    }
    
    // Create more pages to trigger eviction
    for i in 0..pool_size {
        let (page_id, page) = buffer_pool.new_page((i + 1 + pool_size)  as u64).unwrap();
        
        // Write unique data
        {
            let mut p = page.write().unwrap();
            p.get_data_mut()[0] = (i + 100) as u8;
            p.set_dirty(true);
        }
        
        // Unpin the page
        buffer_pool.unpin_page(page_id, true);
    }
    
    // Now try to fetch the original pages (should be read from disk)
    for (i, page_id) in page_ids.iter().enumerate() {
        let fetched_page = buffer_pool.fetch_page(*page_id).unwrap();
        
        let p = fetched_page.read().unwrap();
        assert_eq!(p.get_data()[0], i as u8);
        assert_eq!(p.get_data()[1], (i * 2) as u8);
    }
    
    cleanup_temp_file(&temp_file);
}

#[test]
fn test_concurrent_page_access() {
    use std::thread;
    use std::sync::Arc;
    
    let (temp_file, disk_manager) = create_temp_db_file();
    
    let pool_size = 10;
    let buffer_pool = Arc::new(std::sync::Mutex::new(
        BufferPoolManager::new(pool_size, disk_manager)
    ));
    
    // Create a shared page that multiple threads will access
    let page_id = {
        let (id, _) = buffer_pool.lock().unwrap().new_page(1).unwrap();
        id
    };
    
    // Spawn multiple threads to increment a counter in the page
    let threads: Vec<_> = (0..5)
        .map(|_| {
            let pool_clone = Arc::clone(&buffer_pool);
            let page_id = page_id;
            
            thread::spawn(move || {
                for _ in 0..100 {
                    // Fetch the page
                    let page = {
                        let mut pool = pool_clone.lock().unwrap();
                        pool.fetch_page(page_id).unwrap()
                    };
                    
                    // Increment the counter (first 4 bytes as u32)
                    {
                        let mut p = page.write().unwrap();
                        let data = p.get_data_mut();
                        
                        let mut counter = u32::from_le_bytes([
                            data[0], data[1], data[2], data[3]
                        ]);
                        
                        counter += 1;
                        
                        let bytes = counter.to_le_bytes();
                        data[0] = bytes[0];
                        data[1] = bytes[1];
                        data[2] = bytes[2];
                        data[3] = bytes[3];
                        
                        p.set_dirty(true);
                    }
                    
                    // Unpin the page
                    {
                        let mut pool = pool_clone.lock().unwrap();
                        pool.unpin_page(page_id, true);
                    }
                }
            })
        })
        .collect();
    
    // Wait for all threads to complete
    for thread in threads {
        thread.join().unwrap();
    }
    
    let counter: u32;
    // Check the final counter value
    let final_page = buffer_pool.lock().unwrap().fetch_page(page_id).unwrap();
    let data = final_page.read().unwrap().get_data().clone();
    counter = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    // We expect 5 threads × 100 increments = 500
    assert_eq!(counter, 500);
    
    cleanup_temp_file(&temp_file);
}

#[test]
fn test_large_data_operation() {
    let (temp_file, disk_manager) = create_temp_db_file();
    
    let pool_size = 5;
    let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
    
    // Create a series of pages with sequential data
    let num_pages = 20; // More than buffer pool size to ensure eviction happens
    let mut page_ids = Vec::with_capacity(num_pages);
    
    for i in 0..num_pages {
        let (page_id, page) = buffer_pool.new_page((i+1) as u64).unwrap();
        page_ids.push(page_id);
        
        // Fill the page with a pattern based on its ID
        {
            let mut p = page.write().unwrap();
            for j in 0..64 { // Write 64 bytes of data (not the whole page for efficiency)
                p.get_data_mut()[j] = ((i * j) % 256) as u8;
            }
            p.set_dirty(true);
        }
        
        // Unpin the page so it can be evicted
        buffer_pool.unpin_page(page_id, true);
    }
    
    // Flush all pages to disk
    assert!(buffer_pool.flush_all_pages());
    
    // Now read pages in reverse order to test disk reads
    for (i, &page_id) in page_ids.iter().enumerate().rev() {
        let fetched_page = buffer_pool.fetch_page(page_id).unwrap();
        
        // Verify the data pattern
        {
            let p = fetched_page.read().unwrap();
            for j in 0..64 {
                assert_eq!(p.get_data()[j], ((i * j) % 256) as u8);
            }
        }
        
        // Unpin the page
        buffer_pool.unpin_page(page_id, false);
    }
    
    cleanup_temp_file(&temp_file);
}

#[test]
fn test_buffer_pool_recovery() {
    let (temp_file, disk_manager) = create_temp_db_file();
    
    // First session: Create and write data
    {
        let mut buffer_pool = BufferPoolManager::new(5, disk_manager);
        
        // Create pages with data
        let (page_id1, page1) = buffer_pool.new_page(1).unwrap();
        let (page_id2, page2) = buffer_pool.new_page(2).unwrap();
        
        // Write data
        {
            let mut p1 = page1.write().unwrap();
            p1.get_data_mut()[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
            p1.set_dirty(true);
            
            let mut p2 = page2.write().unwrap();
            p2.get_data_mut()[0..4].copy_from_slice(&[0xCA, 0xFE, 0xBA, 0xBE]);
            p2.set_dirty(true);
        }
        
        // Unpin and flush
        buffer_pool.unpin_page(page_id1, true);
        buffer_pool.unpin_page(page_id2, true);
        buffer_pool.flush_all_pages();
        _ = buffer_pool.close();
    }
    // First buffer pool is dropped here
    
    // Second session: Recover and verify data
    {
        let disk_manager = DiskManager::new(&temp_file).unwrap();
        let mut buffer_pool = BufferPoolManager::new(5, disk_manager);
        
        // Read page 1
        let page1 = buffer_pool.fetch_page(1).unwrap();
        {
            let p = page1.read().unwrap();
            assert_eq!(&p.get_data()[0..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
        }
        
        // Read page 2
        let page2 = buffer_pool.fetch_page(2).unwrap();
        {
            let p = page2.read().unwrap();
            assert_eq!(&p.get_data()[0..4], &[0xCA, 0xFE, 0xBA, 0xBE]);
        }
    }
    
    cleanup_temp_file(&temp_file);
}