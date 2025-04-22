mod buffer;
mod shared;
mod storage;
mod tests;

use std::path::Path;

use buffer::BufferPoolManager;
use shared::page::PageId;
use storage::DiskManager;

fn main() {
    println!("Rusty DB Buffer Pool Manager Demo");

    let db_path = Path::new("test_db.rdb");
    let disk_manager = DiskManager::new(db_path).expect("Failed to create disk manager");
    
    // Create a buffer pool manager with 10 pages
    let mut buffer_pool_manager = BufferPoolManager::new(10, disk_manager);
    
    // Create some new pages and write data to them
    println!("Creating new pages...");
    for _i in 0..5 {
        if let Some((page_id, page_arc)) = buffer_pool_manager.new_page() {
            println!("Created page with ID: {}", page_id);
            
            // Write some data to the page
            let mut page = page_arc.write().unwrap();
            let data = format!("This is test data for page {}", page_id);
            page.copy_from(data.as_bytes());
            
            // Unpin the page and mark it as dirty
            drop(page); // Release the write lock
            buffer_pool_manager.unpin_page(page_id, true);
        }
    }
    
    println!("\nFlushing all pages to disk...");
    buffer_pool_manager.flush_all_pages();
    
    // Fetch a page we created earlier
    println!("\nFetching and reading pages from buffer pool...");
    // Note: In a real application, you'd know the specific page IDs to fetch
    // For this demo, we'll fetch the first few pages created by the manager
    for page_id in 1..3 {
        if let Some(page_arc) = buffer_pool_manager.fetch_page(page_id) {
            let page = page_arc.read().unwrap();
            
            // Read the data from the page
            let data = page.get_data();
            let mut readable_data = String::new();
            for byte in data.iter() {
                if *byte == 0 {
                    break;
                }
                readable_data.push(*byte as char);
            }
            
            println!("Page {}: '{}'", page_id, readable_data);
            
            // Unpin the page
            drop(page); // Release the read lock
            buffer_pool_manager.unpin_page(page_id, false);
        } else {
            println!("Failed to fetch page {}", page_id);
        }
    }
    
    // Delete a page
    println!("\nDeleting a page...");
    let delete_page_id: PageId = 1;
    if buffer_pool_manager.delete_page(delete_page_id) {
        println!("Deleted page {}", delete_page_id);
    } else {
        println!("Failed to delete page {}", delete_page_id);
    }
    
    // Try to fetch the deleted page
    println!("\nTrying to fetch deleted page...");
    if let Some(_) = buffer_pool_manager.fetch_page(delete_page_id) {
        println!("Unexpectedly fetched deleted page {}", delete_page_id);
    } else {
        println!("As expected, couldn't fetch deleted page {}", delete_page_id);
    }
    
    println!("\nRusty DB Buffer Pool Manager Demo completed!");
}
