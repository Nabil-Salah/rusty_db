pub mod lru_replacer;
pub mod linked_lists;

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, RwLock};
use std::collections::LinkedList;

use crate::shared::page::{Page, PageId, FrameId};
use crate::storage::DiskManager;
use self::lru_replacer::LRUReplacer;

pub struct BufferPoolManager {
    pool_size: usize,
    pages: Vec<Arc<RwLock<Page>>>,
    free_list: LinkedList<FrameId>,
    page_table: HashMap<PageId, FrameId>,
    replacer: Mutex<LRUReplacer<FrameId>>,
    disk_manager: DiskManager,
}

impl BufferPoolManager {
    /// Create a new buffer pool manager
    pub fn new(pool_size: usize, disk_manager: DiskManager) -> Self {
        let mut pages = Vec::with_capacity(pool_size);
        let mut free_list = LinkedList::new();
        
        for i in 0..pool_size {
            pages.push(Arc::new(RwLock::new(Page::new(0))));
            free_list.push_back(i);
        }
        
        Self {
            pool_size,
            pages,
            free_list,
            page_table: HashMap::new(),
            replacer: Mutex::new(LRUReplacer::new()),
            disk_manager,
        }
    }
    
    /// Fetch a page from the buffer pool, reading it from disk if necessary
    pub fn fetch_page(&mut self, page_id: PageId) -> Option<Arc<RwLock<Page>>> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let page_arc = Arc::clone(&self.pages[frame_id]);
            {
                let mut page = page_arc.write().unwrap();
                page.pin();
                let mut replacer = self.replacer.lock().unwrap();
                replacer.erase(frame_id);
            }
            
            return Some(page_arc);
        }

        let frame_id = match self.get_free_frame() {
            Some(id) => id,
            None => return None,
        };
        let page_arc = Arc::clone(&self.pages[frame_id]);
        {
            let mut page = page_arc.write().unwrap();
            
            if page.get_page_id() != 0 && page.is_dirty() {
                let old_page_id = page.get_page_id();
                if let Err(e) = self.disk_manager.write_page(old_page_id, &page) {
                    eprintln!("Failed to write dirty page {} to disk: {}", old_page_id, e);
                }
                self.page_table.remove(&old_page_id);
            }
            
            if let Err(e) = self.disk_manager.read_page(page_id, &mut page) {
                eprintln!("Failed to read page {} from disk: {}", page_id, e);
                self.free_list.push_back(frame_id);
                return None;
            }
            
            page.set_page_id(page_id);
            page.pin();
            page.set_dirty(false);
        }
        
        self.page_table.insert(page_id, frame_id);
        
        Some(page_arc)
    }
    
    /// Create a new page in the buffer pool
    /// Never choose a page id of 0, as it is reserved for the metadata page
    pub fn new_page(&mut self, page_id: PageId) -> Option<(PageId, Arc<RwLock<Page>>)> {
        let frame_id = match self.get_free_frame() {
            Some(id) => id,
            None => return None,
        };
        
        let page_arc = self.pages[frame_id].clone();
        {
            let mut page = page_arc.write().unwrap();
            if page.get_page_id() != 0 && page.is_dirty() {
                let old_page_id = page.get_page_id();
                if let Err(e) = self.disk_manager.write_page(old_page_id, &page) {
                    eprintln!("Failed to write dirty page {} to disk: {}", old_page_id, e);
                    // Continue anyway, just losing the old page data
                }
                
                self.page_table.remove(&old_page_id);
            }
            
            page.reset();
            page.set_page_id(page_id);
            page.pin();
        }
        self.page_table.insert(page_id, frame_id);
        
        Some((page_id, page_arc))
    }
    
    /// Unpin a page, potentially allowing it to be evicted
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> bool {
        match self.page_table.get(&page_id) {
            Some(&frame_id) => {
                let page_arc = Arc::clone(&self.pages[frame_id]);
                let mut page = page_arc.write().unwrap();
                
                if is_dirty {
                    page.set_dirty(true);
                }
                
                page.unpin();
                
                if page.get_pin_count() == 0 {
                    let mut replacer = self.replacer.lock().unwrap();
                    replacer.insert(frame_id);
                }
                
                true
            },
            None => false,
        }
    }
    
    /// Flush a specific page to disk
    pub fn flush_page(&mut self, page_id: PageId) -> bool {
        match self.page_table.get(&page_id) {
            Some(&frame_id) => {
                let page_arc = Arc::clone(&self.pages[frame_id]);
                let mut page = page_arc.write().unwrap();
                
                match self.disk_manager.write_page(page_id, &page) {
                    Ok(_) => {
                        page.set_dirty(false);
                        true
                    },
                    Err(e) => {
                        eprintln!("Failed to flush page {} to disk: {}", page_id, e);
                        false
                    }
                }
            },
            None => false,
        }
    }
    
    /// Flush all pages in the buffer pool to disk
    pub fn flush_all_pages(&mut self) -> bool {
        let mut all_success = true;
        
        for (&page_id, _) in &self.page_table.clone() {
            if !self.flush_page(page_id) {
                all_success = false;
            }
        }
        
        all_success
    }
    
    /// Delete a page from the buffer pool and disk
    pub fn delete_page(&mut self, page_id: PageId) -> bool {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let page_arc = Arc::clone(&self.pages[frame_id]);
            let mut page = page_arc.write().unwrap();
            
            if page.get_pin_count() > 0 {
                return false;
            }
            
            page.reset();
            self.page_table.remove(&page_id);
            self.free_list.push_back(frame_id);

            let mut replacer = self.replacer.lock().unwrap();
            replacer.erase(frame_id);
        }
        
        match self.disk_manager.delete_page(page_id) {
            Ok(_) => true,
            Err(e) => {
                eprintln!("Failed to delete page {} from disk: {}", page_id, e);
                false
            }
        }
    }
    
    /// Get a free frame from the free list or by evicting a page
    fn get_free_frame(&mut self) -> Option<FrameId> {
        if let Some(frame_id) = self.free_list.pop_front() {
            return Some(frame_id);
        }
        
        let mut replacer = self.replacer.lock().unwrap();
        match replacer.victim() {
            Some(frame_id) => {
                let page_arc = Arc::clone(&self.pages[frame_id]);
                let page = page_arc.read().unwrap();
                
                self.page_table.remove(&page.get_page_id());
                
                Some(frame_id)
            },
            None => None,
        }
    }

    pub fn close(&mut self) -> io::Result<()>{
        self.disk_manager.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs::{self, File};
    use std::io::Write;

    fn create_temp_db_file() -> (String, DiskManager) {
        let temp_file = format!("/tmp/rusty_db_test_{}.db", std::process::id());
        
        if Path::new(&temp_file).exists() {
            fs::remove_file(&temp_file).unwrap();
        }
        
        // Initialize with a few empty pages
        let file = File::create(&temp_file).unwrap();
        file.set_len(4096 * 10).unwrap(); // 10 pages of 4KB each
        
        let disk_manager = DiskManager::new(&temp_file).unwrap();
        
        (temp_file, disk_manager)
    }
    
    fn cleanup_temp_file(file_path: &str) {
        if Path::new(&file_path).exists() {
            fs::remove_file(&file_path).unwrap();
        }
    }
    
    #[test]
    fn test_buffer_pool_creation() {
        let (temp_file, disk_manager) = create_temp_db_file();
        let pool_size = 5;
        let buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
        
        assert_eq!(buffer_pool.pool_size, pool_size);
        assert_eq!(buffer_pool.pages.len(), pool_size);
        assert_eq!(buffer_pool.free_list.len(), pool_size);
        assert!(buffer_pool.page_table.is_empty());
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_new_page() {
        let (temp_file, disk_manager) = create_temp_db_file();
        let pool_size = 5;
        let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
        
        // Create a new page
        let (page_id, page_arc) = buffer_pool.new_page(0).unwrap();
        assert_eq!(page_id, 0); // First page should have id 0
        
        // Check that the page is in the page table
        assert!(buffer_pool.page_table.contains_key(&page_id));
        
        // Check that the free list has one less entry
        assert_eq!(buffer_pool.free_list.len(), pool_size - 1);
        
        // Create another page
        let (page_id2, _) = buffer_pool.new_page(1).unwrap();
        assert_eq!(page_id2, 1); // Second page should have id 1
        
        // Verify the page properties
        {
            let page = page_arc.read().unwrap();
            assert_eq!(page.get_page_id(), 0);
            assert_eq!(page.get_pin_count(), 1);
            assert!(!page.is_dirty());
        }
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_fetch_and_unpin_page() {
        let (temp_file, disk_manager) = create_temp_db_file();
        let pool_size = 5;
        let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
        
        // Create a new page
        let (page_id, page_arc) = buffer_pool.new_page(0).unwrap();
        
        // Write some data to the page
        {
            let mut page = page_arc.write().unwrap();
            let data = page.get_data_mut();
            data[0] = 42;
            data[1] = 24;
            page.set_dirty(true);
        }
        
        // Unpin the page so it can be evicted
        assert!(buffer_pool.unpin_page(page_id, true));
        
        // The page should now be in the replacer
        assert_eq!(buffer_pool.replacer.lock().unwrap().size(), 1);
        
        // Fetch the page again
        let fetched_page = buffer_pool.fetch_page(page_id).unwrap();
        
        // Verify the data
        {
            let page = fetched_page.read().unwrap();
            assert_eq!(page.get_data()[0], 42);
            assert_eq!(page.get_data()[1], 24);
            assert_eq!(page.get_pin_count(), 1);
        }
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_page_eviction() {
        let (temp_file, disk_manager) = create_temp_db_file();
        let pool_size = 2;
        let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
        
        // Fill the buffer pool
        let (page_id1, _) = buffer_pool.new_page(0).unwrap();
        let (page_id2, _) = buffer_pool.new_page(1).unwrap();
        
        // Unpin both pages
        assert!(buffer_pool.unpin_page(page_id1, true));
        assert!(buffer_pool.unpin_page(page_id2, false));
        
        // Create a new page, should evict one of the previous pages
        let (page_id3, _) = buffer_pool.new_page(2).unwrap();
        
        // The buffer pool should still have only two pages in the page table
        assert_eq!(buffer_pool.page_table.len(), 2);
        
        // One of the previous pages should be evicted
        assert!(buffer_pool.page_table.contains_key(&page_id3));
        
        // Either page_id1 or page_id2 should be evicted
        assert!(
            !buffer_pool.page_table.contains_key(&page_id1) || 
            !buffer_pool.page_table.contains_key(&page_id2)
        );
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_flush_page() {
        let (temp_file, disk_manager) = create_temp_db_file();
        let pool_size = 3;
        let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
        
        // Create a new page
        let (page_id, page_arc) = buffer_pool.new_page(0).unwrap();
        
        // Write some data to the page
        {
            let mut page = page_arc.write().unwrap();
            let data = page.get_data_mut();
            data[0] = 99;
            data[1] = 88;
            page.set_dirty(true);
        }
        
        // Flush the page to disk
        assert!(buffer_pool.flush_page(page_id));
        
        // Unpin the page
        assert!(buffer_pool.unpin_page(page_id, false));
        
        // Evict the page by filling the buffer pool
        for i in 1..=pool_size {
            buffer_pool.new_page(i as PageId).unwrap();
        }
        
        // Unpin at least one page to make room in buffer pool
        assert!(buffer_pool.unpin_page(1, false));
        
        // Fetch the page again, should read from disk
        let fetched_page = buffer_pool.fetch_page(page_id).unwrap();
        
        // Verify the data was persisted
        {
            let page = fetched_page.read().unwrap();
            assert_eq!(page.get_data()[0], 99);
            assert_eq!(page.get_data()[1], 88);
        }
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_delete_page() {
        let (temp_file, disk_manager) = create_temp_db_file();
        let pool_size = 3;
        let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
        
        // Create a new page
        let (page_id, _) = buffer_pool.new_page(1).unwrap();
        
        // Unpin the page so it can be deleted
        assert!(buffer_pool.unpin_page(page_id, false));
        
        // Delete the page
        assert!(buffer_pool.delete_page(page_id));
        
        // Page should no longer be in the page table
        assert!(!buffer_pool.page_table.contains_key(&page_id));
        
        // Trying to fetch the deleted page should fail or create a new one
        let result = buffer_pool.fetch_page(page_id);
        assert!(result.is_none() || result.unwrap().read().unwrap().get_data()[0] == 0);
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_flush_all_pages() {
        let (temp_file, disk_manager) = create_temp_db_file();
        let pool_size = 3;
        let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
        
        // Create several pages with data
        let (page_id1, page_arc1) = buffer_pool.new_page(0).unwrap();
        let (page_id2, page_arc2) = buffer_pool.new_page(1).unwrap();
        
        // Write data to pages
        {
            let mut page1 = page_arc1.write().unwrap();
            let data1 = page1.get_data_mut();
            data1[0] = 11;
            page1.set_dirty(true);
            
            let mut page2 = page_arc2.write().unwrap();
            let data2 = page2.get_data_mut();
            data2[0] = 22;
            page2.set_dirty(true);
        }
        
        // Flush all pages
        assert!(buffer_pool.flush_all_pages());
        
        // Unpin pages
        buffer_pool.unpin_page(page_id1, false);
        buffer_pool.unpin_page(page_id2, false);
        
        // Evict pages by creating new ones
        for i in 2..=4 {
            buffer_pool.new_page(i).unwrap();
        }
        
        // Unpin some pages to make room
        buffer_pool.unpin_page(3, false);
        buffer_pool.unpin_page(2, false);
        
        // Fetch pages again
        let fetched_page1 = buffer_pool.fetch_page(page_id1).unwrap();
        let fetched_page2 = buffer_pool.fetch_page(page_id2).unwrap();
        
        // Verify data persisted
        {
            let page1 = fetched_page1.read().unwrap();
            let page2 = fetched_page2.read().unwrap();
            
            assert_eq!(page1.get_data()[0], 11);
            assert_eq!(page2.get_data()[0], 22);
        }
        
        cleanup_temp_file(&temp_file);
    }
}