pub mod lru_replacer;
pub mod linked_lists;
pub mod error;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::collections::LinkedList;
use log::{debug, error, info, warn};

use crate::shared::page::{Page, PageId, FrameId};
use crate::storage::DiskManager;
use self::lru_replacer::LRUReplacer;
use self::error::BufferError;
use anyhow::Result;

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
        
        info!("Created BufferPoolManager with {} pages", pool_size);
        
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
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<Arc<RwLock<Page>>> {
        debug!("Fetching page {}", page_id);
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let page_arc = Arc::clone(&self.pages[frame_id]);
            {
                let mut page = page_arc.write()
                    .map_err(|e| BufferError::LockError(e.to_string()))?;
                page.pin();
            }
            
            match self.replacer.lock() {
                Ok(mut replacer) => {
                    replacer.erase(frame_id);
                },
                Err(e) => {
                    warn!("Failed to lock replacer: {}", e);
                    // Continue anyway, this is non-critical
                }
            }
            
            return Ok(page_arc);
        }

        // Page not in buffer pool, need to load from disk
        let frame_id = match self.get_free_frame() {
            Some(id) => id,
            None => {
                error!("No free frames available to fetch page {}", page_id);
                return Err(anyhow::Error::from(BufferError::NoFreeFrames));
            }
        };
        
        let page_arc = Arc::clone(&self.pages[frame_id]);
        {
            let mut page = page_arc.write()
                .map_err(|e| BufferError::LockError(e.to_string()))?;
            
            if page.get_page_id() != 0 && page.is_dirty() {
                let old_page_id = page.get_page_id();
                if let Err(e) = self.disk_manager.write_page(old_page_id, &page) {
                    error!("Failed to write dirty page {} to disk: {}", old_page_id, e);
                    // Continue anyway, just losing the old page data
                    // We should log this but not fail the operation
                }
                self.page_table.remove(&old_page_id);
            }
            
            if let Err(e) = self.disk_manager.read_page(page_id, &mut page) {
                error!("Failed to read page {} from disk: {}", page_id, e);
                self.free_list.push_back(frame_id);
                return Err(anyhow::Error::from(BufferError::Io(e)));
            }
            
            page.set_page_id(page_id);
            page.pin();
            page.set_dirty(false);
        }
        
        self.page_table.insert(page_id, frame_id);
        debug!("Page {} loaded into frame {}", page_id, frame_id);
        
        Ok(page_arc)
    }
    
    /// Create a new page in the buffer pool
    /// Never choose a page id of 0, as it is reserved for the metadata page
    pub fn new_page(&mut self, page_id: PageId) -> Result<(PageId, Arc<RwLock<Page>>)> {
        if page_id == 0 {
            error!("Creating page with ID 0, which is reserved for metadata");
        }
        
        let frame_id = match self.get_free_frame() {
            Some(id) => id,
            None => {
                error!("No free frames available to create page {}", page_id);
                return Err(anyhow::Error::from(BufferError::NoFreeFrames));
            }
        };
        
        let page_arc = Arc::clone(&self.pages[frame_id]);
        {
            let mut page = page_arc.write()
                .map_err(|e| BufferError::LockError(e.to_string()))?;
                
            if page.get_page_id() != 0 && page.is_dirty() {
                let old_page_id = page.get_page_id();
                if let Err(e) = self.disk_manager.write_page(old_page_id, &page) {
                    error!("Failed to write dirty page {} to disk: {}", old_page_id, e);
                    // Continue anyway, just losing the old page data
                }
                
                self.page_table.remove(&old_page_id);
            }
            
            page.reset();
            page.set_page_id(page_id);
            page.pin();
        }
        
        self.page_table.insert(page_id, frame_id);
        debug!("Created new page {} in frame {}", page_id, frame_id);
        
        Ok((page_id, page_arc))
    }
    
    /// Unpin a page, potentially allowing it to be evicted
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<()> {
        debug!("Unpinning page {}, dirty: {}", page_id, is_dirty);
        
        let frame_id = match self.page_table.get(&page_id) {
            Some(&frame_id) => frame_id,
            None => {
                warn!("Attempted to unpin non-existent page {}", page_id);
                return Err(anyhow::Error::from(BufferError::PageNotFound(page_id)));
            }
        };
        
        let page_arc = Arc::clone(&self.pages[frame_id]);
        let mut page = page_arc.write()
            .map_err(|e| BufferError::LockError(e.to_string()))?;
        
        if is_dirty {
            page.set_dirty(true);
        }
        
        page.unpin();
        
        if page.get_pin_count() == 0 {
            // Add to replacer if pin count is 0
            match self.replacer.lock() {
                Ok(mut replacer) => {
                    replacer.insert(frame_id);
                },
                Err(e) => {
                    warn!("Failed to lock replacer: {}", e);
                    // Continue anyway, this is non-critical
                }
            }
        }
        
        Ok(())
    }
    
    /// Flush a specific page to disk
    pub fn flush_page(&mut self, page_id: PageId) -> Result<()> {
        debug!("Flushing page {} to disk", page_id);
        
        let frame_id = match self.page_table.get(&page_id) {
            Some(&frame_id) => frame_id,
            None => {
                warn!("Attempted to flush non-existent page {}", page_id);
                return Err(anyhow::Error::from(BufferError::PageNotFound(page_id)));
            }
        };
        
        let page_arc = Arc::clone(&self.pages[frame_id]);
        let mut page = page_arc.write()
            .map_err(|e| BufferError::LockError(e.to_string()))?;
        
        match self.disk_manager.write_page(page_id, &page) {
            Ok(_) => {
                page.set_dirty(false);
                debug!("Successfully flushed page {} to disk", page_id);
                Ok(())
            },
            Err(e) => {
                error!("Failed to flush page {} to disk: {}", page_id, e);
                Err(anyhow::Error::from(BufferError::Io(e)))
            }
        }
    }
    
    /// Flush all pages in the buffer pool to disk
    pub fn flush_all_pages(&mut self) -> Result<()> {
        info!("Flushing all pages to disk");
        let page_ids: Vec<PageId> = self.page_table.keys().cloned().collect();
        
        let mut last_error = None;
        for page_id in page_ids {
            if let Err(e) = self.flush_page(page_id) {
                error!("Error flushing page {}: {}", page_id, e);
                last_error = Some(e);
            }
        }
        
        match last_error {
            Some(e) => Err(e),
            None => Ok(())
        }
    }
    
    /// Delete a page from the buffer pool and disk
    pub fn delete_page(&mut self, page_id: PageId) -> Result<()> {
        debug!("Deleting page {}", page_id);
        
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let page_arc = Arc::clone(&self.pages[frame_id]);
            
            let pin_count = {
                let mut page = page_arc.write()
                    .map_err(|e| BufferError::LockError(e.to_string()))?;
                
                let pin_count = page.get_pin_count();
                if pin_count > 0 {
                    return Err(anyhow::Error::from(BufferError::PagePinned(page_id)));
                }
                
                page.reset();
                pin_count
            };
            
            if pin_count == 0 {
                self.page_table.remove(&page_id);
                self.free_list.push_back(frame_id);
                
                match self.replacer.lock() {
                    Ok(mut replacer) => {
                        replacer.erase(frame_id);
                    },
                    Err(e) => {
                        warn!("Failed to lock replacer: {}", e);
                        // Continue anyway, this is non-critical
                    }
                }
            }
        }
        
        match self.disk_manager.delete_page(page_id) {
            Ok(_) => {
                debug!("Successfully deleted page {}", page_id);
                Ok(())
            },
            Err(e) => {
                error!("Failed to delete page {} from disk: {}", page_id, e);
                Err(anyhow::Error::from(BufferError::Io(e)))
            }
        }
    }
    
    /// Get a free frame from the free list or by evicting a page
    fn get_free_frame(&mut self) -> Option<FrameId> {
        if let Some(frame_id) = self.free_list.pop_front() {
            debug!("Using free frame {}", frame_id);
            return Some(frame_id);
        }
        
        // No free frames, try to evict a page
        debug!("No free frames, attempting to evict a page");
        
        let victim_frame = match self.replacer.lock() {
            Ok(mut replacer) => replacer.victim(),
            Err(e) => {
                error!("Failed to lock replacer: {}", e);
                return None;
            }
        };
        
        match victim_frame {
            Some(frame_id) => {
                debug!("Evicting frame {}", frame_id);
                
                // Get the page ID to remove from page table
                if let Ok(page) = self.pages[frame_id].read() {
                    self.page_table.remove(&page.get_page_id());
                } else {
                    warn!("Failed to lock page in frame {} for eviction", frame_id);
                }
                
                Some(frame_id)
            },
            None => {
                error!("No evictable frames found");
                None
            }
        }
    }

    pub fn close(&mut self) -> Result<()> {
        info!("Closing buffer pool manager");
        
        // Try to flush all pages before closing
        if let Err(e) = self.flush_all_pages() {
            warn!("Error flushing pages during close: {}", e);
            // Continue with close even if flush failed
        }
        
        // Close the disk manager
        match self.disk_manager.close() {
            Ok(_) => {
                info!("Buffer pool manager successfully closed");
                Ok(())
            },
            Err(e) => {
                error!("Failed to close disk manager: {}", e);
                Err(anyhow::Error::from(BufferError::Io(e)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs::{self, File};

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
        let (page_id, page_arc) = buffer_pool.new_page(0).expect("Failed to create new page");
        assert_eq!(page_id, 0); // First page should have id 0
        
        // Check that the page is in the page table
        assert!(buffer_pool.page_table.contains_key(&page_id));
        
        // Check that the free list has one less entry
        assert_eq!(buffer_pool.free_list.len(), pool_size - 1);
        
        // Create another page
        let (page_id2, _) = buffer_pool.new_page(1).expect("Failed to create second page");
        assert_eq!(page_id2, 1); // Second page should have id 1
        
        // Verify the page properties
        {
            let page = page_arc.read().expect("Failed to read page");
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
        let (page_id, page_arc) = buffer_pool.new_page(0).expect("Failed to create new page");
        
        // Write some data to the page
        {
            let mut page = page_arc.write().expect("Failed to write to page");
            let data = page.get_data_mut();
            data[0] = 42;
            data[1] = 24;
            page.set_dirty(true);
        }
        
        // Unpin the page so it can be evicted
        buffer_pool.unpin_page(page_id, true).expect("Failed to unpin page");
        
        // The page should now be in the replacer
        assert_eq!(buffer_pool.replacer.lock().unwrap().size(), 1);
        
        // Fetch the page again
        let fetched_page = buffer_pool.fetch_page(page_id).expect("Failed to fetch page");
        
        // Verify the data
        {
            let page = fetched_page.read().expect("Failed to read fetched page");
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
        let (page_id1, _) = buffer_pool.new_page(0).expect("Failed to create first page");
        let (page_id2, _) = buffer_pool.new_page(1).expect("Failed to create second page");
        
        // Unpin both pages
        buffer_pool.unpin_page(page_id1, true).expect("Failed to unpin first page");
        buffer_pool.unpin_page(page_id2, false).expect("Failed to unpin second page");
        
        // Create a new page, should evict one of the previous pages
        let (page_id3, _) = buffer_pool.new_page(2).expect("Failed to create third page");
        
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
        let (page_id, page_arc) = buffer_pool.new_page(0).expect("Failed to create new page");
        
        // Write some data to the page
        {
            let mut page = page_arc.write().expect("Failed to write to page");
            let data = page.get_data_mut();
            data[0] = 99;
            data[1] = 88;
            page.set_dirty(true);
        }
        
        // Flush the page to disk
        buffer_pool.flush_page(page_id).expect("Failed to flush page");
        
        // Unpin the page
        buffer_pool.unpin_page(page_id, false).expect("Failed to unpin page");
        
        // Evict the page by filling the buffer pool
        for i in 1..=pool_size {
            buffer_pool.new_page(i as PageId).expect("Failed to create page");
        }
        
        // Unpin at least one page to make room in buffer pool
        buffer_pool.unpin_page(1, false).expect("Failed to unpin page");
        
        // Fetch the page again, should read from disk
        let fetched_page = buffer_pool.fetch_page(page_id).expect("Failed to fetch page");
        
        // Verify the data was persisted
        {
            let page = fetched_page.read().expect("Failed to read fetched page");
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
        let (page_id, _) = buffer_pool.new_page(1).expect("Failed to create new page");
        
        // Unpin the page so it can be deleted
        buffer_pool.unpin_page(page_id, false).expect("Failed to unpin page");
        
        // Delete the page
        buffer_pool.delete_page(page_id).expect("Failed to delete page");
        
        // Page should no longer be in the page table
        assert!(!buffer_pool.page_table.contains_key(&page_id));
        
        // Trying to fetch the deleted page should fail
        assert!(buffer_pool.fetch_page(page_id).is_err());
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_flush_all_pages() {
        let (temp_file, disk_manager) = create_temp_db_file();
        let pool_size = 3;
        let mut buffer_pool = BufferPoolManager::new(pool_size, disk_manager);
        
        // Create several pages with data
        let (page_id1, page_arc1) = buffer_pool.new_page(0).expect("Failed to create first page");
        let (page_id2, page_arc2) = buffer_pool.new_page(1).expect("Failed to create second page");
        
        // Write data to pages
        {
            let mut page1 = page_arc1.write().expect("Failed to write to first page");
            let data1 = page1.get_data_mut();
            data1[0] = 11;
            page1.set_dirty(true);
            
            let mut page2 = page_arc2.write().expect("Failed to write to second page");
            let data2 = page2.get_data_mut();
            data2[0] = 22;
            page2.set_dirty(true);
        }
        
        // Flush all pages
        buffer_pool.flush_all_pages().expect("Failed to flush all pages");
        
        // Unpin pages
        buffer_pool.unpin_page(page_id1, false).expect("Failed to unpin first page");
        buffer_pool.unpin_page(page_id2, false).expect("Failed to unpin second page");
        
        // Evict pages by creating new ones
        for i in 2..=4 {
            buffer_pool.new_page(i).expect("Failed to create page");
        }
        
        // Unpin some pages to make room
        buffer_pool.unpin_page(3, false).expect("Failed to unpin page");
        buffer_pool.unpin_page(2, false).expect("Failed to unpin page");
        
        // Fetch pages again
        let fetched_page1 = buffer_pool.fetch_page(page_id1).expect("Failed to fetch first page");
        let fetched_page2 = buffer_pool.fetch_page(page_id2).expect("Failed to fetch second page");
        
        // Verify data persisted
        {
            let page1 = fetched_page1.read().expect("Failed to read first page");
            let page2 = fetched_page2.read().expect("Failed to read second page");
            
            assert_eq!(page1.get_data()[0], 11);
            assert_eq!(page2.get_data()[0], 22);
        }
        
        cleanup_temp_file(&temp_file);
    }
}