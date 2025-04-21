pub mod lru_replacer;
pub mod linked_lists;

use std::collections::HashMap;
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
                    return None;
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
    pub fn new_page(&mut self) -> Option<(PageId, Arc<RwLock<Page>>)> {
        let frame_id = match self.get_free_frame() {
            Some(id) => id,
            None => return None,
        };
        
        let page_arc = self.pages[frame_id].clone();
        let page_id = self.disk_manager.get_next_page_id();
        {
            let mut page = page_arc.write().unwrap();
            page.set_page_id(page_id);
            page.pin();
            page.set_dirty(false);
        }
        {
            let mut page = page_arc.write().unwrap();
            if page.get_page_id() != 0 && page.is_dirty() {
                let old_page_id = page.get_page_id();
                if let Err(e) = self.disk_manager.write_page(old_page_id, &page) {
                    eprintln!("Failed to write dirty page {} to disk: {}", old_page_id, e);
                    return None;
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
    pub fn flush_page(&self, page_id: PageId) -> bool {
        match self.page_table.get(&page_id) {
            Some(&frame_id) => {
                let page_arc = Arc::clone(&self.pages[frame_id]);
                let page = page_arc.read().unwrap();
                
                match self.disk_manager.write_page(page_id, &page) {
                    Ok(_) => true,
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
    pub fn flush_all_pages(&self) -> bool {
        let mut all_success = true;
        
        for (&page_id, _) in &self.page_table {
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
}