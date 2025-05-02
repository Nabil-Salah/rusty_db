use std::collections::{LinkedList, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;
use std::convert::TryInto;

use crate::shared::page::{Page, PageId, PAGE_SIZE};

const CATALOG_PAGE_ID: PageId = 0;

pub struct DiskManager {
    db_file: Mutex<File>,
    file_path: String,
    pages_count: PageId,
    free_page_list: LinkedList<PageId>,
    page_directory: HashMap<PageId, u64>, // Maps page_id to file offset
    initialized: bool,
}

impl DiskManager {
    pub fn new<P: AsRef<Path>>(db_path: P) -> io::Result<Self> {
        let file_path = match db_path.as_ref().to_str(){
            Some(path) => path.to_string(),
            None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid file path")),
        };
        
        let file_exists = db_path.as_ref().exists();
        
        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)?;
        
        let mut disk_manager = Self {
            db_file: Mutex::new(db_file),
            file_path,
            pages_count: 1, // Start from 1 since page 0 is reserved for catalog
            free_page_list: LinkedList::new(),
            page_directory: HashMap::new(),
            initialized: file_exists,
        };
        
        if disk_manager.initialized {
            match disk_manager.load_metadata() {
                Ok(_) => {},
                Err(_) => {
                    disk_manager.pages_count = 1;
                    disk_manager.free_page_list.clear();
                    disk_manager.page_directory.clear();
                    disk_manager.initialized = false;
                }
            }
        } else {
            disk_manager.save_metadata()?;
        }
        
        Ok(disk_manager)
    }
    
    pub fn read_page(&self, page_id: PageId, page: &mut Page) -> io::Result<()> {
        // Check if page exists in directory first
        let offset = match self.page_directory.get(&page_id) {
            Some(offset) => *offset,
            None => return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} does not exist", page_id)
            )),
        };
        
        let mut buffer = [0u8; PAGE_SIZE];
        
        let mut file = self.db_file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        
        match file.read_exact(&mut buffer) {
            Ok(_) => {
                page.copy_from(&buffer);
                Ok(())
            },
            Err(e) => Err(e),
        }
    }
    
    pub fn write_page(&mut self, page_id: PageId, page: &Page) -> io::Result<()> {
        let offset = match self.page_id_to_offset(page_id){
            Some(offset) => offset,
            None => {
                self.get_next_page_index() * PAGE_SIZE as u64
            }
        };
        
        let mut file = self.db_file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(page.get_data())?;
        file.flush()?;

        self.page_directory.insert(page_id, offset);
        
        Ok(())
    }
    
    fn page_id_to_offset(&self, page_id: PageId) -> Option<u64> {
        self.page_directory
            .get(&page_id)
            .copied()
    }
    
    pub fn get_file_path(&self) -> &str {
        &self.file_path
    }

    pub fn get_next_page_index(&mut self) -> PageId {
        if let Some(reused_page_id) = self.free_page_list.pop_front() {
            return reused_page_id;
        }
        let curr_index = self.pages_count;
        self.pages_count += 1;
        curr_index
    }

    pub fn delete_page(&mut self, page_id: PageId) -> io::Result<()> {
        if page_id == CATALOG_PAGE_ID {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied, 
                "Cannot delete catalog page"
            ));
        }
        self.free_page_list.push_back(page_id);
        // Save metadata after deleting a page
        self.save_metadata()?;
        Ok(())
    }
    
    pub fn close(&mut self) -> io::Result<()> {
        self.save_metadata()
    }
    
    fn save_metadata(&mut self) -> io::Result<()> {
        let mut catalog_page = Page::new(CATALOG_PAGE_ID);
        let data = catalog_page.get_data_mut();
        
        // Structure of catalog page:
        // Bytes 0-7: pages_count (u64)
        // Bytes 8-15: free_list_size (u64)
        // Bytes 16-23: page_directory_size (u64)
        // Bytes 24+: Free page IDs followed by page directory entries (page_id, offset pairs)
        
        // Write pages_count
        let pages_count_bytes = self.pages_count.to_le_bytes();
        data[0..8].copy_from_slice(&pages_count_bytes);
        
        // Write free_list_size
        let free_list_size = self.free_page_list.len() as u64;
        let free_list_size_bytes = free_list_size.to_le_bytes();
        data[8..16].copy_from_slice(&free_list_size_bytes);
        
        // Write page_directory_size
        let page_dir_size = self.page_directory.len() as u64;
        let page_dir_size_bytes = page_dir_size.to_le_bytes();
        data[16..24].copy_from_slice(&page_dir_size_bytes);
        
        // Write free page IDs
        let mut offset = 24;
        for page_id in &self.free_page_list {
            if offset + 8 <= PAGE_SIZE {
                data[offset..(offset+8)].copy_from_slice(&page_id.to_le_bytes());
                offset += 8;
            }
        }
        
        // Write page directory entries (page_id -> offset pairs)
        for (page_id, file_offset) in &self.page_directory {
            if offset + 16 <= PAGE_SIZE {
                data[offset..(offset+8)].copy_from_slice(&page_id.to_le_bytes());
                data[(offset+8)..(offset+16)].copy_from_slice(&file_offset.to_le_bytes());
                offset += 16;
            }
        }
        
        // Write the catalog page
        // We don't use self.write_page here to avoid circular logic
        let mut file = self.db_file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?; // Catalog page is always at offset 0
        file.write_all(catalog_page.get_data())?;
        file.flush()?;
        
        // Make sure the catalog page is in our page directory
        self.page_directory.insert(CATALOG_PAGE_ID, 0);
        
        Ok(())
    }
    
    fn load_metadata(&mut self) -> io::Result<()> {
        let mut catalog_page = Page::new(CATALOG_PAGE_ID);
        
        // Try to read the catalog page from disk
        let mut file = self.db_file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        
        let mut buffer = [0u8; PAGE_SIZE];
        match file.read_exact(&mut buffer) {
            Ok(_) => {
                catalog_page.copy_from(&buffer);
                drop(file); // Release the lock before further processing
            },
            Err(e) => return Err(e),
        }
        
        let data = catalog_page.get_data();
        
        // Read pages_count
        let pages_count_bytes = &data[0..8];
        self.pages_count = u64::from_le_bytes(pages_count_bytes.try_into().unwrap());
        
        // Read free_list_size
        let free_list_size_bytes = &data[8..16];
        let free_list_size = u64::from_le_bytes(free_list_size_bytes.try_into().unwrap()) as usize;
        
        // Read page_directory_size
        let page_dir_size_bytes = &data[16..24];
        let page_dir_size = u64::from_le_bytes(page_dir_size_bytes.try_into().unwrap()) as usize;
        
        // Clear existing data structures
        self.free_page_list.clear();
        self.page_directory.clear();
        
        // Read free page IDs
        let mut offset = 24;
        for _ in 0..free_list_size {
            if offset + 8 <= PAGE_SIZE {
                let page_id_bytes = &data[offset..(offset+8)];
                let page_id = u64::from_le_bytes(page_id_bytes.try_into().unwrap());
                self.free_page_list.push_back(page_id);
                offset += 8;
            }
        }
        
        // Read page directory entries
        for _ in 0..page_dir_size {
            if offset + 16 <= PAGE_SIZE {
                let page_id_bytes = &data[offset..(offset+8)];
                let file_offset_bytes = &data[(offset+8)..(offset+16)];
                
                let page_id = u64::from_le_bytes(page_id_bytes.try_into().unwrap());
                let file_offset = u64::from_le_bytes(file_offset_bytes.try_into().unwrap());
                
                self.page_directory.insert(page_id, file_offset);
                offset += 16;
            }
        }
        
        // Make sure the catalog page is in our page directory
        self.page_directory.insert(CATALOG_PAGE_ID, 0);
        
        // Sanity check: pages_count should be at least 1 (for catalog page)
        if self.pages_count < 1 {
            self.pages_count = 1;
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::ErrorKind;

    fn create_temp_db_file() -> String {
        let temp_file = format!("/tmp/rusty_db_test_storage_{}.db", std::process::id());
        
        if Path::new(&temp_file).exists() {
            fs::remove_file(&temp_file).unwrap();
        }
        
        temp_file
    }
    
    fn cleanup_temp_file(file_path: &str) {
        if Path::new(file_path).exists() {
            fs::remove_file(file_path).unwrap();
        }
    }
    
    #[test]
    fn test_disk_manager_creation() {
        let temp_file = create_temp_db_file();
        
        let disk_manager = DiskManager::new(&temp_file).unwrap();
        
        assert_eq!(disk_manager.get_file_path(), temp_file);
        assert_eq!(disk_manager.pages_count, 1); // Starts from 1 due to catalog page
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_get_next_page_index() {
        let temp_file = create_temp_db_file();
        
        let mut disk_manager = DiskManager::new(&temp_file).unwrap();
        
        assert_eq!(disk_manager.get_next_page_index(), 1);
        assert_eq!(disk_manager.get_next_page_index(), 2);
        assert_eq!(disk_manager.get_next_page_index(), 3);
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_write_and_read_page() {
        let temp_file = create_temp_db_file();
        
        let mut disk_manager = DiskManager::new(&temp_file).unwrap();
        
        let page_id = 3;
        let mut page = Page::new(page_id);
        
        // Write some data to the page
        {
            let data = page.get_data_mut();
            data[0] = 123;
            data[1] = 45;
            data[2] = 67;
            data[3] = 89;
        }
        
        // Write the page to disk
        disk_manager.write_page(page_id, &page).unwrap();
        
        // Read the page back from disk
        let mut read_page = Page::new(0);
        disk_manager.read_page(page_id, &mut read_page).unwrap();
        
        // Verify the data
        assert_eq!(read_page.get_data()[0], 123);
        assert_eq!(read_page.get_data()[1], 45);
        assert_eq!(read_page.get_data()[2], 67);
        assert_eq!(read_page.get_data()[3], 89);
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_read_nonexistent_page() {
        let temp_file = create_temp_db_file();
        
        let disk_manager = DiskManager::new(&temp_file).unwrap();
        
        let page_id = 100; // A page we haven't written to
        let mut page = Page::new(0);
        
        // Read a page that doesn't exist
        let result = disk_manager.read_page(page_id, &mut page);
        
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), ErrorKind::NotFound);
        }
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_write_multiple_pages() {
        let temp_file = create_temp_db_file();
        
        let mut disk_manager = DiskManager::new(&temp_file).unwrap();
        
        // Create and write multiple pages
        for i in 1..6 {
            let page_id = i;
            let mut page = Page::new(page_id);
            
            // Write unique data to each page
            {
                let data = page.get_data_mut();
                data[0] = (i * 10) as u8;
                data[1] = (i * 20) as u8;
            }
            
            disk_manager.write_page(page_id, &page).unwrap();
        }
        
        // Read and verify each page
        for i in 1..6 {
            let page_id = i;
            let mut page = Page::new(0);
            
            disk_manager.read_page(page_id, &mut page).unwrap();
            
            assert_eq!(page.get_data()[0], (i * 10) as u8);
            assert_eq!(page.get_data()[1], (i * 20) as u8);
        }
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_overwrite_page() {
        let temp_file = create_temp_db_file();
        
        let mut disk_manager = DiskManager::new(&temp_file).unwrap();
        
        let page_id = 7;
        let mut page = Page::new(page_id);
        
        // Write initial data
        {
            let data = page.get_data_mut();
            data[0] = 111;
            data[1] = 222;
        }
        
        disk_manager.write_page(page_id, &page).unwrap();
        
        // Overwrite with new data
        {
            let data = page.get_data_mut();
            data[0] = 55;
            data[1] = 66;
        }
        
        disk_manager.write_page(page_id, &page).unwrap();
        
        // Read and verify the page has the new data
        let mut read_page = Page::new(0);
        disk_manager.read_page(page_id, &mut read_page).unwrap();
        
        assert_eq!(read_page.get_data()[0], 55);
        assert_eq!(read_page.get_data()[1], 66);
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_page_id_to_offset() {
        let temp_file = create_temp_db_file();
        
        let mut disk_manager = DiskManager::new(&temp_file).unwrap();
        
        // Write pages to create entries in page_directory
        for i in 1..4 {
            let page = Page::new(i);
            disk_manager.write_page(i, &page).unwrap();
        }
        
        // Now check the offsets from the page_directory
        let offset1 = disk_manager.page_id_to_offset(1);
        let offset2 = disk_manager.page_id_to_offset(2);
        let offset3 = disk_manager.page_id_to_offset(3);
        
        assert!(offset1.is_some());
        assert!(offset2.is_some());
        assert!(offset3.is_some());
        assert_eq!(offset1.unwrap(), PAGE_SIZE as u64);
        assert_eq!(offset2.unwrap(), (PAGE_SIZE * 2) as u64);
        assert_eq!(offset3.unwrap(), (PAGE_SIZE * 3) as u64);
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_file_persistence() {
        let temp_file = create_temp_db_file();
        
        // Create a disk manager and write a page
        {
            let mut disk_manager = DiskManager::new(&temp_file).unwrap();
            
            let page_id = 42;
            let mut page = Page::new(page_id);
            
            {
                let data = page.get_data_mut();
                data[0] = 0xAB;
                data[1] = 0xCD;
                data[2] = 0xEF;
            }
            
            disk_manager.write_page(page_id, &page).unwrap();
            
            // Explicitly save metadata before closing
            disk_manager.close().unwrap();
        }
        // DiskManager is dropped here
        
        // Create a new disk manager
        let mut disk_manager = DiskManager::new(&temp_file).unwrap();
        
        // Now we can read it
        let mut read_page = Page::new(0);
        disk_manager.read_page(42, &mut read_page).unwrap();
        
        assert_eq!(read_page.get_data()[0], 0xAB);
        assert_eq!(read_page.get_data()[1], 0xCD);
        assert_eq!(read_page.get_data()[2], 0xEF);
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_delete_and_reuse_page() {
        let temp_file = create_temp_db_file();
        
        let mut disk_manager = DiskManager::new(&temp_file).unwrap();
        
        // Allocate some pages
        let page_id1 = disk_manager.get_next_page_index();
        let page_id2 = disk_manager.get_next_page_index();
        let page_id3 = disk_manager.get_next_page_index();
        
        assert_eq!(page_id1, 1);
        assert_eq!(page_id2, 2);
        assert_eq!(page_id3, 3);
        
        // Delete page 2
        disk_manager.delete_page(page_id2).unwrap();
        
        // Next allocation should reuse page_id2
        let new_page_id = disk_manager.get_next_page_index();
        assert_eq!(new_page_id, page_id2);
        
        // Next allocation should get a new page ID
        let next_new_page_id = disk_manager.get_next_page_index();
        assert_eq!(next_new_page_id, 4);
        
        cleanup_temp_file(&temp_file);
    }

    #[test]
    fn test_metadata_persistence() {
        let temp_file = create_temp_db_file();
        
        // First create a disk manager and perform operations
        {
            let mut disk_manager = DiskManager::new(&temp_file).unwrap();
            
            // Create several pages
            let page1 = 1;
            let page3 = 3;
            let page5 = 5;
            
            // Write content to a few pages
            let test_page = Page::new(page1);
            disk_manager.write_page(page1, &test_page).unwrap();
            
            let test_page = Page::new(page3);
            disk_manager.write_page(page3, &test_page).unwrap();
            
            let test_page = Page::new(page5);
            disk_manager.write_page(page5, &test_page).unwrap();
    
            disk_manager.close().unwrap();
        } // First DiskManager is dropped here
        
        // Create a new disk manager to load the persisted metadata
        {
            let mut disk_manager = DiskManager::new(&temp_file).unwrap();
            
            // Check pages_count was restored
            assert_eq!(disk_manager.pages_count, 4);
            
            assert_eq!(disk_manager.free_page_list.len(), 0);
            
            // Check page_directory was restored
            assert_eq!(disk_manager.page_directory.len(), 4); // catalog page + 3 written pages
            assert!(disk_manager.page_directory.contains_key(&0)); // Catalog page
            assert!(disk_manager.page_directory.contains_key(&1));
            assert!(disk_manager.page_directory.contains_key(&3));
            assert!(disk_manager.page_directory.contains_key(&5));
        }
        
        cleanup_temp_file(&temp_file);
    }
}