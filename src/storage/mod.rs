use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use crate::shared::page::{Page, PageId, PAGE_SIZE};

pub struct DiskManager {
    db_file: Mutex<File>,
    file_path: String,
    next_page_id: PageId,
}

impl DiskManager {
    pub fn new<P: AsRef<Path>>(db_path: P) -> io::Result<Self> {
        let file_path = match db_path.as_ref().to_str(){
            Some(path) => path.to_string(),
            None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid file path")),
        };
        
        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)?;
            
        Ok(Self {
            db_file: Mutex::new(db_file),
            file_path,
            next_page_id: 0,
        })
    }
    
    pub fn read_page(&self, page_id: PageId, page: &mut Page) -> io::Result<()> {
        let offset = self.page_id_to_offset(page_id);
        let mut buffer = [0u8; PAGE_SIZE];
        
        let mut file = self.db_file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        
        match file.read_exact(&mut buffer) {
            Ok(_) => {
                page.copy_from(&buffer);
                Ok(())
            },
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Page {} does not exist", page_id)
                ))
            },
            Err(e) => Err(e),
        }
    }
    
    pub fn write_page(&self, page_id: PageId, page: &Page) -> io::Result<()> {
        let offset = self.page_id_to_offset(page_id);
        
        let mut file = self.db_file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(page.get_data())?;
        file.flush()?;
        
        Ok(())
    }
    
    fn page_id_to_offset(&self, page_id: PageId) -> u64 {
        page_id * PAGE_SIZE as u64
    }
    
    pub fn get_file_path(&self) -> &str {
        &self.file_path
    }

    pub fn get_next_page_id(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        page_id
    }

    pub fn delete_page(&self, _page_id: PageId) -> io::Result<()> {
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
        assert_eq!(disk_manager.next_page_id, 0);
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_get_next_page_id() {
        let temp_file = create_temp_db_file();
        
        let mut disk_manager = DiskManager::new(&temp_file).unwrap();
        
        assert_eq!(disk_manager.get_next_page_id(), 0);
        assert_eq!(disk_manager.get_next_page_id(), 1);
        assert_eq!(disk_manager.get_next_page_id(), 2);
        
        cleanup_temp_file(&temp_file);
    }
    
    #[test]
    fn test_write_and_read_page() {
        let temp_file = create_temp_db_file();
        
        let disk_manager = DiskManager::new(&temp_file).unwrap();
        
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
        
        let disk_manager = DiskManager::new(&temp_file).unwrap();
        
        // Create and write multiple pages
        for i in 0..5 {
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
        for i in 0..5 {
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
        
        let disk_manager = DiskManager::new(&temp_file).unwrap();
        
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
        
        let disk_manager = DiskManager::new(&temp_file).unwrap();
        
        // Check offset calculation for different page IDs
        let offset0 = disk_manager.page_id_to_offset(0);
        let offset1 = disk_manager.page_id_to_offset(1);
        let offset10 = disk_manager.page_id_to_offset(10);
        
        assert_eq!(offset0, 0);
        assert_eq!(offset1, PAGE_SIZE as u64);
        assert_eq!(offset10, (PAGE_SIZE * 10) as u64);
        
        cleanup_temp_file(&temp_file);
    }
    
    
    #[test]
    fn test_file_persistence() {
        let temp_file = create_temp_db_file();
        
        // Create a disk manager and write a page
        {
            let disk_manager = DiskManager::new(&temp_file).unwrap();
            
            let page_id = 42;
            let mut page = Page::new(page_id);
            
            {
                let data = page.get_data_mut();
                data[0] = 0xAB;
                data[1] = 0xCD;
                data[2] = 0xEF;
            }
            
            disk_manager.write_page(page_id, &page).unwrap();
        }
        // DiskManager is dropped here
        
        // Create a new disk manager and read the same page
        {
            let disk_manager = DiskManager::new(&temp_file).unwrap();
            
            let page_id = 42;
            let mut page = Page::new(0);
            
            disk_manager.read_page(page_id, &mut page).unwrap();
            
            assert_eq!(page.get_data()[0], 0xAB);
            assert_eq!(page.get_data()[1], 0xCD);
            assert_eq!(page.get_data()[2], 0xEF);
        }
        
        cleanup_temp_file(&temp_file);
    }
}