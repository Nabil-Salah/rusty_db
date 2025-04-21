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