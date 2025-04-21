use std::sync::RwLock;

pub const PAGE_SIZE: usize = 4096; // Page size is 4KB

pub type PageId = u64;
pub type FrameId = usize;

pub struct Page {
    data: Box<[u8; PAGE_SIZE]>,
    page_id: PageId,
    pin_count: u32,
    is_dirty: bool,
    latch: RwLock<()>,
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        Self {
            data: Box::new([0; PAGE_SIZE]),
            page_id,
            pin_count: 0,
            is_dirty: false,
            latch: RwLock::new(()),
        }
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    pub fn unpin(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
    }

    pub fn get_pin_count(&self) -> u32 {
        self.pin_count
    }

    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn get_data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn get_data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }

    pub fn copy_from(&mut self, data: &[u8]) {
        let len = std::cmp::min(data.len(), PAGE_SIZE);
        self.data[..len].copy_from_slice(&data[..len]);
    }

    pub fn latch(&self) -> std::sync::RwLockReadGuard<()> {
        self.latch.read().unwrap()
    }

    pub fn reset(&mut self) {
        self.data.fill(0);
        self.pin_count = 0;
        self.is_dirty = false;
        self.page_id = 0;
    }
}