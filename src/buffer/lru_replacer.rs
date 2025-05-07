use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};
use std::hash::Hash;
use std::fmt::Debug;
use anyhow::{Result as AnyhowResult, Context};
use log::{debug, trace, warn, error};
use thiserror::Error;

use super::linked_lists::{LinkedListLru, Node, LinkedListError};

#[derive(Error, Debug)]
pub enum LRUReplacerError {
    #[error("No victim available")]
    NoVictimAvailable,
    
    #[error("Lock error: {0}")]
    LockError(String),
    
    #[error("Linked list error: {0}")]
    LinkedListError(#[from] LinkedListError),
}

pub type LRUResult<T> = std::result::Result<T, LRUReplacerError>;

pub struct LRUReplacer<T> 
where 
    T: Clone + Copy + Eq + Hash + Debug,
{
    list: LinkedListLru<T>,
    map: HashMap<T, Arc<RwLock<Node<T>>>>,
    size: usize,
}

impl<T> LRUReplacer<T> 
where 
    T: Clone + Copy + Eq + Hash + Debug,
{
    pub fn new() -> Self {
        trace!("Creating new LRUReplacer");
        Self {
            list: LinkedListLru::new(),
            map: HashMap::new(),
            size: 0,
        }
    }
    
    /// Insert a new item into the replacer or move it to the front if it exists
    pub fn insert(&mut self, item: T) {
        trace!("Inserting {:?} into LRUReplacer", item);
        
        self.erase(item);
        let node_ref = self.list.push_front(item);
        self.map.insert(item, node_ref);
        self.size += 1;
        
        debug!("Inserted {:?} into LRUReplacer, new size: {}", item, self.size);
    }

    /// Remove and return the least recently used item (victim)
    pub fn victim(&mut self) -> Option<T> {
        trace!("Finding victim in LRUReplacer");
        
        if self.list.is_empty() {
            debug!("No victim available, list is empty");
            return None;
        }
        
        if let Some(item) = self.list.pop_back() {
            debug!("Found victim: {:?}", item);
            self.map.remove(&item);
            self.size -= 1; 
            Some(item)
        } else {
            debug!("Failed to pop item from back of list");
            None
        }
    }

    /// Remove a specific item from the replacer
    /// Returns true if the item was removed, false if it wasn't found
    pub fn erase(&mut self, item: T) -> bool {
        trace!("Erasing {:?} from LRUReplacer", item);
        
        if let Some(node_ref) = self.map.remove(&item) {
            match self.list.remove_node(&node_ref) {
                Ok(true) => {
                    self.size -= 1;
                    debug!("Erased {:?} from LRUReplacer, new size: {}", item, self.size);
                    true
                },
                Ok(false) => {
                    debug!("Node for {:?} was found in map but not successfully removed from list", item);
                    // Re-insert the reference back into the map since we didn't remove it
                    self.map.insert(item, node_ref);
                    false
                },
                Err(e) => {
                    error!("Error removing node for {:?} from list: {}", item, e);
                    // Re-insert the reference back into the map since we didn't remove it
                    self.map.insert(item, node_ref);
                    false
                }
            }
        } else {
            trace!("Item {:?} not found in LRUReplacer", item);
            false
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }
}