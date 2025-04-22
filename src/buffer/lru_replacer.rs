use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};
use std::hash::Hash;

use super::linked_lists::{LinkedListLru, Node};

pub struct LRUReplacer<T> 
where 
    T: Clone + Copy + Eq + Hash,
{
    list: LinkedListLru<T>,
    map: HashMap<T, Arc<RwLock<Node<T>>>>,
    size: usize,
}

impl<T> LRUReplacer<T> 
where 
    T: Clone + Copy + Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            list: LinkedListLru::new(),
            map: HashMap::new(),
            size: 0,
        }
    }
    
    /// Insert a new item into the replacer or move it to the front if it exists
    pub fn insert(&mut self, item: T) {
        self.erase(item);
        let node_ref = self.list.push_front(item);
        self.map.insert(item, node_ref);
        self.size += 1;
    }

    /// Remove and return the least recently used item (victim)
    pub fn victim(&mut self) -> Option<T> {
        if self.list.is_empty() {
            return None;
        }
        
        if let Some(item) = self.list.pop_back() {
            self.map.remove(&item);
            self.size -= 1; 
            Some(item)
        } else {
            None
        }
    }

    /// Remove a specific item from the replacer
    pub fn erase(&mut self, item: T) -> bool {
        if let Some(node_ref) = self.map.remove(&item) {
            self.list.remove_node(&node_ref);
            self.size -= 1;
            true
        } else {
            false
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }
}