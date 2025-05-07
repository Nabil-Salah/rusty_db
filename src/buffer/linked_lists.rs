use std::sync::{Arc, Weak, RwLock};
use std::fmt;
use anyhow::{Result, Context};
use log::{debug, trace};
use thiserror::Error;

/// Custom error type for linked list operations
#[derive(Error, Debug)]
pub enum LinkedListError {
    #[error("Node lock error: {0}")]
    LockError(String),
    
    #[error("Empty list")]
    EmptyList,
    
    #[error("Invalid node")]
    InvalidNode,
    
    #[error("Failed to read node value")]
    ValueReadError,
}

/// A node in the doubly linked list
pub struct Node<T> {
    pub value: T,
    pub next: Option<Arc<RwLock<Node<T>>>>,
    pub prev: Option<Weak<RwLock<Node<T>>>>,
}

impl<T> Node<T> {
    /// Create a new node with the given value
    pub fn new(value: T) -> Self {
        Node {
            value,
            next: None,
            prev: None,
        }
    }
}

/// A doubly linked list implementation
pub struct LinkedListLru<T> {
    head: Option<Arc<RwLock<Node<T>>>>,
    tail: Option<Arc<RwLock<Node<T>>>>,
    size: usize,
}

impl<T: Clone> LinkedListLru<T> {
    /// Create a new empty linked list
    pub fn new() -> Self {
        trace!("Creating new LinkedListLru");
        LinkedListLru {
            head: None,
            tail: None,
            size: 0,
        }
    }

    /// Returns true if the linked list is empty
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Returns the number of elements in the linked list
    pub fn len(&self) -> usize {
        self.size
    }

    /// Insert a new node with the given value at the front of the list
    pub fn push_front(&mut self, value: T) -> Arc<RwLock<Node<T>>> {
        trace!("Pushing value to front of list");
        let new_node = Arc::new(RwLock::new(Node::new(value)));
        
        match self.head.take() {
            Some(old_head) => {
                if let Err(e) = old_head.write()
                    .map_err(|e| LinkedListError::LockError(e.to_string()))
                    .map(|mut head| head.prev = Some(Arc::downgrade(&new_node)))
                {
                    debug!("Error setting prev pointer on old head: {}", e);
                    self.head = Some(old_head);
                    return Arc::new(RwLock::new(Node::new(new_node.read().unwrap().value.clone())));
                }
                
                if let Err(e) = new_node.write()
                    .map_err(|e| LinkedListError::LockError(e.to_string()))
                    .map(|mut node| node.next = Some(old_head.clone()))
                {
                    debug!("Error setting next pointer on new node: {}", e);
                    let _ = old_head.write().map(|mut head| head.prev = None);
                    self.head = Some(old_head);
                    return Arc::new(RwLock::new(Node::new(new_node.read().unwrap().value.clone())));
                }
                
                self.head = Some(Arc::clone(&new_node));
            }
            None => {
                self.head = Some(Arc::clone(&new_node));
                self.tail = Some(Arc::clone(&new_node));
            }
        }
        
        self.size += 1;
        new_node
    }

    /// Insert a new node with the given value at the back of the list
    pub fn push_back(&mut self, value: T) -> Arc<RwLock<Node<T>>> {
        trace!("Pushing value to back of list");
        let new_node = Arc::new(RwLock::new(Node::new(value)));
        
        match self.tail.take() {
            Some(old_tail) => {
                if let Err(e) = old_tail.write()
                    .map_err(|e| LinkedListError::LockError(e.to_string()))
                    .map(|mut tail| tail.next = Some(Arc::clone(&new_node)))
                {
                    debug!("Error setting next pointer on old tail: {}", e);
                    self.tail = Some(old_tail);
                    return Arc::new(RwLock::new(Node::new(new_node.read().unwrap().value.clone())));
                }
                
                if let Err(e) = new_node.write()
                    .map_err(|e| LinkedListError::LockError(e.to_string()))
                    .map(|mut node| node.prev = Some(Arc::downgrade(&old_tail)))
                {
                    debug!("Error setting prev pointer on new node: {}", e);
                    let _ = old_tail.write().map(|mut tail| tail.next = None);
                    self.tail = Some(old_tail);
                    return Arc::new(RwLock::new(Node::new(new_node.read().unwrap().value.clone())));
                }
                
                self.tail = Some(Arc::clone(&new_node));
            }
            None => {
                self.head = Some(Arc::clone(&new_node));
                self.tail = Some(Arc::clone(&new_node));
            }
        }
        
        self.size += 1;
        new_node
    }

    /// Remove the node at the front of the list and return its value
    pub fn pop_front(&mut self) -> Option<T> {
        trace!("Popping from front of list");
        
        self.head.take().and_then(|old_head| {
            self.size -= 1;
            
            let next_node = {
                match old_head.write() {
                    Ok(mut head) => head.next.take(),
                    Err(e) => {
                        debug!("Failed to lock head node: {}", e);
                        None
                    }
                }
            };
            
            match next_node {
                Some(new_head) => {
                    if let Err(e) = new_head.write()
                        .map_err(|e| LinkedListError::LockError(e.to_string()))
                        .map(|mut head| head.prev = None)
                    {
                        debug!("Error updating prev pointer on new head: {}", e);
                    }
                    self.head = Some(new_head);
                }
                None => {
                    self.tail = None;
                }
            }
            
            if let Ok(head) = old_head.read() {
                return Some(head.value.clone());
            }
            
            debug!("Failed to read value from head node, trying Arc::try_unwrap");
            match Arc::try_unwrap(old_head) {
                Ok(lock) => match lock.into_inner() {
                    Ok(node) => Some(node.value),
                    Err(e) => {
                        debug!("Failed to get value from node after unwrapping: {}", e);
                        None
                    }
                },
                Err(_) => {
                    debug!("Failed to unwrap Arc, still has references");
                    None
                }
            }
        })
    }

    /// Remove the node at the back of the list and return its value
    pub fn pop_back(&mut self) -> Option<T> {
        trace!("Popping from back of list");
        
        self.tail.take().and_then(|old_tail| {
            self.size -= 1;
            
            let prev_weak = {
                match old_tail.write() {
                    Ok(mut tail) => tail.prev.take(),
                    Err(e) => {
                        debug!("Failed to lock tail node: {}", e);
                        None
                    }
                }
            };
            
            match prev_weak {
                Some(prev_weak) => {
                    if let Some(prev) = prev_weak.upgrade() {
                        if let Err(e) = prev.write()
                            .map_err(|e| LinkedListError::LockError(e.to_string()))
                            .map(|mut prev| prev.next = None)
                        {
                            debug!("Error updating next pointer on new tail: {}", e);
                        }
                        self.tail = Some(prev);
                    } else {
                        debug!("Failed to upgrade weak reference to prev node");
                        self.head = None;
                    }
                }
                None => {
                    self.head = None;
                }
            }
            
            if let Ok(tail) = old_tail.read() {
                return Some(tail.value.clone());
            }
            
            debug!("Failed to read value from tail node, trying Arc::try_unwrap");
            match Arc::try_unwrap(old_tail) {
                Ok(lock) => match lock.into_inner() {
                    Ok(node) => Some(node.value),
                    Err(e) => {
                        debug!("Failed to get value from node after unwrapping: {}", e);
                        None
                    }
                },
                Err(_) => {
                    debug!("Failed to unwrap Arc, still has references");
                    None
                }
            }
        })
    }

    /// Remove a specific node from the list
    /// Returns Ok(true) if node was successfully removed, 
    /// Ok(false) if node couldn't be found or wasn't removed,
    /// or Err with the error that occurred
    pub fn remove_node(&mut self, node: &Arc<RwLock<Node<T>>>) -> Result<bool, LinkedListError> {
        trace!("Removing specific node from list");
        
        if self.size == 0 {
            return Ok(false);
        }
        
        let is_head = self.head.as_ref().map_or(false, |head| Arc::ptr_eq(head, node));
        let is_tail = self.tail.as_ref().map_or(false, |tail| Arc::ptr_eq(tail, node));
        
        if is_head {
            return match self.pop_front() {
                Some(_) => Ok(true),
                None => Ok(false),
            };
        }
        
        if is_tail {
            return match self.pop_back() {
                Some(_) => Ok(true),
                None => Ok(false),
            };
        }
        
        let node_lock_result = node.write();
        if let Err(e) = node_lock_result {
            debug!("Failed to lock node for removal: {}", e);
            return Err(LinkedListError::LockError(e.to_string()));
        }
        
        let mut node_ref = node_lock_result.unwrap();
        
        if let Some(prev_weak) = node_ref.prev.take() {
            if let Some(prev) = prev_weak.upgrade() {
                if let Some(next) = node_ref.next.take() {
                    if let Err(e) = prev.write()
                        .map_err(|e| LinkedListError::LockError(e.to_string()))
                        .map(|mut prev| prev.next = Some(Arc::clone(&next)))
                    {
                        debug!("Error updating next pointer on prev node: {}", e);
                        node_ref.prev = Some(prev_weak);
                        return Err(LinkedListError::LockError(e.to_string()));
                    }
                    
                    if let Err(e) = next.write()
                        .map_err(|e| LinkedListError::LockError(e.to_string()))
                        .map(|mut next| next.prev = Some(Arc::downgrade(&prev)))
                    {
                        debug!("Error updating prev pointer on next node: {}", e);
                        let _ = prev.write().map(|mut p| p.next = None);
                        node_ref.prev = Some(prev_weak);
                        node_ref.next = Some(next);
                        return Err(LinkedListError::LockError(e.to_string()));
                    }
                    
                    self.size -= 1;
                    return Ok(true);
                }
            }
        }
        
        Ok(false)
    }

    /// Get an iterator over the values in the list (front to back)
    pub fn iter(&self) -> Iter<T> {
        Iter {
            current: self.head.as_ref().map(Arc::clone),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Iterator for LinkedListLru
pub struct Iter<T> {
    current: Option<Arc<RwLock<Node<T>>>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Clone> Iterator for Iter<T> {
    type Item = T;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.current.take().and_then(|current| {
            match current.read() {
                Ok(node) => {
                    let value = node.value.clone();
                    self.current = node.next.as_ref().map(Arc::clone);
                    Some(value)
                },
                Err(e) => {
                    debug!("Failed to read node during iteration: {}", e);
                    None
                }
            }
        })
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for LinkedListLru<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::shared::page::FrameId;

    use super::*;
    
    #[test]
    fn test_basic_operations() {
        let mut list = LinkedListLru::new();
        assert!(list.is_empty());
        
        list.push_back(1);
        list.push_back(2);
        list.push_front(0);
        
        assert_eq!(list.len(), 3);
        assert_eq!(list.pop_front(), Some(0));
        assert_eq!(list.pop_back(), Some(2));
        assert_eq!(list.len(), 1);
    }
    
    #[test]
    fn test_remove_and_move() {
        let mut list = LinkedListLru::new();
        list.push_back(1);
        let node2 = list.push_back(2);
        list.push_back(3);
        
        assert_eq!(list.len(), 3);
        
        let remove_result = list.remove_node(&node2);
        assert!(remove_result.is_ok());
        assert!(remove_result.unwrap());
        assert_eq!(list.len(), 2);
        
        let values: Vec<FrameId> = list.iter().collect();
        assert_eq!(values, vec![1, 3]);
    }
    
    #[test]
    fn test_lru_operations() {
        // Test the interaction with LRUReplacer
        use super::super::lru_replacer::LRUReplacer;
        
        let mut replacer = LRUReplacer::<FrameId>::new();
        
        // Insert some items
        replacer.insert(1);
        replacer.insert(2);
        replacer.insert(3);
        
        assert_eq!(replacer.size(), 3);
        
        // Test victim (should be the least recently used - FrameId 1)
        assert_eq!(replacer.victim(), Some(1));
        assert_eq!(replacer.size(), 2);
        
        // Erase an item
        assert!(replacer.erase(2));
        assert_eq!(replacer.size(), 1);
        
        // Insert some more
        replacer.insert(4);
        replacer.insert(5);
        
        // The victim should now be FrameId 3 since it's the least recently used
        assert_eq!(replacer.victim(), Some(3));
        
        // Test erasing a non-existent item
        assert!(!replacer.erase(10));
    }
}