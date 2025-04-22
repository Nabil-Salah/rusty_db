use std::sync::{Arc, Weak, RwLock};
use std::fmt;

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
        let new_node = Arc::new(RwLock::new(Node::new(value)));
        
        match self.head.take() {
            Some(old_head) => {
                old_head.write().unwrap().prev = Some(Arc::downgrade(&new_node));
                new_node.write().unwrap().next = Some(old_head);
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
        let new_node = Arc::new(RwLock::new(Node::new(value)));
        
        match self.tail.take() {
            Some(old_tail) => {
                old_tail.write().unwrap().next = Some(Arc::clone(&new_node));
                new_node.write().unwrap().prev = Some(Arc::downgrade(&old_tail));
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
        self.head.take().map(|old_head| {
            self.size -= 1;
            
            match old_head.write().unwrap().next.take() {
                Some(new_head) => {
                    new_head.write().unwrap().prev = None;
                    self.head = Some(new_head);
                }
                None => {
                    self.tail = None;
                }
            }
            
            let value = old_head.read().unwrap().value.clone();
            
            // Try to unwrap, but if there are still references, just return the value
            match Arc::try_unwrap(old_head) {
                Ok(lock) => lock.into_inner().unwrap().value,
                Err(_) => value,
            }
        })
    }

    /// Remove the node at the back of the list and return its value
    pub fn pop_back(&mut self) -> Option<T> {
        self.tail.take().map(|old_tail| {
            self.size -= 1;
            
            match old_tail.write().unwrap().prev.take() {
                Some(prev_weak) => {
                    if let Some(prev) = prev_weak.upgrade() {
                        prev.write().unwrap().next = None;
                        self.tail = Some(prev);
                    }
                }
                None => {
                    self.head = None;
                }
            }
            
            let value = old_tail.read().unwrap().value.clone();
            
            // Try to unwrap, but if there are still references, just return the value
            match Arc::try_unwrap(old_tail) {
                Ok(lock) => lock.into_inner().unwrap().value,
                Err(_) => value,
            }
        })
    }

    /// Remove a specific node from the list
    pub fn remove_node(&mut self, node: &Arc<RwLock<Node<T>>>) {
        if self.size == 0 {
            return;
        }
        
        let is_head = self.head.as_ref().map_or(false, |head| Arc::ptr_eq(head, node));
        let is_tail = self.tail.as_ref().map_or(false, |tail| Arc::ptr_eq(tail, node));
        
        // Handle removing head
        if is_head {
            self.pop_front();
            return;
        }
        
        // Handle removing tail
        if is_tail {
            self.pop_back();
            return;
        }
        
        // Handle removing from middle
        let mut node_ref = node.write().unwrap();
        
        if let Some(prev_weak) = node_ref.prev.take() {
            if let Some(prev) = prev_weak.upgrade() {
                if let Some(next) = node_ref.next.take() {
                    // Link prev and next nodes
                    prev.write().unwrap().next = Some(Arc::clone(&next));
                    next.write().unwrap().prev = Some(Arc::downgrade(&prev));
                    
                    self.size -= 1;
                }
            }
        }
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
        self.current.take().map(|current| {
            let value = current.read().unwrap().value.clone();
            self.current = current.read().unwrap().next.as_ref().map(Arc::clone);
            value
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
        
        list.remove_node(&node2);
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
        assert_eq!(replacer.erase(2), true);
        assert_eq!(replacer.size(), 1);
        
        // Insert some more
        replacer.insert(4);
        replacer.insert(5);
        
        // The victim should now be FrameId 3 since it's the least recently used
        assert_eq!(replacer.victim(), Some(3));
        
        // Test erasing a non-existent item
        assert_eq!(replacer.erase(10), false);
    }
}