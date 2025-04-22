use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use serde_bencode;
use std::io::{self, Cursor, Error, ErrorKind, Read};
use std::time::{SystemTime, UNIX_EPOCH};

/// Operations that can be performed in a log record
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogOperation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
}

/// A record in the write-ahead log
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    pub timestamp: u64,
    pub operation: LogOperation,
}

impl LogRecord {
    /// Create a new log record for a PUT operation
    pub fn new_put(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            timestamp: current_time_millis(),
            operation: LogOperation::Put { key, value },
        }
    }

    /// Create a new log record for a DELETE operation
    pub fn new_delete(key: Vec<u8>) -> Self {
        Self {
            timestamp: current_time_millis(),
            operation: LogOperation::Delete { key },
        }
    }

    /// Serialize the log record with CRC checksum
    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        let payload = serde_bencode::to_bytes(self)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Serialization error: {}", e)))?;
        
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();
        
        let mut buffer = Vec::with_capacity(8 + payload.len());
        
        buffer.write_u32::<BigEndian>(crc)
            .map_err(|e| Error::new(ErrorKind::Other, format!("Failed to write CRC: {}", e)))?;
        buffer.write_u32::<BigEndian>(payload.len() as u32)
            .map_err(|e| Error::new(ErrorKind::Other, format!("Failed to write size: {}", e)))?;
        
        buffer.extend_from_slice(&payload);
        
        Ok(buffer)
    }

    /// Deserialize a log record from bytes
    pub fn deserialize(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < 8 {
            return Err(Error::new(ErrorKind::InvalidData, "Record too short"));
        }
        
        let mut cursor = Cursor::new(bytes);
        
        let stored_crc = cursor.read_u32::<BigEndian>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Failed to read CRC: {}", e)))?;
        let payload_size = cursor.read_u32::<BigEndian>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Failed to read size: {}", e)))?;
        
        let header_size = 8; // 4 bytes CRC + 4 bytes size
        let payload_start = header_size as usize;
        let payload_end = payload_start + payload_size as usize;
        
        if bytes.len() < payload_end {
            return Err(Error::new(ErrorKind::InvalidData, "Incomplete record"));
        }
        
        let payload = &bytes[payload_start..payload_end];
        
        let mut hasher = Hasher::new();
        hasher.update(payload);
        let calculated_crc = hasher.finalize();
        
        if calculated_crc != stored_crc {
            return Err(Error::new(ErrorKind::InvalidData, "CRC check failed"));
        }
        
        let record = serde_bencode::from_bytes(payload)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Deserialization error: {}", e)))?;
        
        Ok(record)
    }
}

/// Helper function to get current time in milliseconds
fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_millis() as u64
}