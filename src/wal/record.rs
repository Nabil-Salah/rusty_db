use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use serde_bencode;
use std::io::{self, Cursor, Error, ErrorKind, Read};
use std::time::{SystemTime, UNIX_EPOCH};
use log::{debug, error, trace};
use anyhow::{Result, Context, anyhow};

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
        debug!("Creating PUT log record with key size {} and value size {}", key.len(), value.len());
        Self {
            timestamp: current_time_millis(),
            operation: LogOperation::Put { key, value },
        }
    }

    /// Create a new log record for a DELETE operation
    pub fn new_delete(key: Vec<u8>) -> Self {
        debug!("Creating DELETE log record with key size {}", key.len());
        Self {
            timestamp: current_time_millis(),
            operation: LogOperation::Delete { key },
        }
    }

    /// Serialize the log record with CRC checksum
    pub fn serialize(&self) -> Result<Vec<u8>> {
        trace!("Serializing log record");
        let payload = serde_bencode::to_bytes(self)
            .map_err(|e| anyhow!("Serialization error: {}", e))?;
        
        trace!("Serialized payload size: {}", payload.len());
        
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();
        
        let mut buffer = Vec::with_capacity(8 + payload.len());
        
        buffer.write_u32::<BigEndian>(crc)
            .context("Failed to write CRC")?;
        buffer.write_u32::<BigEndian>(payload.len() as u32)
            .context("Failed to write payload size")?;
        
        buffer.extend_from_slice(&payload);
        
        debug!("Record serialized with size {}", buffer.len());
        Ok(buffer)
    }

    /// Deserialize a log record from bytes
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 {
            return Err(anyhow!("Record too short (< 8 bytes)"));
        }
        
        let mut cursor = Cursor::new(bytes);
        
        let stored_crc = cursor.read_u32::<BigEndian>()
            .context("Failed to read CRC")?;
        let payload_size = cursor.read_u32::<BigEndian>()
            .context("Failed to read payload size")?;
        
        let header_size = 8; // 4 bytes CRC + 4 bytes size
        let payload_start = header_size as usize;
        let payload_end = payload_start + payload_size as usize;
        
        if bytes.len() < payload_end {
            return Err(anyhow!("Incomplete record: expected {} bytes, got {}", payload_end, bytes.len()));
        }
        
        let payload = &bytes[payload_start..payload_end];
        
        let mut hasher = Hasher::new();
        hasher.update(payload);
        let calculated_crc = hasher.finalize();
        
        if calculated_crc != stored_crc {
            error!("CRC check failed: stored={}, calculated={}", stored_crc, calculated_crc);
            return Err(anyhow!("CRC check failed"));
        }
        
        let record: Self = serde_bencode::from_bytes(payload)
            .map_err(|e| anyhow!("Deserialization error: {}", e))?;
        
        trace!("Successfully deserialized record with timestamp {}", record.timestamp);
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