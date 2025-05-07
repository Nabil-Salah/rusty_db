mod record;

use chrono::Utc;
pub use record::{LogOperation, LogRecord};

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use log::{debug, error, info, trace, warn};
use anyhow::{Result, Context};


/// Write-Ahead Log for durability and crash recovery
pub struct WriteAheadLog {
    writer: BufWriter<File>,
    path: String,
    last_time: i64,
}

impl WriteAheadLog {
    /// Create a new WriteAheadLog at the specified path with default configuration
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        info!("Creating WriteAheadLog at {}", path_str);
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .context(format!("Failed to open WAL file at {}", path_str))?;

        let writer = BufWriter::new(file);

        Ok(Self {
            writer,
            path: path_str,
            last_time: Utc::now().timestamp(),
        })
    }

    /// Append a log record to the WAL with potential auto-flush
    pub fn append(&mut self, record: LogRecord) -> Result<()> {
        trace!("Appending record with timestamp {}", record.timestamp);
        
        let serialized = record.serialize()
            .context("Failed to serialize log record")?;
        let record_size = serialized.len();
        debug!("Serialized record size: {}", record_size);
        
        self.writer.write_all(&serialized)
            .context("Failed to write record to WAL")?;

        
        if Utc::now().timestamp() - self.last_time > 10 {
            debug!("Auto-flushing WAL (time-based)");
            self.writer.flush()
                .context("Failed to auto-flush WAL")?;
            self.last_time = Utc::now().timestamp();
        }
        
        Ok(())
    }

    /// Create a log record for a PUT operation and append it
    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref();
        let value_bytes = value.as_ref();
        debug!("Creating PUT record for key of size {}", key_bytes.len());
        
        let record = LogRecord::new_put(key_bytes.to_vec(), value_bytes.to_vec());
        self.append(record)
    }

    /// Create a log record for a DELETE operation and append it
    pub fn delete<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref();
        debug!("Creating DELETE record for key of size {}", key_bytes.len());
        
        let record = LogRecord::new_delete(key_bytes.to_vec());
        self.append(record)
    }

    /// Flush any buffered data to disk
    pub fn flush(&mut self) -> Result<()> {
        debug!("Manually flushing WAL");
        self.writer.flush()
            .context("Failed to flush WAL")?;
        self.last_time = Utc::now().timestamp();
        Ok(())
    }

    /// Get the path of the WAL file
    pub fn path(&self) -> &str {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_log_record_serialization() {
        // Create a log record
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        let record = LogRecord::new_put(key.clone(), value.clone());
        
        // Serialize it
        let serialized = record.serialize().unwrap();
        
        // Deserialize it
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        
        // Verify the deserialized record matches the original
        assert_eq!(record.timestamp, deserialized.timestamp);
        
        // Check that the operation was correctly serialized/deserialized
        match deserialized.operation {
            LogOperation::Put { key: k, value: v } => {
                assert_eq!(k, key);
                assert_eq!(v, value);
            }
            _ => panic!("Expected Put operation"),
        }
    }

    #[test]
    fn test_log_record_delete_operation() {
        // Create a delete log record
        let key = b"key_to_delete".to_vec();
        let record = LogRecord::new_delete(key.clone());
        
        // Serialize and deserialize
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        
        // Verify the record
        match deserialized.operation {
            LogOperation::Delete { key: k } => {
                assert_eq!(k, key);
            }
            _ => panic!("Expected Delete operation"),
        }
    }

    #[test]
    fn test_crc_validation() {
        // Create a log record
        let record = LogRecord::new_put(b"key".to_vec(), b"value".to_vec());
        
        // Serialize it
        let mut serialized = record.serialize().unwrap();
        
        // Corrupt the payload (change a byte in the payload, not in the header)
        if serialized.len() > 10 {
            serialized[10] ^= 0xFF; // Flip all bits at position 10
        }
        
        // Attempt to deserialize should fail with an error
        assert!(LogRecord::deserialize(&serialized).is_err());
    }

    #[test]
    fn test_wal_basic_functionality() {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        
        // Create a new WAL
        let mut wal = WriteAheadLog::new(&wal_path).unwrap();
        
        // Write some records
        wal.put("key1", "value1").unwrap();
        wal.put("key2", "value2").unwrap();
        wal.delete("key1").unwrap();
        
        // Flush to ensure all data is written
        wal.flush().unwrap();
        
        // Now verify the file exists and contains data
        assert!(wal_path.exists());
        let metadata = fs::metadata(&wal_path).unwrap();
        assert!(metadata.len() > 0);
    }
    
}