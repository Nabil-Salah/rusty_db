# Rusty DB

RustyDB is a learning project implementing a persistent key-value storage engine based on the Log-Structured Merge-Tree (LSM-Tree) architecture, written entirely in Rust. The primary goal of this project is to explore and understand core database storage engine internals, including memory management, disk I/O, data structures, concurrency, and durability mechanisms.

Note: This is primarily an educational project and is not intended for production use in its current state.

## Features

- **Log-Structured Merge (LSM) Storage**: The core orchestrator exists, managing WAL, active/immutable Memtables (Arc<Memtable>, RwLock state), and holding the shared Arc<BufferPoolManager>. Basic Put/Delete operations writing to WAL and Memtable, including Memtable rotation logic.
- **Write-Ahead Logging (WAL)**: Ensures data durability and supports recovery in case of failures.
- **Buffer Pool Management**: Implements an LRU-based (implemented via HashMap + Doubly Linked List) buffer pool for efficient page caching and eviction, responsible for caching disk pages (SSTable blocks) in memory.
- **SSTable Management**: Supports creation and management of sorted string tables (SSTables) for persistent storage.
- **Memtable**: An in-memory, sorted buffer for recent writes implemented using a concurrent Skip List (crossbeam-skiplist). Handles deletes via tombstones and tracks approximate size for triggering flushes.
- **Concurrency Support**: Designed to handle concurrent access to database resources.

## Project Structure

The project is organized into the following modules:

- `src/main.rs`: Entry point of the application.
- `src/buffer/`: Contains buffer pool management logic, including LRU replacement and linked list utilities.
- `src/lsm_storage/`: Implements the LSM storage engine.
- `src/memtable/`: Provides in-memory table structures for fast data access.
- `src/shared/`: Includes shared utilities like page management.
- `src/sstable/`: Handles SSTable creation and iteration.
- `src/storage/`: Manages disk storage and page operations.
- `src/wal/`: Implements write-ahead logging for durability.
- `src/tests/`: Contains integration tests for various components.

## Getting Started

### Prerequisites

- Rust and Cargo installed on your machine. You can install Rust using [rustup](https://rustup.rs/).

### Building the Project

To build the project, run:

```bash
cargo build
```

### Running Tests

To run the tests, execute:

```bash
cargo test
```

### Running the Application

To run the application, use:

```bash
cargo run
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.
