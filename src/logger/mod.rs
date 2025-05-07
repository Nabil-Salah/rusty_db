use log::{Level, LevelFilter, Log, Metadata, Record, SetLoggerError};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

/// Global debug mode flag
static DEBUG_MODE: AtomicBool = AtomicBool::new(false);

/// Custom logger implementation for RustyDB
struct RustyDBLogger;

impl Log for RustyDBLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        if DEBUG_MODE.load(Ordering::Relaxed) {
            metadata.level() <= Level::Debug
        } else {
            metadata.level() <= Level::Info
        }
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            let level = record.level();
            let target = record.target();
            let args = record.args();
            
            // File and line information when available
            let location = match (record.file(), record.line()) {
                (Some(file), Some(line)) => format!(" [{}:{}]", file, line),
                _ => String::new(),
            };

            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "[{} {} {}{}] {}", now, level, target, location, args);
        }
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

// Static instance of our logger
static LOGGER: RustyDBLogger = RustyDBLogger;

/// Initialize the RustyDB logger
pub fn init(debug_mode: bool) -> Result<(), SetLoggerError> {
    // Set the global debug mode flag
    DEBUG_MODE.store(debug_mode, Ordering::Relaxed);

    // Set the logger
    let max_level = if debug_mode {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    };

    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(max_level))
}

/// Check if debug mode is enabled
pub fn is_debug_mode() -> bool {
    DEBUG_MODE.load(Ordering::Relaxed)
}

/// Re-export log macros for convenience
pub use log::{debug, error, info, trace, warn};