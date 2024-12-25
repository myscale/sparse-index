use std::ffi::{c_char, CStr};

use crate::error_ck;

use super::utils::{empty_log_callback, LogCallback, LoggerConfig, SparseIndexLogger, LOG4RS_HANDLE, LOG_CALLBACK};

#[no_mangle]
pub extern "C" fn sparse_index_log4rs_initialize(
    log_directory: *const c_char,
    log_level: *const c_char,
    log_in_file: bool,
    console_display: bool,
    only_record_sparse_index: bool,
) -> bool {
    sparse_index_log4rs_initialize_with_callback(log_directory, log_level, log_in_file, console_display, only_record_sparse_index, empty_log_callback)
}
/// Initializes the logger configuration for the sparse_index lib.
///
/// Arguments:
/// - `log_path`: The path where log files are saved. sparse_index lib will generate multiple log files.
/// - `log_level`: The logging level to use. Supported levels: info, debug, trace, error, warn.
/// - `log_in_file`: Whether record log content in file.
/// - `console_display`: Enables logging to the console if set to true.
/// - `only_record_sparse_index`: Only record `target=sparse_index` log content.
/// - `callback`: A callback function, typically provided by ClickHouse.
///
/// Returns:
/// - `true` if the logger is successfully initialized, `false` otherwise.
#[no_mangle]
pub extern "C" fn sparse_index_log4rs_initialize_with_callback(
    log_directory: *const c_char,
    log_level: *const c_char,
    log_in_file: bool,
    console_display: bool,
    only_record_sparse_index: bool,
    callback: LogCallback,
) -> bool {
    if log_directory.is_null() || log_level.is_null() {
        error_ck!("`log_directory` or `log_level` can't be nullptr");
        return false;
    }
    // Safely convert C strings to Rust String, checking for null pointers.
    let log_directory: String = match unsafe { CStr::from_ptr(log_directory) }.to_str() {
        Ok(path) => path.to_owned(),
        Err(_) => {
            error_ck!("`log_directory` (string) is invalid");
            return false;
        }
    };
    let log_level: String = match unsafe { CStr::from_ptr(log_level) }.to_str() {
        Ok(level) => level.to_owned(),
        Err(_) => {
            error_ck!("`log_level` (string) is invalid");
            return false;
        }
    };

    let logger_config = LoggerConfig::new(log_directory.clone(), log_level.clone(), log_in_file, console_display, only_record_sparse_index);

    match SparseIndexLogger::update_log_callback(&LOG_CALLBACK, callback) {
        Ok(_) => {}
        Err(e) => {
            error_ck!("{:?}", e);
            return false;
        }
    };
    let config = match logger_config.build_logger_config() {
        Ok(config) => config,
        Err(e) => {
            error_ck!("{:?}", e);
            return false;
        }
    };
    match SparseIndexLogger::update_log4rs_handler(&LOG4RS_HANDLE, config) {
        Ok(_) => {}
        Err(e) => {
            error_ck!("{:?}", e);
            return false;
        }
    };

    true
}
