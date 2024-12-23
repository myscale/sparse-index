#[macro_export]
macro_rules! row_content(
    () => {
        {
            ($crate::SparseRowContent::default())
        }
    }; // avoids a warning due to the useless `mut`.
    ($($field:expr => $value:expr),*) => {
        {
            let mut row = $crate::core::SparseRowContent::default();
            $(
                row.add_sparse_vector($field, $value);
            )*
            row
        }
    };
    // if there is a trailing comma retry with the trailing comma stripped.
    ($($field:expr => $value:expr),+ ,) => {
        row_content!( $( $field => $value ), *)
    };
);

#[macro_export]
macro_rules! thread_name {
    () => {
        std::thread::current().name().unwrap_or_default()
    };
}

#[macro_export]
macro_rules! error_ck {
    // provide `target`, `function` and `message`
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::error!(target: $target, "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(-1, format!("[{} - {}] ~ {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `function` and `message`
    (function: $function:expr, $($arg:tt)+) => {{
        log::error!(target: "sparse_index", "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(-1, format!("[sparse_index - {}] ~ {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `target` and `message`
    (target: $target:expr, $($arg:tt)+) => {{
        log::error!(target: $target, $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(-1, format!("[{} - null_func] ~ {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide `message`.
    ($($arg:tt)+) => {{
        log::error!(target: "sparse_index", $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(-1, format!("[sparse_index] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[macro_export]
macro_rules! warn_ck {
    // provide `target`, `function` and `message`
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::warn!(target: $target, "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(0, format!("[{} - {}] ~ {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `function` and `message`
    (function: $function:expr, $($arg:tt)+) => {{
        log::warn!(target: "sparse_index", "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(0, format!("[sparse_index - {}] ~ {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `target` and `message`
    (target: $target:expr, $($arg:tt)+) => {{
        log::warn!(target: $target, $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(0, format!("[{} - null_func] ~ {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide `message`.
    ($($arg:tt)+) => {{
        log::warn!(target: "sparse_index", $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(0, format!("[sparse_index] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[macro_export]
macro_rules! info_ck {
    // provide `target`, `function` and `message`
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::info!(target: $target, "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(1, format!("[{} - {}] ~ {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `function` and `message`
    (function: $function:expr, $($arg:tt)+) => {{
        log::info!(target: "sparse_index", "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(1, format!("[sparse_index - {}] ~ {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `target` and `message`
    (target: $target:expr, $($arg:tt)+) => {{
        log::info!(target: $target, $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(1, format!("[{} - null_func] ~ {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide `message`.
    ($($arg:tt)+) => {{
        log::info!(target: "sparse_index", $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(1, format!("[sparse_index] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[macro_export]
macro_rules! debug_ck {
    // provide `target`, `function` and `message`
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::debug!(target: $target, "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(2, format!("[{} - {}] ~ {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `function` and `message`
    (function: $function:expr, $($arg:tt)+) => {{
        log::debug!(target: "sparse_index", "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(2, format!("[sparse_index - {}] ~ {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `target` and `message`
    (target: $target:expr, $($arg:tt)+) => {{
        log::debug!(target: $target, $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(2, format!("[{} - null_func] ~ {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide `message`.
    ($($arg:tt)+) => {{
        log::debug!(target: "sparse_index", $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(2, format!("[sparse_index] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[macro_export]
macro_rules! trace_ck {
    // provide `target`, `function` and `message`
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::trace!(target: $target, "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(2, format!("[{} - {}] ~ {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `function` and `message`
    (function: $function:expr, $($arg:tt)+) => {{
        log::trace!(target: "sparse_index", "{} ~ {}", $function, format_args!($($arg)+));
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(2, format!("[sparse_index - {}] ~ {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide `target` and `message`
    (target: $target:expr, $($arg:tt)+) => {{
        log::trace!(target: $target, $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(2, format!("[{} - null_func] ~ {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide `message`.
    ($($arg:tt)+) => {{
        log::trace!(target: "sparse_index", $($arg)+);
        if let Some(callback) = crate::api::cxx_ffi::utils::LOG_CALLBACK.get() {
            crate::api::cxx_ffi::utils::SparseIndexLogger::trigger_logger_callback(2, format!("[sparse_index] - {}", format!($($arg)+)), *callback);
        }
    }};
}
