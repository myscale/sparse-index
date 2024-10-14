// SPDX-License-Identifier: Apache-2.0

#ifndef SPARSE_INDEX_H
#define SPARSE_INDEX_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

using SparseIndexLogCallback = void(*)(int32_t, const char*, const char*);

using SparseIndexDimWeight = float;

extern "C" {

bool sparse_index_log4rs_initialize(const char *log_directory,
                                    const char *log_level,
                                    bool log_in_file,
                                    bool console_display,
                                    bool only_record_sparse_index);

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
bool sparse_index_log4rs_initialize_with_callback(const char *log_directory,
                                                  const char *log_level,
                                                  bool log_in_file,
                                                  bool console_display,
                                                  bool only_record_sparse_index,
                                                  SparseIndexLogCallback callback);

} // extern "C"

#endif // SPARSE_INDEX_H
