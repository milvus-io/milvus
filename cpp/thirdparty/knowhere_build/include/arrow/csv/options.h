// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_CSV_OPTIONS_H
#define ARROW_CSV_OPTIONS_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/util/visibility.h"

namespace arrow {

class DataType;

namespace csv {

struct ARROW_EXPORT ParseOptions {
  // Parsing options

  // Field delimiter
  char delimiter = ',';
  // Whether quoting is used
  bool quoting = true;
  // Quoting character (if `quoting` is true)
  char quote_char = '"';
  // Whether a quote inside a value is double-quoted
  bool double_quote = true;
  // Whether escaping is used
  bool escaping = false;
  // Escaping character (if `escaping` is true)
  char escape_char = '\\';
  // Whether values are allowed to contain CR (0x0d) and LF (0x0a) characters
  bool newlines_in_values = false;
  // Whether empty lines are ignored.  If false, an empty line represents
  // a single empty value (assuming a one-column CSV file).
  bool ignore_empty_lines = true;

  // XXX Should this be in ReadOptions?
  // Number of header rows to skip (including the first row containing column names)
  int32_t header_rows = 1;

  static ParseOptions Defaults();
};

struct ARROW_EXPORT ConvertOptions {
  // Conversion options

  // Whether to check UTF8 validity of string columns
  bool check_utf8 = true;
  // Optional per-column types (disabling type inference on those columns)
  std::unordered_map<std::string, std::shared_ptr<DataType>> column_types;
  // Recognized spellings for null values
  std::vector<std::string> null_values;
  // Recognized spellings for boolean values
  std::vector<std::string> true_values;
  std::vector<std::string> false_values;
  // Whether string / binary columns can have null values.
  // If true, then strings in "null_values" are considered null for string columns.
  // If false, then all strings are valid string values.
  bool strings_can_be_null = false;

  static ConvertOptions Defaults();
};

struct ARROW_EXPORT ReadOptions {
  // Reader options

  // Whether to use the global CPU thread pool
  bool use_threads = true;
  // Block size we request from the IO layer; also determines the size of
  // chunks when use_threads is true
  int32_t block_size = 1 << 20;  // 1 MB

  static ReadOptions Defaults();
};

}  // namespace csv
}  // namespace arrow

#endif  // ARROW_CSV_OPTIONS_H
