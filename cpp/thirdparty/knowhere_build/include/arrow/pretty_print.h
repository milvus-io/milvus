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

#ifndef ARROW_PRETTY_PRINT_H
#define ARROW_PRETTY_PRINT_H

#include <iosfwd>
#include <string>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Column;
class ChunkedArray;
class RecordBatch;
class Schema;
class Status;
class Table;

struct PrettyPrintOptions {
  PrettyPrintOptions(int indent_arg, int window_arg = 10, int indent_size_arg = 2,
                     std::string null_rep_arg = "null", bool skip_new_lines_arg = false)
      : indent(indent_arg),
        indent_size(indent_size_arg),
        window(window_arg),
        null_rep(null_rep_arg),
        skip_new_lines(skip_new_lines_arg) {}

  /// Number of spaces to shift entire formatted object to the right
  int indent;

  /// Size of internal indents
  int indent_size;

  /// Maximum number of elements to show at the beginning and at the end.
  int window;

  /// String to use for representing a null value, defaults to "null"
  std::string null_rep;

  /// Skip new lines between elements, defaults to false
  bool skip_new_lines;
};

/// \brief Print human-readable representation of RecordBatch
ARROW_EXPORT
Status PrettyPrint(const RecordBatch& batch, int indent, std::ostream* sink);

/// \brief Print human-readable representation of Table
ARROW_EXPORT
Status PrettyPrint(const Table& table, const PrettyPrintOptions& options,
                   std::ostream* sink);

/// \brief Print human-readable representation of Array
ARROW_EXPORT
Status PrettyPrint(const Array& arr, int indent, std::ostream* sink);

/// \brief Print human-readable representation of Array
ARROW_EXPORT
Status PrettyPrint(const Array& arr, const PrettyPrintOptions& options,
                   std::ostream* sink);

/// \brief Print human-readable representation of Array
ARROW_EXPORT
Status PrettyPrint(const Array& arr, const PrettyPrintOptions& options,
                   std::string* result);

/// \brief Print human-readable representation of ChunkedArray
ARROW_EXPORT
Status PrettyPrint(const ChunkedArray& chunked_arr, const PrettyPrintOptions& options,
                   std::ostream* sink);

/// \brief Print human-readable representation of ChunkedArray
ARROW_EXPORT
Status PrettyPrint(const ChunkedArray& chunked_arr, const PrettyPrintOptions& options,
                   std::string* result);

/// \brief Print human-readable representation of Column
ARROW_EXPORT
Status PrettyPrint(const Column& column, const PrettyPrintOptions& options,
                   std::ostream* sink);

ARROW_EXPORT
Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::ostream* sink);

ARROW_EXPORT
Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::string* result);

ARROW_EXPORT
Status DebugPrint(const Array& arr, int indent);

}  // namespace arrow

#endif  // ARROW_PRETTY_PRINT_H
