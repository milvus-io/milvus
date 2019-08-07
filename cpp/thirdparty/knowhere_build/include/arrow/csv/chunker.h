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

#ifndef ARROW_CSV_CHUNKER_H
#define ARROW_CSV_CHUNKER_H

#include <cstdint>

#include "arrow/csv/options.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace csv {

/// \class Chunker
/// \brief A reusable block-based chunker for CSV data
///
/// The chunker takes a block of CSV data and finds a suitable place
/// to cut it up without splitting a row.
/// If the block is truncated (i.e. not all data can be chunked), it is up
/// to the caller to arrange the next block to start with the trailing data.
///
/// Note: if the previous block ends with CR (0x0d) and a new block starts
/// with LF (0x0a), the chunker will consider the leading newline as an empty line.
class ARROW_EXPORT Chunker {
 public:
  explicit Chunker(ParseOptions options);

  /// \brief Carve up a chunk in a block of data
  ///
  /// Process a block of CSV data, reading up to size bytes.
  /// The number of bytes in the chunk is returned in out_size.
  Status Process(const char* data, uint32_t size, uint32_t* out_size);

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Chunker);

  // Like Process(), but specialized for some parsing options
  template <bool quoting, bool escaping>
  Status ProcessSpecialized(const char* data, uint32_t size, uint32_t* out_size);

  // Detect a single line from the data pointer.  Return the line end,
  // or nullptr if the remaining line is truncated.
  template <bool quoting, bool escaping>
  inline const char* ReadLine(const char* data, const char* data_end);

  ParseOptions options_;
};

}  // namespace csv
}  // namespace arrow

#endif  // ARROW_CSV_CHUNKER_H
