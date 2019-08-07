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

#pragma once

#include <memory>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;

namespace json {

struct ParseOptions;

/// \class Chunker
/// \brief A reusable block-based chunker for JSON data
///
/// The chunker takes a block of JSON data and finds a suitable place
/// to cut it up without splitting an object.
class ARROW_EXPORT Chunker {
 public:
  virtual ~Chunker() = default;

  /// \brief Carve up a chunk in a block of data to contain only whole objects
  /// \param[in] block json data to be chunked
  /// \param[out] whole subrange of block containing whole json objects
  /// \param[out] partial subrange of block a partial json object
  virtual Status Process(const std::shared_ptr<Buffer>& block,
                         std::shared_ptr<Buffer>* whole,
                         std::shared_ptr<Buffer>* partial) = 0;

  /// \brief Carve the completion of a partial object out of a block
  /// \param[in] partial incomplete json object
  /// \param[in] block json data
  /// \param[out] completion subrange of block containing the completion of partial
  /// \param[out] rest subrange of block containing what completion does not cover
  virtual Status ProcessWithPartial(const std::shared_ptr<Buffer>& partial,
                                    const std::shared_ptr<Buffer>& block,
                                    std::shared_ptr<Buffer>* completion,
                                    std::shared_ptr<Buffer>* rest) = 0;

  static std::unique_ptr<Chunker> Make(const ParseOptions& options);

 protected:
  Chunker() = default;
  ARROW_DISALLOW_COPY_AND_ASSIGN(Chunker);
};

}  // namespace json
}  // namespace arrow
