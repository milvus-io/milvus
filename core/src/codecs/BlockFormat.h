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
#include <string>
#include <vector>

#include "db/Types.h"
#include "knowhere/common/BinarySet.h"
#include "storage/FSHandler.h"

namespace milvus {
namespace codec {

struct ReadRange {
    ReadRange(int64_t offset, int64_t num_bytes) : offset_(offset), num_bytes_(num_bytes) {
    }
    int64_t offset_;
    int64_t num_bytes_;
};

using ReadRanges = std::vector<ReadRange>;

class BlockFormat {
 public:
    BlockFormat() = default;

    void
    Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, engine::BinaryDataPtr& raw);

    void
    Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, int64_t offset, int64_t num_bytes,
         engine::BinaryDataPtr& raw);

    void
    Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, const ReadRanges& read_ranges,
         engine::BinaryDataPtr& raw);

    void
    Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, const engine::BinaryDataPtr& raw);

    // No copy and move
    BlockFormat(const BlockFormat&) = delete;
    BlockFormat(BlockFormat&&) = delete;

    BlockFormat&
    operator=(const BlockFormat&) = delete;
    BlockFormat&
    operator=(BlockFormat&&) = delete;
};

using BlockFormatPtr = std::shared_ptr<BlockFormat>;

}  // namespace codec
}  // namespace milvus
