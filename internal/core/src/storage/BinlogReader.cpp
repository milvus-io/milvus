// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/BinlogReader.h"
#include "common/EasyAssert.h"

namespace milvus::storage {

milvus::SegcoreError
BinlogReader::Read(int64_t nbytes, void* out) {
    auto remain = size_ - tell_;
    if (nbytes > remain) {
        return SegcoreError(milvus::UnexpectedError,
                            "out range of binlog data");
    }
    std::memcpy(out, data_.get() + tell_, nbytes);
    tell_ += nbytes;
    return SegcoreError(milvus::Success, "");
}

std::pair<milvus::SegcoreError, std::shared_ptr<uint8_t[]>>
BinlogReader::Read(int64_t nbytes) {
    auto remain = size_ - tell_;
    if (nbytes > remain) {
        return std::make_pair(
            SegcoreError(milvus::UnexpectedError, "out range of binlog data"),
            nullptr);
    }
    auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
    auto res = std::shared_ptr<uint8_t[]>(data_.get() + tell_, deleter);
    tell_ += nbytes;
    return std::make_pair(SegcoreError(milvus::Success, ""), res);
}

}  // namespace milvus::storage
