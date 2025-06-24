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

#pragma once

#include <memory>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::storage {

class BinlogReader {
 public:
    explicit BinlogReader(const std::shared_ptr<uint8_t[]> binlog_data,
                          int64_t length)
        : data_(binlog_data), size_(length), tell_(0) {
    }

    template <typename T>
    SegcoreError
    ReadSingleValue(T& val) {
        auto needed_size = sizeof(T);
        if (needed_size > size_ - tell_) {
            return SegcoreError(milvus::UnexpectedError,
                                "out range of binlog data");
        }
        val = *reinterpret_cast<T*>(data_.get() + tell_);
        tell_ += needed_size;
        return SegcoreError(milvus::Success, "");
    }

    SegcoreError
    Read(int64_t nbytes, void* out);

    std::pair<SegcoreError, std::shared_ptr<uint8_t[]>>
    Read(int64_t nbytes);

    int64_t
    Tell() const {
        return tell_;
    }

 private:
    std::shared_ptr<uint8_t[]> data_;
    int64_t size_;
    int64_t tell_;
};

using BinlogReaderPtr = std::shared_ptr<BinlogReader>;

}  // namespace milvus::storage
