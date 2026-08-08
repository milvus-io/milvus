// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/FastMem.h"
#include "filemanager/InputStream.h"

namespace milvus::storage::detail {

// Serves fully covered ranges from a retained suffix and forwards every other
// operation to the wrapped stream.
class TailCachedInputStream final : public milvus::InputStream {
 public:
    TailCachedInputStream(std::shared_ptr<milvus::InputStream> inner,
                          size_t tail_offset,
                          std::vector<uint8_t> tail)
        : inner_(std::move(inner)),
          tail_offset_(tail_offset),
          tail_(std::move(tail)) {
    }

    using milvus::InputStream::Read;

    size_t
    Size() const override {
        return inner_->Size();
    }

    bool
    Seek(int64_t offset) override {
        return inner_->Seek(offset);
    }

    size_t
    Tell() const override {
        return inner_->Tell();
    }

    bool
    Eof() const override {
        return inner_->Eof();
    }

    size_t
    Read(void* data, size_t size) override {
        return inner_->Read(data, size);
    }

    size_t
    ReadAt(void* data, size_t offset, size_t size) override {
        // Partial stitching would combine cached and provider reads into one
        // logical operation and change short-read/provider-error semantics.
        if (offset >= tail_offset_) {
            const size_t relative_offset = offset - tail_offset_;
            if (relative_offset <= tail_.size() &&
                size <= tail_.size() - relative_offset) {
                if (size > 0) {
                    milvus::fastmem::FastMemcpy(
                        data, tail_.data() + relative_offset, size);
                }
                return size;
            }
        }
        return inner_->ReadAt(data, offset, size);
    }

    size_t
    Read(int fd, size_t size) override {
        return inner_->Read(fd, size);
    }

 private:
    const std::shared_ptr<milvus::InputStream> inner_;
    const size_t tail_offset_;
    const std::vector<uint8_t> tail_;
};

}  // namespace milvus::storage::detail
