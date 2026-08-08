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

#include "local/io/MappedRegion.h"

#include <sys/mman.h>
#include <utility>

namespace milvus::local::io {

MappedRegion::MappedRegion(void* mapping,
                           size_t mapping_size,
                           const std::byte* data,
                           size_t size) noexcept
    : mapping_(mapping), mapping_size_(mapping_size), data_(data), size_(size) {
}

MappedRegion::MappedRegion(MappedRegion&& other) noexcept
    : mapping_(std::exchange(other.mapping_, nullptr)),
      mapping_size_(std::exchange(other.mapping_size_, 0)),
      data_(std::exchange(other.data_, nullptr)),
      size_(std::exchange(other.size_, 0)) {
}

MappedRegion&
MappedRegion::operator=(MappedRegion&& other) noexcept {
    if (this != &other) {
        Reset();
        mapping_ = std::exchange(other.mapping_, nullptr);
        mapping_size_ = std::exchange(other.mapping_size_, 0);
        data_ = std::exchange(other.data_, nullptr);
        size_ = std::exchange(other.size_, 0);
    }
    return *this;
}

MappedRegion::~MappedRegion() {
    Reset();
}

void
MappedRegion::Reset() noexcept {
    if (mapping_ != nullptr) {
        munmap(mapping_, mapping_size_);
    }
    mapping_ = nullptr;
    mapping_size_ = 0;
    data_ = nullptr;
    size_ = 0;
}

}  // namespace milvus::local::io
