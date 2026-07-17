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

#include <span>

#include <cstddef>

namespace milvus::local {
class FileSystem;
}

namespace milvus::local::io {

class MappedRegion final {
 public:
    MappedRegion() = default;

    MappedRegion(const MappedRegion&) = delete;
    MappedRegion&
    operator=(const MappedRegion&) = delete;

    MappedRegion(MappedRegion&& other) noexcept;
    MappedRegion&
    operator=(MappedRegion&& other) noexcept;

    ~MappedRegion();

    std::span<const std::byte>
    Data() const noexcept {
        return {data_, size_};
    }

 private:
    friend class local::FileSystem;

    MappedRegion(void* mapping,
                 size_t mapping_size,
                 const std::byte* data,
                 size_t size) noexcept;

    void
    Reset() noexcept;

    void* mapping_{nullptr};
    size_t mapping_size_{0};
    const std::byte* data_{nullptr};
    size_t size_{0};
};

}  // namespace milvus::local::io
