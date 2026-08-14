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

#include "common/OffsetMapping.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace milvus {

namespace {

class FileDescriptorGuard {
 public:
    explicit FileDescriptorGuard(int fd) : fd_(fd) {
    }

    ~FileDescriptorGuard() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    int
    Get() const {
        return fd_;
    }

 private:
    int fd_;
};

}  // namespace

OffsetMappingArray::~OffsetMappingArray() {
    Clear();
}

void
OffsetMappingArray::Resize(size_t size,
                           int32_t value,
                           bool enable_mmap,
                           std::string filepath) {
    Clear();
    if (size == 0) {
        return;
    }

    if (enable_mmap) {
        AssertInfo(!filepath.empty(), "offset mapping mmap filepath is empty");
        AssertInfo(size <= std::numeric_limits<size_t>::max() / sizeof(int32_t),
                   "offset mapping mmap buffer is too large");

        const auto mmap_size = sizeof(int32_t) * size;
        std::filesystem::create_directories(
            std::filesystem::path(filepath).parent_path());

        const int fd = open(filepath.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
        if (fd < 0) {
            ThrowInfo(MmapError,
                      "failed to create offset mapping mmap file {}: {}",
                      filepath,
                      std::strerror(errno));
        }
        FileDescriptorGuard fd_guard(fd);

        if (ftruncate(fd_guard.Get(), mmap_size) != 0) {
            const auto err = errno;
            unlink(filepath.c_str());
            ThrowInfo(MmapError,
                      "failed to resize offset mapping mmap file {} to {} "
                      "bytes: {}",
                      filepath,
                      mmap_size,
                      std::strerror(err));
        }

        auto* data = mmap(nullptr,
                          mmap_size,
                          PROT_READ | PROT_WRITE,
                          MAP_SHARED,
                          fd_guard.Get(),
                          0);
        if (data == MAP_FAILED) {
            const auto err = errno;
            unlink(filepath.c_str());
            ThrowInfo(MmapError,
                      "failed to mmap offset mapping file {}: {}",
                      filepath,
                      std::strerror(err));
        }

        mmap_data_ = static_cast<int32_t*>(data);
        mmap_size_ = mmap_size;
        mmap_filepath_ = std::move(filepath);
        size_ = size;
        std::fill_n(mmap_data_, size_, value);
        return;
    }

    vec_.assign(size, value);
    size_ = size;
}

void
OffsetMappingArray::Clear() {
    if (mmap_data_ != nullptr) {
        munmap(mmap_data_, mmap_size_);
    }
    if (!mmap_filepath_.empty()) {
        unlink(mmap_filepath_.c_str());
    }
    size_ = 0;
    vec_.clear();
    mmap_data_ = nullptr;
    mmap_size_ = 0;
    mmap_filepath_.clear();
}

bool
OffsetMapping::IsValid(int64_t logical_offset) const {
    return GetPhysicalOffset(logical_offset) >= 0;
}

bool
OffsetMapping::ShouldSkipBitsetTransform(const BitsetView& bitset,
                                         int64_t total_count,
                                         TargetBitmap& result,
                                         BitsetTransformStatus& status) {
    result.clear();
    if (bitset.empty()) {
        status = BitsetTransformStatus::NoFilter;
        return true;
    }
    if (bitset.all()) {
        status = BitsetTransformStatus::AllFiltered;
        return true;
    }
    if (static_cast<int64_t>(bitset.size()) >= total_count && bitset.none()) {
        status = BitsetTransformStatus::NoFilter;
        return true;
    }
    return false;
}

bool
OffsetMapping::IsMmap() const {
    return false;
}

int64_t
NoOpOffsetMapping::GetPhysicalOffset(int64_t logical_offset) const {
    return logical_offset;
}

int64_t
NoOpOffsetMapping::GetLogicalOffset(int64_t physical_offset) const {
    return physical_offset;
}

int64_t
NoOpOffsetMapping::GetValidCount() const {
    return 0;
}

int64_t
NoOpOffsetMapping::ValidCountBelow(int64_t logical_bound) const {
    // No mapping: physical space == logical space.
    return std::max<int64_t>(0, logical_bound);
}

bool
NoOpOffsetMapping::IsEnabled() const {
    return false;
}

int64_t
NoOpOffsetMapping::GetTotalCount() const {
    return 0;
}

OffsetMapping::BitsetTransformStatus
NoOpOffsetMapping::TransformBitset(const BitsetView& bitset,
                                   TargetBitmap& result) const {
    (void)bitset;
    result.clear();
    return BitsetTransformStatus::NoFilter;
}

void
NoOpOffsetMapping::TransformOffsets(std::vector<int64_t>& offsets) const {
    (void)offsets;
}

void
NoOpOffsetMapping::TransformLogicalOffsets(
    std::vector<int64_t>& offsets) const {
    (void)offsets;
}

void
NoOpOffsetMapping::FilterValidLogicalOffsets(
    const int64_t* logical_offsets,
    int64_t count,
    bool* valid_data,
    std::vector<int64_t>& physical_offsets) const {
    physical_offsets.clear();
    physical_offsets.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        const bool valid = logical_offsets[i] >= 0;
        valid_data[i] = valid;
        if (valid) {
            physical_offsets.push_back(logical_offsets[i]);
        }
    }
}

}  // namespace milvus
