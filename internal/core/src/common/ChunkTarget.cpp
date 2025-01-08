// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <common/ChunkTarget.h>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include "common/EasyAssert.h"
#include <sys/mman.h>
#include <unistd.h>

const uint32_t SYS_PAGE_SIZE = sysconf(_SC_PAGE_SIZE);
namespace milvus {
void
MemChunkTarget::write(const void* data, size_t size, bool append) {
    AssertInfo(size + size_ <= cap_, "can not exceed target capacity");
    std::memcpy(data_ + size_, data, size);
    size_ += append ? size : 0;
}

void
MemChunkTarget::write(uint32_t value, bool append) {
    AssertInfo(sizeof(uint32_t) + size_ <= cap_, "can not exceed target capacity");
    *reinterpret_cast<uint32_t*>(data_ + size_) = value;
    size_ += append ? sizeof(uint32_t) : 0;
}

void
MemChunkTarget::skip(size_t size) {
    size_ += size;
}

void
MemChunkTarget::seek(size_t offset) {
    size_ = offset;
}

std::pair<char*, size_t>
MemChunkTarget::get() {
    return {data_, cap_};
}

size_t
MemChunkTarget::tell() {
    return size_;
}

void
MmapChunkTarget::flush() {
    if (buffer_.pos == 0) {
        return;
    }

    auto n = file_.Write(buffer_.buf, buffer_.pos);
    AssertInfo(n != -1, "failed to write data to file");
    buffer_.clear();
}

void
MmapChunkTarget::write(const void* data, size_t size, bool append) {
    if (buffer_.sufficient(size)) {
        buffer_.write(data, size);
        size_ += append ? size : 0;
        return;
    }

    flush();

    if (buffer_.sufficient(size)) {
        buffer_.write(data, size);
        size_ += append ? size : 0;
        return;
    }

    auto n = file_.Write(data, size);
    AssertInfo(n != -1, "failed to write data to file");
    size_ += append ? size : 0;
}

void
MmapChunkTarget::write(uint32_t value, bool append) {
    if (buffer_.sufficient(sizeof(uint32_t))) {
        buffer_.write(value);
        size_ += append ? sizeof(uint32_t) : 0;
        return;
    }
    flush();
    if (buffer_.sufficient(sizeof(uint32_t))) {
        buffer_.write(value);
        size_ += append ? sizeof(uint32_t) : 0;
        return;
    }
    // highly impossible to reach, as buffer is at least 16kb
    auto n = file_.Write(reinterpret_cast<void*>(&value), sizeof(uint32_t));
    AssertInfo(n != -1, "failed to write data to file");
    size_ += append ? sizeof(uint32_t) : 0;
}

void
MmapChunkTarget::skip(size_t size) {
    flush();
    file_.Seek(size, SEEK_CUR);
    size_ += size;
}

void
MmapChunkTarget::seek(size_t offset) {
    flush();
    file_.Seek(offset_ + offset, SEEK_SET);
}

std::pair<char*, size_t>
MmapChunkTarget::get() {
    // Write padding to align with the page size, ensuring the offset_ aligns with the page size.
    auto padding_size =
        (size_ / SYS_PAGE_SIZE + (size_ % SYS_PAGE_SIZE != 0)) * SYS_PAGE_SIZE -
        size_;
    char padding[padding_size];
    memset(padding, 0, sizeof(padding));
    write(padding, padding_size);

    flush();
    file_.FFlush();

    auto m = mmap(
        nullptr, size_, PROT_READ, MAP_SHARED, file_.Descriptor(), offset_);
    AssertInfo(m != MAP_FAILED,
               "failed to map: {}, map_size={}, offset={}",
               strerror(errno),
               size_,
               offset_);
    return {(char*)m, size_};
}

size_t
MmapChunkTarget::tell() {
    return size_;
}
}  // namespace milvus