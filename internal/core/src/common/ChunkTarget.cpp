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
#include <cstring>
#include "common/EasyAssert.h"
#include <sys/mman.h>

namespace milvus {
void
MemChunkTarget::write(const void* data, size_t size, bool append) {
    AssertInfo(size + size_ <= cap_, "can not exceed target capacity");
    std::memcpy(data_ + size_, data, size);
    size_ += append ? size : 0;
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
MmapChunkTarget::write(const void* data, size_t size, bool append) {
    auto n = file_.Write(data, size);
    AssertInfo(n != -1, "failed to write data to file");
    size_ += append ? size : 0;
}

void
MmapChunkTarget::skip(size_t size) {
    file_.Seek(size, SEEK_CUR);
    size_ += size;
}

void
MmapChunkTarget::seek(size_t offset) {
    file_.Seek(offset_ + offset, SEEK_SET);
}

std::pair<char*, size_t>
MmapChunkTarget::get() {
    auto m = mmap(
        nullptr, size_, PROT_READ, MAP_SHARED, file_.Descriptor(), offset_);
    return {(char*)m, size_};
}

size_t
MmapChunkTarget::tell() {
    return size_;
}
}  // namespace milvus