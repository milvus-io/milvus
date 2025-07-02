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
#include "File.h"

const uint32_t SYS_PAGE_SIZE = sysconf(_SC_PAGE_SIZE);
namespace milvus {
void
MemChunkTarget::write(const void* data, size_t size) {
    AssertInfo(size + size_ <= cap_, "can not exceed target capacity");
    std::memcpy(data_ + size_, data, size);
    size_ += size;
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
    file_writer_->Finish();
}

void
MmapChunkTarget::write(const void* data, size_t size) {
    file_writer_->Write(data, size);
    size_ += size;
}

std::pair<char*, size_t>
MmapChunkTarget::get() {
    flush();

    auto file = File::Open(file_path_, O_RDWR);
    auto m = mmap(nullptr, size_, PROT_READ, MAP_SHARED, file.Descriptor(), 0);
    AssertInfo(m != MAP_FAILED,
               "failed to map: {}, map_size={}",
               strerror(errno),
               size_);
    return {static_cast<char*>(m), size_};
}

size_t
MmapChunkTarget::tell() {
    return size_;
}
}  // namespace milvus