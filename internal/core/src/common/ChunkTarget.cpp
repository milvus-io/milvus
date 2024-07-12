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