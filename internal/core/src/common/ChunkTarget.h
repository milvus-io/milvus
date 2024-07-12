#pragma once
#include <sys/types.h>
#include <unistd.h>
#include <cstddef>
#include "common/File.h"
namespace milvus {
class ChunkTarget {
 public:
    virtual void
    write(const void* data, size_t size, bool append = true) = 0;

    virtual void
    skip(size_t size) = 0;

    virtual void
    seek(size_t offset) = 0;

    virtual std::pair<char*, size_t>
    get() = 0;

    virtual ~ChunkTarget() = default;

    virtual size_t
    tell() = 0;
};

class MmapChunkTarget : public ChunkTarget {
 public:
    MmapChunkTarget(File& file, size_t offset) : file_(file), offset_(offset) {
    }
    void
    write(const void* data, size_t size, bool append = true) override;

    void
    skip(size_t size) override;

    void
    seek(size_t offset) override;

    std::pair<char*, size_t>
    get() override;

    size_t
    tell() override;

 private:
    File& file_;
    size_t offset_ = 0;
    size_t size_ = 0;
};

class MemChunkTarget : public ChunkTarget {
 public:
    MemChunkTarget(size_t cap) : cap_(cap) {
        data_ = new char[cap];
    }

    void
    write(const void* data, size_t size, bool append = true) override;

    void
    skip(size_t size) override;

    void
    seek(size_t offset) override;

    std::pair<char*, size_t>
    get() override;

    size_t
    tell() override;

 private:
    char* data_;  // no need to delete in destructor, will be deleted by Chunk
    size_t cap_;
    size_t size_ = 0;
};

}  // namespace milvus