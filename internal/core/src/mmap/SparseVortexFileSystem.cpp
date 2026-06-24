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
#include "mmap/SparseVortexFileSystem.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fcntl.h>
#ifdef __linux__
#include <linux/falloc.h>
#endif
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <fmt/format.h>

#include "milvus-storage/format/vortex/vortex_types.h"

namespace milvus {

namespace {

#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif

[[noreturn]] inline void
ThrowSystemError(std::string_view action) {
    throw std::runtime_error(
        fmt::format("{} failed: {}", action, std::strerror(errno)));
}

inline int
CreateAnonymousSparseFile(std::string_view name) {
#ifndef SYS_memfd_create
    (void)name;
    errno = ENOSYS;
    return -1;
#else
    auto fd = static_cast<int>(
        ::syscall(SYS_memfd_create, std::string(name).c_str(), MFD_CLOEXEC));
    return fd;
#endif
}

}  // namespace

class SparseVortexMmapFile final
    : public milvus_storage::vortex::VortexRangeFile,
      public std::enable_shared_from_this<SparseVortexMmapFile> {
 public:
    explicit SparseVortexMmapFile(std::string name) {
        fd_ = CreateAnonymousSparseFile(name);
        if (fd_ < 0) {
            ThrowSystemError("create vortex sparse file");
        }
    }

    ~SparseVortexMmapFile() override {
        if (mapping_ != nullptr) {
            ::munmap(mapping_, static_cast<size_t>(mapped_size_));
        }
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    void
    Resize(uint64_t size) override {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (size == size_) {
            return;
        }
        if (size > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
            throw std::runtime_error(fmt::format(
                "vortex sparse file size {} exceeds off_t limit", size));
        }
        if (::ftruncate(fd_, static_cast<off_t>(size)) != 0) {
            ThrowSystemError("resize vortex sparse file");
        }
        size_ = size;
        RemapLocked();
    }

    uint64_t
    Size() const override {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return size_;
    }

    arrow::Status
    WriteAt(const uint64_t& offset,
            const std::shared_ptr<arrow::Buffer>& data) override {
        if (!data) {
            return arrow::Status::Invalid(
                "VortexRangeFile::WriteAt got null buffer");
        }
        const auto length = static_cast<uint64_t>(data->size());
        if (length == 0) {
            return arrow::Status::OK();
        }
        if (offset > std::numeric_limits<uint64_t>::max() - length) {
            return arrow::Status::Invalid(
                "VortexRangeFile::WriteAt range overflows");
        }

        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (offset + length > size_) {
            return arrow::Status::IOError(
                fmt::format("VortexRangeFile::WriteAt out of range, offset={}, "
                            "length={}, size={}",
                            offset,
                            length,
                            size_));
        }
        if (mapping_ == nullptr) {
            return arrow::Status::IOError(
                "VortexRangeFile::WriteAt got empty mapping");
        }

        std::memcpy(mapping_ + offset, data->data(), length);
        return arrow::Status::OK();
    }

    void
    Punch(uint64_t offset, uint64_t length) override {
        if (length == 0) {
            return;
        }
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (offset >= size_) {
            return;
        }
        length = std::min<uint64_t>(length, size_ - offset);
        if (length == 0 || mapping_ == nullptr) {
            return;
        }

#if defined(FALLOC_FL_PUNCH_HOLE) && defined(SYS_fallocate)
        if (offset <=
                static_cast<uint64_t>(std::numeric_limits<off_t>::max()) &&
            length <=
                static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
            if (::syscall(SYS_fallocate,
                          fd_,
                          FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                          static_cast<off_t>(offset),
                          static_cast<off_t>(length)) == 0) {
                return;
            }
        }
#endif

        std::memset(mapping_ + offset, 0, length);
    }

    arrow::Result<int64_t>
    ReadAt(int64_t position, int64_t nbytes, void* out) const override {
        if (position < 0 || nbytes < 0) {
            return arrow::Status::Invalid(
                "VortexRangeFile::ReadAt got negative position or length");
        }
        if (nbytes == 0) {
            return 0;
        }
        if (out == nullptr) {
            return arrow::Status::Invalid(
                "VortexRangeFile::ReadAt got null output");
        }

        std::shared_lock<std::shared_mutex> lock(mutex_);
        const auto pos = static_cast<uint64_t>(position);
        if (pos >= size_) {
            return 0;
        }
        const auto available = size_ - pos;
        const auto read = std::min(static_cast<uint64_t>(nbytes), available);
        if (read == 0) {
            return 0;
        }
        if (mapping_ == nullptr) {
            return arrow::Status::IOError(
                "VortexRangeFile::ReadAt got empty mapping");
        }

        std::memcpy(out, mapping_ + pos, read);
        return static_cast<int64_t>(read);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    ReadAt(int64_t position, int64_t nbytes) const override {
        if (nbytes < 0) {
            return arrow::Status::Invalid(
                "VortexRangeFile::ReadAt got negative length");
        }
        if (position < 0) {
            return arrow::Status::Invalid(
                "VortexRangeFile::ReadAt got negative position");
        }
        if (nbytes == 0) {
            return arrow::Buffer::FromString(std::string());
        }

        std::shared_lock<std::shared_mutex> lock(mutex_);
        const auto pos = static_cast<uint64_t>(position);
        if (pos >= size_) {
            return arrow::Buffer::FromString(std::string());
        }
        const auto available = size_ - pos;
        const auto read = std::min(static_cast<uint64_t>(nbytes), available);
        if (read == 0) {
            return arrow::Buffer::FromString(std::string());
        }
        if (mapping_ == nullptr) {
            return arrow::Status::IOError(
                "VortexRangeFile::ReadAt got empty mapping");
        }
        auto owner = shared_from_this();
        return std::shared_ptr<arrow::Buffer>(
            new arrow::Buffer(mapping_ + pos, static_cast<int64_t>(read)),
            [owner = std::move(owner)](arrow::Buffer* buffer) {
                delete buffer;
            });
    }

 private:
    void
    RemapLocked() {
        if (mapping_ != nullptr) {
            if (::munmap(mapping_, static_cast<size_t>(mapped_size_)) != 0) {
                ThrowSystemError("unmap vortex sparse file");
            }
            mapping_ = nullptr;
            mapped_size_ = 0;
        }
        if (size_ == 0) {
            return;
        }
        if (size_ > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            throw std::runtime_error(fmt::format(
                "vortex sparse file size {} exceeds mmap limit", size_));
        }
        auto* mapped = ::mmap(nullptr,
                              static_cast<size_t>(size_),
                              PROT_READ | PROT_WRITE,
                              MAP_SHARED,
                              fd_,
                              0);
        if (mapped == MAP_FAILED) {
            ThrowSystemError("mmap vortex sparse file");
        }
        mapping_ = static_cast<uint8_t*>(mapped);
        mapped_size_ = size_;
    }

    mutable std::shared_mutex mutex_;
    int fd_ = -1;
    uint8_t* mapping_ = nullptr;
    uint64_t mapped_size_ = 0;
    uint64_t size_ = 0;
};

class SparseVortexInputFile final : public arrow::io::RandomAccessFile {
 public:
    explicit SparseVortexInputFile(
        std::shared_ptr<milvus_storage::vortex::VortexRangeFile> file)
        : file_(std::move(file)) {
    }

    arrow::Status
    Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }

    bool
    closed() const override {
        return closed_;
    }

    arrow::Result<int64_t>
    Tell() const override {
        return position_;
    }

    arrow::Status
    Seek(int64_t position) override {
        if (position < 0) {
            return arrow::Status::Invalid(
                "negative seek in SparseVortexInputFile");
        }
        position_ = position;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t>
    Read(int64_t nbytes, void* out) override {
        ARROW_ASSIGN_OR_RAISE(auto read, ReadAt(position_, nbytes, out));
        position_ += read;
        return read;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(position_, nbytes));
        position_ += buffer->size();
        return buffer;
    }

    arrow::Result<int64_t>
    ReadAt(int64_t position, int64_t nbytes, void* out) override {
        if (closed_) {
            return arrow::Status::IOError("SparseVortexInputFile is closed");
        }
        return file_->ReadAt(position, nbytes, out);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    ReadAt(int64_t position, int64_t nbytes) override {
        if (closed_) {
            return arrow::Status::IOError("SparseVortexInputFile is closed");
        }
        return file_->ReadAt(position, nbytes);
    }

    arrow::Result<int64_t>
    GetSize() override {
        return static_cast<int64_t>(file_->Size());
    }

 private:
    std::shared_ptr<milvus_storage::vortex::VortexRangeFile> file_;
    int64_t position_ = 0;
    bool closed_ = false;
};

class SparseVortexFileSystem final
    : public arrow::fs::FileSystem,
      public milvus_storage::vortex::VortexRangeFileProvider {
 public:
    explicit SparseVortexFileSystem(std::string path)
        : file_(std::make_shared<SparseVortexMmapFile>(path)),
          path_(std::move(path)) {
    }

    std::string
    type_name() const override {
        return "vortex-sparse-mmap";
    }

    bool
    Equals(const arrow::fs::FileSystem& other) const override {
        return this == &other;
    }

    arrow::Result<std::shared_ptr<milvus_storage::vortex::VortexRangeFile>>
    GetVortexRangeFile(const std::string& path) const override {
        ARROW_RETURN_NOT_OK(CheckPath(path));
        return file_;
    }

    arrow::Result<arrow::fs::FileInfo>
    GetFileInfo(const std::string& path) override {
        ARROW_RETURN_NOT_OK(CheckPath(path));
        arrow::fs::FileInfo info(path.empty() ? path_ : path);
        info.set_type(arrow::fs::FileType::File);
        info.set_size(static_cast<int64_t>(file_->Size()));
        return info;
    }

    arrow::Result<std::vector<arrow::fs::FileInfo>>
    GetFileInfo(const arrow::fs::FileSelector& select) override {
        ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(select.base_dir));
        return std::vector<arrow::fs::FileInfo>{std::move(info)};
    }

    arrow::Status
    CreateDir(const std::string&, bool) override {
        return arrow::Status::OK();
    }

    arrow::Status
    DeleteDir(const std::string&) override {
        return arrow::Status::NotImplemented("DeleteDir");
    }

    arrow::Status
    DeleteDirContents(const std::string&, bool) override {
        return arrow::Status::NotImplemented("DeleteDirContents");
    }

    arrow::Status
    DeleteRootDirContents() override {
        return arrow::Status::NotImplemented("DeleteRootDirContents");
    }

    arrow::Status
    DeleteFile(const std::string&) override {
        return arrow::Status::NotImplemented("DeleteFile");
    }

    arrow::Status
    Move(const std::string&, const std::string&) override {
        return arrow::Status::NotImplemented("Move");
    }

    arrow::Status
    CopyFile(const std::string&, const std::string&) override {
        return arrow::Status::NotImplemented("CopyFile");
    }

    arrow::Result<std::shared_ptr<arrow::io::InputStream>>
    OpenInputStream(const std::string& path) override {
        ARROW_RETURN_NOT_OK(CheckPath(path));
        return std::static_pointer_cast<arrow::io::InputStream>(
            std::make_shared<SparseVortexInputFile>(file_));
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        ARROW_RETURN_NOT_OK(CheckPath(path));
        return std::make_shared<SparseVortexInputFile>(file_);
    }

    arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
    OpenOutputStream(
        const std::string&,
        const std::shared_ptr<const arrow::KeyValueMetadata>&) override {
        return arrow::Status::NotImplemented("OpenOutputStream");
    }

    arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
    OpenAppendStream(
        const std::string&,
        const std::shared_ptr<const arrow::KeyValueMetadata>&) override {
        return arrow::Status::NotImplemented("OpenAppendStream");
    }

 private:
    arrow::Status
    CheckPath(std::string_view path) const {
        if (!path.empty() && path != path_) {
            return arrow::Status::Invalid(
                fmt::format("SparseVortexFileSystem path mismatch, got {}, "
                            "expected {}",
                            path,
                            path_));
        }
        return arrow::Status::OK();
    }

    std::shared_ptr<milvus_storage::vortex::VortexRangeFile> file_;
    std::string path_;
};

std::string
MakeSparseVortexPath(std::string_view source_path) {
    return fmt::format("vortex-sparse-{:016x}.vortex",
                       std::hash<std::string_view>{}(source_path));
}

std::shared_ptr<arrow::fs::FileSystem>
MakeSparseVortexFileSystem(std::string path) {
    return std::make_shared<SparseVortexFileSystem>(std::move(path));
}

}  // namespace milvus
