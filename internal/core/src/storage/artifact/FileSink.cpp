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

#include "storage/artifact/FileSink.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <set>
#include <utility>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Slice.h"
#include "knowhere/binaryset.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::storage {

namespace {

enum class SinkState {
    Open,
    Finished,
    Failed,
};

void
AssertOpen(SinkState state) {
    AssertInfo(state == SinkState::Open,
               state == SinkState::Finished
                   ? "cannot modify an artifact sink after Finish"
                   : "cannot modify a failed artifact sink");
}

class ScopedFd {
 public:
    explicit ScopedFd(int fd) : fd_(fd) {
    }
    ~ScopedFd() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }
    int
    Get() const {
        return fd_;
    }

    void
    CloseChecked(const std::string& path) {
        const auto fd = std::exchange(fd_, -1);
        if (::close(fd) != 0) {
            const auto error = errno;
            ThrowInfo(FileReadFailed,
                      "failed to close borrowed local artifact file {}: {}",
                      path,
                      std::strerror(error));
        }
    }

 private:
    int fd_;
};

// Named per unit rather than plain BaseName: the sink and its sibling
// translation unit both need this helper, and milvus_storage_artifact is a
// unity-build target, where two file-local BaseName definitions would merge
// into one translation unit and redefine each other.
std::string
SinkBaseName(const std::string& path) {
    return std::filesystem::path(path).filename().string();
}

std::string
RemotePath(const DiskFileManagerImpl& manager,
           ArtifactStoragePath storage_path,
           std::string_view name) {
    const auto prefix = storage_path == ArtifactStoragePath::Index
                            ? manager.GetRemoteIndexObjectPrefix()
                            : manager.GetRemoteTextLogPrefix();
    return prefix + "/" + std::string(name);
}

int64_t
StreamRawFile(DiskFileManagerImpl& manager,
              ArtifactStoragePath storage_path,
              std::string_view name,
              const std::string& local_path) {
    int fd = ::open(local_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        const auto error = errno;
        ThrowInfo(FileOpenFailed,
                  "failed to open borrowed raw artifact file {}: {}",
                  local_path,
                  std::strerror(error));
    }
    ScopedFd scoped_fd(fd);

    struct stat stat_buf;
    if (::fstat(fd, &stat_buf) != 0) {
        const auto error = errno;
        ThrowInfo(FileReadFailed,
                  "failed to stat borrowed raw artifact file {}: {}",
                  local_path,
                  std::strerror(error));
    }
    if (!S_ISREG(stat_buf.st_mode) || stat_buf.st_size < 0) {
        ThrowInfo(FileReadFailed,
                  "borrowed raw artifact path is not a regular file: {}",
                  local_path);
    }
    const auto unsigned_size = static_cast<uintmax_t>(stat_buf.st_size);
    if (unsigned_size >
            static_cast<uintmax_t>(std::numeric_limits<int64_t>::max()) ||
        unsigned_size >
            static_cast<uintmax_t>(std::numeric_limits<size_t>::max())) {
        ThrowInfo(FileReadFailed,
                  "borrowed raw artifact file is too large: {}",
                  local_path);
    }
    const auto file_size = static_cast<size_t>(unsigned_size);

    auto output = manager.OpenOutputStream(
        std::string(name), storage_path == ArtifactStoragePath::Index);
    if (output == nullptr) {
        ThrowInfo(FileCreateFailed,
                  "failed to open a raw artifact output stream for {}",
                  name);
    }

    const auto buffer_size =
        std::min(file_size, static_cast<size_t>(DEFAULT_INDEX_FILE_SLICE_SIZE));
    std::vector<uint8_t> buffer(buffer_size);
    size_t remaining = file_size;
    while (remaining != 0) {
        const auto requested = std::min(remaining, buffer.size());
        ssize_t bytes_read;
        do {
            bytes_read = ::read(fd, buffer.data(), requested);
        } while (bytes_read < 0 && errno == EINTR);
        if (bytes_read < 0) {
            const auto error = errno;
            ThrowInfo(FileReadFailed,
                      "failed to read borrowed raw artifact file {}: {}",
                      local_path,
                      std::strerror(error));
        }
        if (bytes_read == 0) {
            ThrowInfo(FileReadFailed,
                      "unexpected end of borrowed raw artifact file {}",
                      local_path);
        }
        const auto chunk_size = static_cast<size_t>(bytes_read);
        const auto bytes_written = output->Write(buffer.data(), chunk_size);
        if (bytes_written != chunk_size) {
            ThrowInfo(FileWriteFailed,
                      "short write for raw artifact entry {}: wrote {} of {} "
                      "bytes",
                      name,
                      bytes_written,
                      chunk_size);
        }
        remaining -= chunk_size;
    }

    scoped_fd.CloseChecked(local_path);
    output->Close();
    return static_cast<int64_t>(file_size);
}

void
AppendBuffer(knowhere::BinarySet& binary_set,
             std::string_view name,
             const void* data,
             size_t size) {
    AssertInfo(!binary_set.Contains(std::string(name)),
               "duplicate artifact entry: {}",
               name);
    AssertInfo(size <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
               "artifact entry {} exceeds BinarySet size domain",
               name);
    auto bytes = std::shared_ptr<uint8_t[]>(new uint8_t[size]);
    if (size != 0) {
        AssertInfo(data != nullptr, "null data for non-empty entry {}", name);
        std::memcpy(bytes.get(), data, size);
    }
    binary_set.Append(std::string(name), std::move(bytes), size);
}

ArtifactStats
StatsFromRemoteMaps(int64_t memory_size,
                    ArtifactStoragePath storage_path,
                    const std::string& text_log_prefix,
                    const std::map<std::string, int64_t>& disk_files,
                    const std::map<std::string, int64_t>& memory_files = {}) {
    auto location = [&](const std::string& path) {
        if (storage_path == ArtifactStoragePath::Index) {
            return path;
        }
        const auto prefix = text_log_prefix + "/";
        return path.size() > prefix.size() &&
                       path.compare(0, prefix.size(), prefix) == 0
                   ? path.substr(prefix.size())
                   : path;
    };
    std::vector<SerializedFileInfo> files;
    files.reserve(disk_files.size() + memory_files.size());
    for (const auto& [path, size] : disk_files) {
        files.emplace_back(location(path), size);
    }
    for (const auto& [path, size] : memory_files) {
        files.emplace_back(location(path), size);
    }
    return ArtifactStats(memory_size, std::move(files));
}

bool
AddMemoryFiles(MemFileManagerImpl& manager,
               ArtifactStoragePath storage_path,
               knowhere::BinarySet& entries,
               bool already_disassembled = false) {
    if (entries.binary_map_.empty()) {
        return true;
    }
    if (!already_disassembled) {
        milvus::Disassemble(entries);
    }
    return storage_path == ArtifactStoragePath::TextLog
               ? manager.AddTextLog(entries)
               : manager.AddFile(entries);
}

}  // namespace

class V1DiskSink::Impl {
 public:
    Impl(const FileManagerContext& context, ArtifactStoragePath storage_path)
        : disk_manager(std::make_shared<DiskFileManagerImpl>(context)),
          memory_manager(std::make_shared<MemFileManagerImpl>(context)),
          storage_path(storage_path) {
    }

    std::string
    StagingDirectory() const {
        return storage_path == ArtifactStoragePath::TextLog
                   ? disk_manager->GetLocalTextIndexPrefix()
                   : disk_manager->GetLocalIndexObjectPrefix();
    }

    bool
    AddDiskFile(const std::string& path) {
        return storage_path == ArtifactStoragePath::TextLog
                   ? disk_manager->AddTextLog(path)
                   : disk_manager->AddFile(path);
    }

    std::string
    RemotePathFor(std::string_view name) const {
        return RemotePath(*disk_manager, storage_path, name);
    }

    std::string
    ReserveEntryName(std::string_view name) {
        auto entry_name = std::string(name);
        AssertInfo(entry_names.insert(entry_name).second,
                   "duplicate artifact entry: {}",
                   name);
        return entry_name;
    }

    void
    AssertRemotePathAvailable(
        const std::string& remote_path,
        const std::map<std::string, int64_t>& disk_files) const {
        AssertInfo(raw_remote_files.find(remote_path) == raw_remote_files.end(),
                   "duplicate artifact remote path: {}",
                   remote_path);
        AssertInfo(disk_files.find(remote_path) == disk_files.end(),
                   "duplicate artifact remote path: {}",
                   remote_path);
    }

    void
    AssertSlicedPathsAvailable(std::string_view entry_name,
                               uintmax_t file_size) const {
        const auto slice_size = FILE_SLICE_SIZE.load();
        AssertInfo(slice_size > 0, "artifact slice size must be positive");
        const auto disk_files = disk_manager->GetRemotePathsToFileSize();
        uintmax_t offset = 0;
        size_t slice = 0;
        while (offset < file_size) {
            AssertRemotePathAvailable(RemotePathFor(milvus::GenSlicedFileName(
                                          std::string(entry_name), slice)),
                                      disk_files);
            offset += std::min<uintmax_t>(file_size - offset,
                                          static_cast<uintmax_t>(slice_size));
            ++slice;
        }
    }

    void
    PrepareMemoryEntries() {
        milvus::Disassemble(memory_entries);
        const auto disk_files = disk_manager->GetRemotePathsToFileSize();
        for (const auto& entry : memory_entries.binary_map_) {
            const auto remote_path = RemotePathFor(entry.first);
            AssertInfo(
                raw_remote_files.find(remote_path) == raw_remote_files.end(),
                "duplicate artifact remote path: {}",
                remote_path);
            AssertInfo(disk_files.find(remote_path) == disk_files.end(),
                       "duplicate artifact remote path: {}",
                       remote_path);
        }
    }

    int64_t
    TotalSerializedSize() const {
        const auto disk_size = disk_manager->GetAddedTotalFileSize();
        const auto memory_size = memory_manager->GetAddedTotalMemSize();
        AssertInfo(raw_size >= 0, "negative artifact serialized size");
        const auto max_size =
            static_cast<uintmax_t>(std::numeric_limits<int64_t>::max());
        const auto disk_bytes = static_cast<uintmax_t>(disk_size);
        const auto raw_bytes = static_cast<uintmax_t>(raw_size);
        AssertInfo(disk_bytes <= max_size && raw_bytes <= max_size - disk_bytes,
                   "artifact serialized size overflow");
        const auto disk_and_raw = disk_bytes + raw_bytes;
        const auto memory_bytes = static_cast<uintmax_t>(memory_size);
        AssertInfo(memory_bytes <= max_size - disk_and_raw,
                   "artifact serialized size overflow");
        const auto total = disk_and_raw + memory_bytes;
        return static_cast<int64_t>(total);
    }

    std::map<std::string, int64_t>
    PublishedDiskFiles() const {
        auto files = disk_manager->GetRemotePathsToFileSize();
        for (const auto& [path, size] : raw_remote_files) {
            if (storage_path == ArtifactStoragePath::Index) {
                // Consume exactly the manager record installed by this raw
                // entry's AddFileMeta call. Other path collisions were
                // rejected before their upload.
                const auto registered = files.find(path);
                AssertInfo(
                    registered != files.end() && registered->second == size,
                    "invalid raw artifact registration: {}",
                    path);
                files.erase(registered);
            }
            const auto inserted = files.emplace(path, size).second;
            AssertInfo(inserted, "duplicate artifact remote path: {}", path);
        }
        const auto memory_files = memory_manager->GetRemotePathsToFileSize();
        for (const auto& file : memory_files) {
            AssertInfo(files.find(file.first) == files.end(),
                       "duplicate artifact remote path: {}",
                       file.first);
        }
        return files;
    }

    std::shared_ptr<DiskFileManagerImpl> disk_manager;
    std::shared_ptr<MemFileManagerImpl> memory_manager;
    ArtifactStoragePath storage_path;
    knowhere::BinarySet memory_entries;
    std::set<std::string> entry_names;
    std::map<std::string, int64_t> raw_remote_files;
    int64_t raw_size{0};
    SinkState state{SinkState::Open};
    bool released{false};
};

V1DiskSink::V1DiskSink(const FileManagerContext& context,
                       ArtifactStoragePath storage_path)
    : impl_(std::make_unique<Impl>(context, storage_path)) {
}

V1DiskSink::~V1DiskSink() = default;

Generation
V1DiskSink::Gen() const {
    return Generation::V1V2;
}

void
V1DiskSink::WriteEntry(std::string_view name, const void* data, size_t size) {
    AssertOpen(impl_->state);
    // Legacy file-shaped indexes uploaded small sidecars through
    // MemFileManager and their engine files through DiskFileManager.
    try {
        impl_->ReserveEntryName(name);
        AppendBuffer(impl_->memory_entries, name, data, size);
    } catch (...) {
        impl_->state = SinkState::Failed;
        throw;
    }
}

void
V1DiskSink::WriteEntryFromLocalFile(std::string_view name,
                                    const std::string& local_path) {
    AssertOpen(impl_->state);
    try {
        const auto entry_name = impl_->ReserveEntryName(name);
        AssertInfo(SinkBaseName(entry_name) == entry_name,
                   "local-file artifact entry must be a basename: {}",
                   name);

        // DiskFileManager removes its generation-specific local prefix in its
        // destructor. Always upload a sink-owned copy from that prefix so the
        // caller's borrowed source can never be swept by manager cleanup.
        const auto directory =
            std::filesystem::path(impl_->StagingDirectory()) / "artifact_sink";
        std::error_code error;
        std::filesystem::create_directories(directory, error);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to create artifact staging directory {}: {}",
                      directory.string(),
                      error.message());
        }
        const auto upload_path = (directory / entry_name).string();
        std::filesystem::copy_file(local_path,
                                   upload_path,
                                   std::filesystem::copy_options::none,
                                   error);
        if (error) {
            ThrowInfo(FileWriteFailed,
                      "failed to copy borrowed artifact entry {} to staging "
                      "{}: {}",
                      local_path,
                      upload_path,
                      error.message());
        }
        const auto staged_size = std::filesystem::file_size(upload_path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to stat staged artifact entry {}: {}",
                      upload_path,
                      error.message());
        }
        impl_->AssertSlicedPathsAvailable(entry_name, staged_size);
        if (!impl_->AddDiskFile(upload_path)) {
            ThrowInfo(FileWriteFailed,
                      "failed to upload local artifact entry {}",
                      name);
        }
    } catch (...) {
        impl_->state = SinkState::Failed;
        throw;
    }
}

void
V1DiskSink::WriteRawEntryFromLocalFile(std::string_view name,
                                       const std::string& local_path) {
    AssertOpen(impl_->state);
    try {
        const auto entry_name = impl_->ReserveEntryName(name);
        AssertInfo(
            !entry_name.empty() && SinkBaseName(entry_name) == entry_name,
            "raw artifact entry must be a non-empty basename: {}",
            name);
        const auto remote_path = impl_->RemotePathFor(entry_name);
        const auto disk_files = impl_->disk_manager->GetRemotePathsToFileSize();
        impl_->AssertRemotePathAvailable(remote_path, disk_files);

        const auto size = StreamRawFile(
            *impl_->disk_manager, impl_->storage_path, entry_name, local_path);
        AssertInfo(
            size <= std::numeric_limits<int64_t>::max() - impl_->raw_size,
            "artifact serialized size overflow");
        if (impl_->storage_path == ArtifactStoragePath::Index &&
            !impl_->disk_manager->AddFileMeta(
                FileMeta{entry_name, static_cast<size_t>(size)})) {
            ThrowInfo(FileWriteFailed,
                      "failed to register raw artifact entry {}",
                      name);
        }
        const auto inserted =
            impl_->raw_remote_files.emplace(remote_path, size).second;
        AssertInfo(inserted, "duplicate artifact remote path: {}", remote_path);
        impl_->raw_size += size;
    } catch (...) {
        impl_->state = SinkState::Failed;
        throw;
    }
}

ArtifactStats
V1DiskSink::Finish() {
    AssertOpen(impl_->state);
    try {
        impl_->PrepareMemoryEntries();
        if (!AddMemoryFiles(*impl_->memory_manager,
                            impl_->storage_path,
                            impl_->memory_entries,
                            /*already_disassembled=*/true)) {
            ThrowInfo(FileWriteFailed,
                      "failed to upload V1/V2 artifact sidecars");
        }
        const auto disk_files = impl_->PublishedDiskFiles();
        auto stats = StatsFromRemoteMaps(
            impl_->TotalSerializedSize(),
            impl_->storage_path,
            impl_->disk_manager->GetRemoteTextLogPrefix(),
            disk_files,
            impl_->memory_manager->GetRemotePathsToFileSize());
        impl_->state = SinkState::Finished;
        return stats;
    } catch (...) {
        impl_->state = SinkState::Failed;
        throw;
    }
}

void
V1DiskSink::ReleaseLocalStaging() {
    if (impl_->released) {
        return;
    }
    if (impl_->storage_path == ArtifactStoragePath::TextLog) {
        impl_->disk_manager->RemoveTextLogFiles();
    } else {
        impl_->disk_manager->RemoveIndexFiles();
    }
    impl_->released = true;
}

}  // namespace milvus::storage
