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

#include "storage/artifact/DiskEngineFileHandle.h"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <mutex>
#include <optional>
#include <unordered_set>
#include <utility>

#include "common/EasyAssert.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"

namespace milvus::storage {
namespace {

class RestrictedDiskEngineFileManager;

class RecordingInputStream final : public milvus::InputStream {
 public:
    RecordingInputStream(
        std::shared_ptr<milvus::InputStream> input,
        std::shared_ptr<RestrictedDiskEngineFileManager> manager)
        : manager_(std::move(manager)), input_(std::move(input)) {
        AssertInfo(input_ != nullptr,
                   "disk vector load manager received a null input stream");
        AssertInfo(manager_ != nullptr,
                   "disk vector load manager is missing from input stream");
    }

    size_t
    Size() const override;

    bool
    Seek(int64_t offset) override;

    size_t
    Tell() const override;

    bool
    Eof() const override;

    size_t
    Read(void* data, size_t size) override;

    size_t
    ReadAt(void* data, size_t offset, size_t size) override;

    size_t
    Read(int fd, size_t size) override;

 private:
    template <typename F>
    auto
    Invoke(F&& function) const -> decltype(std::forward<F>(function)());

    // The manager owns the delegate/generation and therefore precedes the
    // stream so the underlying stream is destroyed first.
    std::shared_ptr<RestrictedDiskEngineFileManager> manager_;
    std::shared_ptr<milvus::InputStream> input_;
};

class RestrictedDiskEngineFileManager final
    : public milvus::FileManager,
      public std::enable_shared_from_this<RestrictedDiskEngineFileManager> {
 public:
    RestrictedDiskEngineFileManager(
        const FileManagerContext& context,
        const std::vector<std::string>& remote_paths,
        const std::vector<std::string>& engine_entry_names)
        : delegate_(std::make_shared<DiskFileManagerImpl>(context)) {
        std::unordered_set<std::string> names;
        names.reserve(engine_entry_names.size());
        for (const auto& name : engine_entry_names) {
            AssertInfo(!name.empty() && names.insert(name).second,
                       "disk vector engine inventory has an empty or "
                       "duplicate entry name");
        }
        allowed_remote_paths_.reserve(engine_entry_names.size());
        for (const auto& remote_path : remote_paths) {
            const auto name = delegate_->GetFileName(remote_path);
            if (names.find(name) != names.end()) {
                allowed_remote_paths_.insert(
                    remote_path.find('/') == std::string::npos
                        ? delegate_->GetRemoteIndexObjectPrefix() + "/" +
                              remote_path
                        : remote_path);
            }
        }
    }

    bool
    LoadFile(const std::string& filename) override {
        try {
            return delegate_->LoadFile(filename);
        } catch (...) {
            RecordFailure(std::current_exception());
            return false;
        }
    }

    bool
    AddFile(const std::string& filename) override {
        try {
            ThrowInfo(FileWriteFailed,
                      "stream-loading disk vector index attempted to add file "
                      "{}",
                      filename);
        } catch (...) {
            RecordFailure(std::current_exception());
            return false;
        }
    }

    bool
    AddFileMeta(const milvus::FileMeta& file_meta) override {
        try {
            ThrowInfo(FileWriteFailed,
                      "stream-loading disk vector index attempted to add file "
                      "metadata {}",
                      file_meta.file_path);
        } catch (...) {
            RecordFailure(std::current_exception());
            return false;
        }
    }

    std::optional<bool>
    IsExisted(const std::string& filename) override {
        try {
            return delegate_->IsExisted(filename);
        } catch (...) {
            RecordFailure(std::current_exception());
            return std::nullopt;
        }
    }

    bool
    RemoveFile(const std::string& filename) override {
        try {
            ThrowInfo(
                FileWriteFailed,
                "stream-loading disk vector index attempted to remove file "
                "{}",
                filename);
        } catch (...) {
            RecordFailure(std::current_exception());
            return false;
        }
    }

    std::shared_ptr<milvus::InputStream>
    OpenInputStream(const std::string& filename) override {
        try {
            ValidateRawPath(filename);
            auto input = delegate_->OpenInputStream(filename);
            return std::make_shared<RecordingInputStream>(
                std::move(input), shared_from_this());
        } catch (...) {
            RecordFailure(std::current_exception());
            throw;
        }
    }

    std::shared_ptr<milvus::OutputStream>
    OpenOutputStream(const std::string& filename) override {
        try {
            ThrowInfo(FileWriteFailed,
                      "stream-loading disk vector index attempted to open "
                      "output {}",
                      filename);
        } catch (...) {
            RecordFailure(std::current_exception());
            throw;
        }
    }

    std::string
    LocalIndexPrefix() const {
        return delegate_->GetLocalIndexObjectPrefix();
    }

    void
    RethrowFirstFailure() const {
        std::exception_ptr failure;
        {
            std::lock_guard lock(mutex_);
            failure = first_failure_;
        }
        if (failure != nullptr) {
            std::rethrow_exception(failure);
        }
    }

    void
    RecordFailure(std::exception_ptr failure) noexcept {
        try {
            std::lock_guard lock(mutex_);
            if (first_failure_ == nullptr) {
                first_failure_ = std::move(failure);
            }
        } catch (...) {
        }
    }

 private:
    std::string
    ResolveRemotePath(const std::string& filename) const {
        const auto name = delegate_->GetFileName(filename);
        AssertInfo(!name.empty(),
                   "disk vector engine requested an empty file name");
        return delegate_->GetRemoteIndexObjectPrefix() + "/" + name;
    }

    void
    ValidateRawPath(const std::string& filename) const {
        const auto remote_path = ResolveRemotePath(filename);
        if (allowed_remote_paths_.find(remote_path) ==
            allowed_remote_paths_.end()) {
            ThrowInfo(
                DataFormatBroken,
                "disk vector engine requested unadvertised raw object {}",
                remote_path);
        }
    }

    std::shared_ptr<DiskFileManagerImpl> delegate_;
    std::unordered_set<std::string> allowed_remote_paths_;
    mutable std::mutex mutex_;
    std::exception_ptr first_failure_;
};

template <typename F>
auto
RecordingInputStream::Invoke(F&& function) const
    -> decltype(std::forward<F>(function)()) {
    try {
        return std::forward<F>(function)();
    } catch (...) {
        manager_->RecordFailure(std::current_exception());
        throw;
    }
}

size_t
RecordingInputStream::Size() const {
    return Invoke([this] { return input_->Size(); });
}

bool
RecordingInputStream::Seek(int64_t offset) {
    return Invoke([this, offset] { return input_->Seek(offset); });
}

size_t
RecordingInputStream::Tell() const {
    return Invoke([this] { return input_->Tell(); });
}

bool
RecordingInputStream::Eof() const {
    return Invoke([this] { return input_->Eof(); });
}

size_t
RecordingInputStream::Read(void* data, size_t size) {
    return Invoke([this, data, size] { return input_->Read(data, size); });
}

size_t
RecordingInputStream::ReadAt(void* data, size_t offset, size_t size) {
    return Invoke([this, data, offset, size] {
        return input_->ReadAt(data, offset, size);
    });
}

size_t
RecordingInputStream::Read(int fd, size_t size) {
    return Invoke([this, fd, size] { return input_->Read(fd, size); });
}

}  // namespace

class DiskEngineFileHandle::Impl {
 public:
    Impl(const FileManagerContext& context,
         DiskEngineFileMode mode,
         const std::vector<std::string>& remote_paths,
         const std::vector<std::string>& engine_entry_names) {
        if (mode == DiskEngineFileMode::LocalFiles) {
            auto local = std::make_shared<DiskFileManagerImpl>(context);
            local_prefix_ = local->GetLocalIndexObjectPrefix();
            manager_ = std::move(local);
            return;
        }

        restricted_ = std::make_shared<RestrictedDiskEngineFileManager>(
            context, remote_paths, engine_entry_names);
        local_prefix_ = restricted_->LocalIndexPrefix();
        manager_ = restricted_;
    }

    std::shared_ptr<milvus::FileManager> manager_;
    std::shared_ptr<RestrictedDiskEngineFileManager> restricted_;
    std::string local_prefix_;
};

DiskEngineFileHandle::DiskEngineFileHandle(
    const FileManagerContext& context,
    DiskEngineFileMode mode,
    const std::vector<std::string>& remote_paths,
    const std::vector<std::string>& engine_entry_names)
    : impl_(std::make_unique<Impl>(
          context, mode, remote_paths, engine_entry_names)) {
}

DiskEngineFileHandle::~DiskEngineFileHandle() = default;

std::shared_ptr<milvus::FileManager>
DiskEngineFileHandle::Manager() const {
    return impl_->manager_;
}

const std::string&
DiskEngineFileHandle::LocalPrefix() const {
    return impl_->local_prefix_;
}

void
DiskEngineFileHandle::RethrowFirstFailure() const {
    if (impl_->restricted_ != nullptr) {
        impl_->restricted_->RethrowFirstFailure();
    }
}

}  // namespace milvus::storage
