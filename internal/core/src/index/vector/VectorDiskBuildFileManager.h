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

#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "filemanager/FileManager.h"
#include "index/vector/VectorDiskArtifactFile.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index {

// Build-only FileManager. It records completed files inside one owned staging
// directory and never publishes or opens remote storage.
class VectorDiskBuildFileManager final
    : public milvus::FileManager,
      public std::enable_shared_from_this<VectorDiskBuildFileManager> {
 public:
    explicit VectorDiskBuildFileManager(
        std::shared_ptr<storage::LocalDirectory> local_files);
    ~VectorDiskBuildFileManager() override = default;

    bool
    LoadFile(const std::string& filename) override;
    bool
    AddFile(const std::string& filename) override;
    bool
    AddFileMeta(const milvus::FileMeta& file_meta) override;
    std::optional<bool>
    IsExisted(const std::string& filename) override;
    bool
    RemoveFile(const std::string& filename) override;
    std::shared_ptr<milvus::InputStream>
    OpenInputStream(const std::string& filename) override;
    std::shared_ptr<milvus::OutputStream>
    OpenOutputStream(const std::string& filename) override;

    const std::string&
    Directory() const;
    std::string
    IndexPrefix() const;

    void
    RegisterOwnedFile(const std::string& filename);
    void
    RegisterOwnedFile(const std::string& filename,
                      VectorDiskFileTransport transport);
    void
    BeginOutput(const std::string& filename);
    void
    CompleteOutput(const std::string& filename, size_t size);
    std::vector<VectorDiskArtifactFile>
    Files() const;
    void
    RethrowFirstFailure() const;

    // FileManager's bool/optional API and some knowhere backends collapse an
    // exception into a status. Streams record at the throw site as well, so
    // the builder/loader can restore the original typed failure afterwards.
    void
    RecordFailure(std::exception_ptr failure) noexcept;

 private:
    void
    AbortOutput(const std::string& filename) noexcept;
    void
    RegisterCompletedFileLocked(const std::string& filename,
                                size_t size,
                                VectorDiskFileTransport transport);

    std::shared_ptr<storage::LocalDirectory> local_files_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, VectorDiskArtifactFile> files_;
    std::unordered_map<std::string, size_t> pending_meta_;
    std::unordered_map<std::string, size_t> open_outputs_;
    std::exception_ptr first_failure_;
};

}  // namespace milvus::index
