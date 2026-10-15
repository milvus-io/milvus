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
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "filemanager/FileManager.h"
#include "index/vector/VectorDiskArtifactFile.h"
#include "storage/artifact/LocalDirectory.h"

namespace arrow {
class Buffer;
}

namespace milvus::index {

// Adapts engine file operations to local files and buffered output. The artifact
// takes completed buffers for publication; the manager never invokes the engine
// or uploads data.
class VectorDiskBuildFileManager final
    : public milvus::FileManager,
      public std::enable_shared_from_this<VectorDiskBuildFileManager> {
 public:
    explicit VectorDiskBuildFileManager(
        std::shared_ptr<storage::LocalDirectory> local_files);
    ~VectorDiskBuildFileManager() override = default;

    // The builder supplies format-specific input preparation for LoadFile and
    // clears it before returning. Tensor consumers never invoke this loader.
    void
    SetFileLoader(std::function<void(const std::string&)> loader);

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
    OpenOutputStream(const std::string& filename,
                     size_t file_size = 0) override;

    const std::string&
    Directory() const;
    std::string
    IndexPrefix() const;

    void
    RegisterOwnedFile(const std::string& filename,
                      VectorDiskFileTransport transport =
                          VectorDiskFileTransport::LegacySliced);
    std::vector<VectorDiskArtifactFile>
    LocalFiles() const;
    std::map<std::string, std::shared_ptr<arrow::Buffer>>
    TakeOutput();
    void
    ResetOutput();
    void
    RethrowFirstFailure() const;

    // FileManager's bool/optional API and some knowhere backends collapse an
    // exception into a status. Retain the original typed failure for the
    // builder or artifact to rethrow after the engine returns.
    void
    RecordFailure(std::exception_ptr failure) noexcept;

 private:
    class BufferedOutput;

    std::shared_ptr<storage::LocalDirectory> local_files_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, VectorDiskArtifactFile> files_;
    std::exception_ptr first_failure_;
    std::function<void(const std::string&)> file_loader_;
    std::map<std::string, std::shared_ptr<BufferedOutput>> output_streams_;
};

}  // namespace milvus::index
