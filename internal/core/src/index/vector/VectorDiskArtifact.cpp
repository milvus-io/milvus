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

#include "index/vector/VectorDiskArtifact.h"

#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::index {
VectorDiskArtifact::VectorDiskArtifact(
    std::shared_ptr<storage::LocalDirectory> local_files_owner,
    std::vector<VectorDiskArtifactFile> local_files)
    : local_files_owner_(std::move(local_files_owner)),
      local_files_(std::move(local_files)) {
    AssertInfo(local_files_owner_ != nullptr,
               "vector disk artifact has no staging owner");
    AssertInfo(!local_files_.empty(),
               "vector disk artifact has no completed files");
}

void
VectorDiskArtifact::Serialize(storage::FileSink& sink) const {
    if (sink.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "disk vector artifacts have no V3 persisted format");
    }

    const auto* owner = local_files_owner_.get();
    const auto& files = local_files_;
    AssertInfo(owner != nullptr, "vector disk artifact owner is missing");

    std::set<std::string> names;
    for (const auto& file : files) {
        AssertInfo(file.transport == VectorDiskFileTransport::LegacySliced ||
                       file.transport == VectorDiskFileTransport::RawUnsliced,
                   "vector disk artifact file has an invalid transport");
        std::error_code error;
        if (!owner->Owns(file.path) ||
            !std::filesystem::is_regular_file(file.path, error) || error) {
            ThrowInfo(FileReadFailed,
                      "vector disk artifact file {} is unavailable{}{}",
                      file.path,
                      error ? ": " : "",
                      error ? error.message() : "");
        }
        const auto actual_size = std::filesystem::file_size(file.path, error);
        if (error || actual_size != file.size) {
            ThrowInfo(FileReadFailed,
                      "vector disk artifact file {} size changed from {}{}{}",
                      file.path,
                      file.size,
                      error ? ": " : " to ",
                      error ? error.message() : std::to_string(actual_size));
        }
        const auto name = std::filesystem::path(file.path).filename().string();
        AssertInfo(!name.empty() && names.insert(name).second,
                   "vector disk artifact has a duplicate or empty file name "
                   "{}",
                   name);
    }

    for (const auto& file : files) {
        const auto name = std::filesystem::path(file.path).filename().string();
        switch (file.transport) {
            case VectorDiskFileTransport::LegacySliced:
                sink.WriteEntryFromLocalFile(name, file.path);
                break;
            case VectorDiskFileTransport::RawUnsliced:
                sink.WriteRawEntryFromLocalFile(name, file.path);
                break;
        }
    }
}

}  // namespace milvus::index
