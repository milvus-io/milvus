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
#include <string>
#include <utility>
#include <vector>

#include <folly/ScopeGuard.h>

#include "common/EasyAssert.h"
#include "index/Utils.h"
#include "knowhere/binaryset.h"

namespace milvus::index {
VectorDiskArtifact::VectorDiskArtifact(
    std::unique_ptr<KnowhereEngine> engine,
    std::shared_ptr<VectorDiskBuildFileManager> file_manager)
    : file_manager_(std::move(file_manager)), engine_(std::move(engine)) {
    AssertInfo(file_manager_ != nullptr,
               "vector disk artifact has no file manager");
    AssertInfo(engine_ || !file_manager_->LocalFiles().empty(),
               "vector disk artifact has no completed files");
}

void
VectorDiskArtifact::Serialize(storage::FileSink& sink) const {
    if (sink.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "disk vector artifacts have no V3 persisted format");
    }

    std::vector<VectorDiskArtifactFile> files;
    if (engine_) {
        file_manager_->ResetOutput();
        auto output_guard =
            folly::makeGuard([this] { file_manager_->ResetOutput(); });
        knowhere::BinarySet entries;
        const auto status = engine_->native_index.Serialize(entries);
        file_manager_->RethrowFirstFailure();
        if (status != knowhere::Status::success) {
            ThrowInfo(KnowhereStatusToErrorCode(status),
                      "failed to serialize disk vector index: {}",
                      knowhere::Status2String(status));
        }
        AssertInfo(entries.binary_map_.empty(),
                   "disk vector index unexpectedly returned in-memory "
                   "serialized entries");
        auto output = file_manager_->TakeOutput();
        files = file_manager_->LocalFiles();
        AssertInfo(!output.empty() || !files.empty(),
                   "vector disk artifact has no completed output");
        // Transfer complete buffers to the sink. A failed attempt discards its
        // output; the retained engine can serialize again into a fresh sink.
        for (auto& [path, buffer] : output) {
            sink.WriteRawEntry(std::filesystem::path(path).filename().string(),
                               std::move(buffer));
        }
    } else {
        files = file_manager_->LocalFiles();
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
