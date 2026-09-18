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

#include "index/scalar/inverted/InvertedIndexArtifact.h"

#include <filesystem>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

std::vector<std::filesystem::path>
IndexFiles(const std::string& directory) {
    auto files = storage::ListLocalFiles(
        directory, storage::LocalEntrySelection::NonDirectories, "inverted");
    return files;
}

}  // namespace

std::shared_ptr<storage::LocalDirectory>
CreateInvertedIndexDirectory(const std::string& parent) {
    std::error_code error;
    auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                               : std::filesystem::path(parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate temporary directory for inverted index: "
                  "{}",
                  error.message());
    }
    return storage::LocalDirectory::CreateOwned(
        root.string(), "inverted_XXXXXX", "inverted");
}

InvertedIndexArtifact::InvertedIndexArtifact(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::vector<size_t> null_offsets)
    : directory_(std::move(directory)),
      null_offsets_(std::move(null_offsets)) {
    AssertInfo(directory_ != nullptr,
               "inverted artifact requires an owned directory");
}

InvertedIndexArtifact::~InvertedIndexArtifact() = default;

void
InvertedIndexArtifact::Serialize(storage::FileSink& sink) const {
    const auto files = IndexFiles(directory_->Path());
    if (sink.Gen() == storage::Generation::V3) {
        std::vector<std::string> file_names;
        file_names.reserve(files.size());
        for (const auto& file : files) {
            file_names.push_back(file.filename().string());
        }
        sink.PutMeta(FILE_NAMES, nlohmann::json(file_names));
        sink.PutMeta(HAS_NULL, nlohmann::json(!null_offsets_.empty()));
    }

    storage::WriteEntriesFromLocalFiles(sink, files);
    if (!null_offsets_.empty()) {
        sink.WriteEntry(INDEX_NULL_OFFSET,
                        null_offsets_.data(),
                        null_offsets_.size() * sizeof(size_t));
    }
}

}  // namespace milvus::index
