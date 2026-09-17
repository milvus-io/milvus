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

#include "index/scalar/ngram/NgramIndexArtifact.h"

#include <filesystem>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"

namespace milvus::index {
namespace {

constexpr std::string_view kAvgRowSizeEntry = "ngram_avg_row_size";

std::vector<std::filesystem::path>
IndexFiles(const std::string& directory) {
    auto files = storage::ListLocalFiles(
        directory, storage::LocalEntrySelection::RegularFiles, "NGRAM");
    return files;
}

}  // namespace

std::shared_ptr<storage::LocalDirectory>
CreateNgramIndexDirectory(const std::string& parent) {
    std::error_code error;
    auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                               : std::filesystem::path(parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate temporary directory for NGRAM: {}",
                  error.message());
    }
    return storage::LocalDirectory::CreateOwned(
        root.string(), "ngram_XXXXXX", "NGRAM");
}

NgramIndexArtifact::NgramIndexArtifact(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::vector<size_t> null_offsets,
    size_t avg_row_size)
    : directory_(std::move(directory)),
      null_offsets_(std::move(null_offsets)),
      avg_row_size_(avg_row_size) {
    AssertInfo(directory_ != nullptr,
               "NGRAM artifact requires an owned directory");
}

NgramIndexArtifact::~NgramIndexArtifact() = default;

void
NgramIndexArtifact::Serialize(storage::FileSink& sink) const {
    const auto files = IndexFiles(directory_->Path());
    AssertInfo(!files.empty(), "NGRAM artifact has no Tantivy engine files");

    if (sink.Gen() == storage::Generation::V3) {
        std::vector<std::string> file_names;
        file_names.reserve(files.size());
        for (const auto& file : files) {
            file_names.push_back(file.filename().string());
        }
        // Exact inherited V3 Tantivy metadata. min/max gram, type, path and
        // count remain runtime-only just as in the baseline format.
        sink.PutMeta(FILE_NAMES, nlohmann::json(file_names));
        sink.PutMeta(HAS_NULL, nlohmann::json(!null_offsets_.empty()));
    }

    storage::WriteEntriesFromLocalFiles(sink, files);
    if (!null_offsets_.empty()) {
        if (null_offsets_.size() >
            std::numeric_limits<size_t>::max() / sizeof(size_t)) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM null-offset byte size overflows");
        }
        sink.WriteEntry(INDEX_NULL_OFFSET,
                        null_offsets_.data(),
                        null_offsets_.size() * sizeof(size_t));
    }
    // The baseline V1/V2 upload never emitted this sidecar and its loader used
    // the 5000-byte default. V3 has always written one native size_t.
    if (sink.Gen() == storage::Generation::V3) {
        sink.WriteEntry(
            kAvgRowSizeEntry, &avg_row_size_, sizeof(avg_row_size_));
    }
}

}  // namespace milvus::index
