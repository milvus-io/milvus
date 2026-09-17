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

#include "index/scalar/json/JsonFlatIndexArtifact.h"

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
        directory, storage::LocalEntrySelection::NonDirectories, "JSON flat");
    if (files.empty()) {
        ThrowInfo(DataFormatBroken, "JSON flat artifact has no engine files");
    }
    for (const auto& file : files) {
        if (file.filename().string() == INDEX_NULL_OFFSET) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat engine file conflicts with null sidecar {}",
                      INDEX_NULL_OFFSET);
        }
    }
    return files;
}

void
ValidateMaterializedFiles(const std::vector<std::filesystem::path>& files) {
    for (const auto& file : files) {
        std::error_code error;
        const auto exists = std::filesystem::is_regular_file(file, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect JSON flat engine file {}: {}",
                      file.string(),
                      error.message());
        }
        if (!exists) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat artifact is missing engine file {}",
                      file.filename().string());
        }
    }
}

}  // namespace

std::shared_ptr<storage::LocalDirectory>
CreateJsonFlatIndexDirectory(const std::string& parent) {
    std::error_code error;
    auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                               : std::filesystem::path(parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate JSON flat temporary directory: {}",
                  error.message());
    }
    return storage::LocalDirectory::CreateOwned(
        root.string(), "json_flat_XXXXXX", "JSON flat");
}

JsonFlatIndexArtifact::JsonFlatIndexArtifact(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::vector<size_t> null_offsets)
    : state_(ArtifactState{std::move(directory), std::move(null_offsets)}) {
    AssertInfo(state_.directory != nullptr,
               "JSON flat artifact requires an owned directory");
}

JsonFlatIndexArtifact::~JsonFlatIndexArtifact() = default;

void
JsonFlatIndexArtifact::Serialize(storage::FileSink& sink) const {
    const auto& directory = state_.directory;
    const auto& null_offsets = state_.null_offsets;
    AssertInfo(directory != nullptr,
               "JSON flat artifact state is incomplete");
    auto files = IndexFiles(directory->Path());
    ValidateMaterializedFiles(files);
    if (sink.Gen() == storage::Generation::V3) {
        std::vector<std::string> file_names;
        file_names.reserve(files.size());
        for (const auto& file : files) {
            file_names.push_back(file.filename().string());
        }
        sink.PutMeta(FILE_NAMES, nlohmann::json(file_names));
        sink.PutMeta(HAS_NULL, nlohmann::json(!null_offsets.empty()));
    }

    storage::WriteEntriesFromLocalFiles(sink, files);
    if (!null_offsets.empty()) {
        AssertInfo(null_offsets.size() <=
                       std::numeric_limits<size_t>::max() / sizeof(size_t),
                   "JSON flat null-offset byte size overflows size_t");
        sink.WriteEntry(INDEX_NULL_OFFSET,
                        null_offsets.data(),
                        null_offsets.size() * sizeof(size_t));
    }
}

}  // namespace milvus::index
