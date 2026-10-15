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

#include <memory>
#include <variant>
#include <vector>
#include "storage/AsyncIndexEntryReader.h"
#include "storage/IndexEntryReader.h"
#include "storage/FileManager.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index {

/** @brief Storage namespace for a single packed artifact. */
struct PackedIndexStorageConfig {
    storage::ArtifactStorageNamespace storage_namespace{
        storage::ArtifactStorageNamespace::Index};
};

/** @brief Physical layout and storage namespace for legacy artifact files. */
struct LegacyIndexStorageConfig {
    storage::V1SourceLayout layout{storage::V1SourceLayout::MemoryEntries};
    storage::ArtifactStorageNamespace storage_namespace{
        storage::ArtifactStorageNamespace::Index};
};

/**
 * @brief Unopened artifact paths with their storage context and storage
 * configuration.
 * @note Packed input requires exactly one path. The context's pinned async mode
 * selects transport; an unset value falls back to the current storage setting.
 */
struct IndexFiles {
    storage::FileManagerContext context;
    std::vector<std::string> paths;
    std::variant<PackedIndexStorageConfig, LegacyIndexStorageConfig>
        storage_config;
};

/**
 * @brief Shared legacy logical-entry source and its fixed transport mode.
 * @note The source must be non-null. Operations sharing it must be sequential;
 * a borrowed-source adapter requires the original owner to outlive those calls.
 */
struct LegacyIndexSource {
    std::shared_ptr<storage::FileSource> source;
    bool use_async{false};
};

/**
 * @brief Packed artifact source with an already-parsed entry directory and
 * metadata.
 *
 * The reader alternative fixes the transport mode. Directory and metadata are
 * properties of this source, not capabilities of every index loader.
 * @pre The selected reader is non-null and is not used concurrently.
 */
struct PackedIndexSource {
    std::variant<std::shared_ptr<storage::IndexEntryReader>,
                 std::shared_ptr<storage::AsyncIndexEntryReader>>
        reader;

    /**
     * @return Entry locations owned by the retained reader. No I/O is issued.
     */
    const storage::IndexEntryDirectory&
    Directory() const {
        return std::visit(
            [](const auto& value) -> const storage::IndexEntryDirectory& {
                return value->Directory();
            },
            reader);
    }

    /** @return Parsed index properties owned by the retained reader. */
    const nlohmann::json&
    Metadata() const {
        return std::visit(
            [](const auto& value) -> const nlohmann::json& {
                return value->IndexMeta();
            },
            reader);
    }
};

/** @brief Opened storage source selected by format, before family decoding. */
using OpenedIndexSource = std::variant<LegacyIndexSource, PackedIndexSource>;

/**
 * @brief Source description and fixed options for one complete index load.
 *
 * Existing sources avoid another remote open. options.op_ctx is borrowed for
 * the complete top-level load. The concrete loader retains fixed options with
 * that pointer cleared and receives the context separately for each Load.
 */
struct IndexLoadRequest {
    std::variant<IndexFiles, OpenedIndexSource> source;
    storage::LoadOptions options;
};

}  // namespace milvus::index
