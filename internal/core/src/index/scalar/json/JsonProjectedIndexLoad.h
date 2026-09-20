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

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/IndexLoadPlan.h"
#include "storage/IndexEntryFormat.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index {

/** @brief Whether persisted metadata proves JSON missing-value completeness. */
enum class JsonProjectionCompleteness {
    NotProjected,
    LegacyUnknown,
    Complete,
};

/**
 * @brief Per-load JSON projection metadata and optional non-exist sidecar
 * targets.
 * @note Owns values and staging resources, never a source or OpContext. This is
 * reader-decoration state prepared during Load, not reusable IndexLoader state.
 */
struct JsonProjectedOpenPlan {
    JsonProjectionCompleteness completeness{
        JsonProjectionCompleteness::NotProjected};
    std::string json_path;
    std::optional<JsonCastType> cast_type;
    bool has_non_exist_entry{false};
    std::optional<size_t> declared_non_exist_bytes;
    std::string staging_parent;
    std::shared_ptr<std::vector<size_t>> packed_non_exist_offsets;
    std::shared_ptr<void> packed_directory;
    std::shared_ptr<storage::IndexFileTarget> packed_non_exist_file;
};

/**
 * @brief Replace runtime completeness annotations with evidence from the
 * source.
 * @note Called before DeriveCaps; annotations are runtime-only, never
 * persisted.
 */
Config
AnnotateJsonProjectionCompleteness(Config params, storage::FileSource& source);

/**
 * @brief Annotate completeness from already-parsed packed metadata without I/O.
 */
Config
AnnotateJsonProjectionCompleteness(
    Config params,
    const storage::IndexEntryDirectory& directory,
    const nlohmann::json& metadata);

/**
 * @brief Validate projection metadata and append non-exist sidecar
 * destinations.
 * @param row_count Known row bound; absent means stage the sidecar on disk.
 * @note Does not read payloads. Retain the result through packed finalization.
 */
JsonProjectedOpenPlan
PreparePackedJsonProjectedOpen(std::string_view family,
                               const storage::IndexEntryDirectory& directory,
                               const nlohmann::json& metadata,
                               const storage::LoadOptions& opts,
                               IndexLoadPlan& targets,
                               std::optional<size_t> row_count = std::nullopt);

/**
 * @brief Decorate the reader using populated non-exist targets and its row
 * bound.
 * @pre Planned reads/writes are complete; this may read local staging files.
 */
IIndexReaderBasePtr
FinishPackedJsonProjectedOpen(JsonProjectedOpenPlan plan,
                              IIndexReaderBasePtr inner);

/**
 * @brief Adjust inner capabilities using the verified completeness annotation.
 */
ReaderCaps
DeriveJsonProjectedCaps(std::string_view family,
                        const Config& annotated_params,
                        ReaderCaps inner_caps);

/**
 * @brief Validate JSON parameters and completeness before family payload
 * loading.
 * @note Re-observes source metadata and checks the cold-plan annotation; the
 * returned state does not retain source or the opening context.
 */
JsonProjectedOpenPlan
PrepareJsonProjectedOpen(std::string_view family,
                         storage::FileSource& source,
                         const storage::LoadOptions& opts);

/**
 * @brief Blocking adapter that decorates a legacy reader with JSON projection.
 * @note Incomplete legacy metadata leaves inner unchanged. Complete artifacts
 * stage any native-size_t sidecar locally, validate its size against the
 * reader's row count, then read offsets and construct a JsonPathIndexReader.
 */
std::unique_ptr<IIndexReaderBase>
FinishJsonProjectedOpen(JsonProjectedOpenPlan plan,
                        storage::FileSource& source,
                        std::unique_ptr<IIndexReaderBase> inner);

/**
 * @brief Complete legacy JSON decoration with selectable source transport.
 * @pre source outlives the task; the caller provides the local I/O executor.
 * @note Only source I/O suspends. Local sidecar reads and reader construction
 * remain synchronous; use_async=false also keeps source reads synchronous.
 */
folly::coro::Task<std::unique_ptr<IIndexReaderBase>>
FinishJsonProjectedOpenAsync(bool use_async,
                             JsonProjectedOpenPlan plan,
                             storage::FileSource& source,
                             std::unique_ptr<IIndexReaderBase> inner);

}  // namespace milvus::index
