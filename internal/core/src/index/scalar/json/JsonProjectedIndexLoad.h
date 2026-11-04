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
#include "index/scalar/json/JsonPathIndexReader.h"
#include "common/Types.h"
#include "index/contracts/query/ReaderCaps.h"
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
 * index-decoration state prepared during Load, not reusable IndexLoader state.
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

/** @brief Derive optional routing metadata without loading storage. */
std::optional<JsonProjectedIndexSpec>
JsonProjectedSpec(const Config& params, std::optional<int64_t> row_count);

/** @brief Validate loaded sidecar data before publishing its owning storage cell. */
std::shared_ptr<const TargetBitmap>
FinishPackedJsonExists(JsonProjectedOpenPlan plan, int64_t row_count);

/** @brief Load and validate the legacy sidecar as part of its storage transaction. */
folly::coro::Task<std::shared_ptr<const TargetBitmap>>
LoadJsonExists(bool use_async,
               JsonProjectedOpenPlan plan,
               storage::FileSource& source,
               int64_t row_count);

/**
 * @brief Whether a JSON path projects multiple values per parent row.
 * @note Keep the outer JSON field type for projection decoration; families
 * use this to disable scalar-only coordinate and reverse-lookup assumptions.
 */
bool
IsJsonArrayProjection(const Config& params);

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

}  // namespace milvus::index
