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

#pragma once

#include <optional>
#include <string_view>
#include <utility>

#include "index/Families.h"
#include "index/IndexLoader.h"
#include "index/IndexLoadInput.h"
#include "index/IndexLoadPlan.h"
#include "storage/IndexEntryFormat.h"
#include "index/contracts/IIndex.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index {

struct NgramIndexStorage;

/**
 * @brief Loads ngram readers from legacy or packed V3 artifacts.
 *
 * Historical artifacts omit gram bounds and JSON path/type; runtime options
 * supply these values and Create validates them. Owns the opened source and
 * fixed load options. Cached construction transfers the source to the cache slot.
 */
class NgramIndexLoader final : public IndexLoader {
 public:
    static constexpr std::string_view kFamily = families::kNgram;

    /** @brief Derive capabilities from runtime parameters without I/O. */
    static ReaderCaps
    DeriveCaps(const Config& index_meta);

    /**
     * @brief Validate runtime parameters and legacy inventory; packed targets
     * await Load.
     * @param source Opened storage source owned by the resulting loader.
     * @param options Fixed load options without a retained operation context.
     * @return A ready loader with prepared family metadata.
     */
    static folly::coro::Task<std::unique_ptr<IndexLoader>>
    Create(OpenedIndexSource source, storage::LoadOptions options);

    /** @copydoc IndexLoader::Load */
    IndexPtr
    Load(milvus::OpContext* context, const IndexCacheConfig& cache) override;

 private:
    friend struct LoaderTestAccess;

    std::optional<OpenedIndexSource> source_;
    // Retained options never keep the Create caller's op_ctx.
    storage::LoadOptions options_;

    NgramIndexLoader(OpenedIndexSource source, storage::LoadOptions options)
        : source_(std::move(source)), options_(std::move(options)) {
    }

    static folly::coro::Task<std::unique_ptr<NgramIndexStorage>>
    LoadStorage(OpenedIndexSource& source,
                const storage::LoadOptions& options,
                milvus::OpContext* context);

    static folly::coro::Task<std::unique_ptr<NgramIndexStorage>>
    FinishPackedStorage(IndexLoadPlan& plan,
                        const storage::LoadOptions& opts,
                        bool use_async);

    /**
     * @brief Allocate payload destinations and state for one packed Load.
     * @note Does not read payloads. RunPackedIndexLoad owns cleanup until commit.
     */
    static IndexLoadPlan
    PlanPacked(const storage::IndexEntryDirectory& directory,
               const nlohmann::json& metadata,
               const storage::LoadOptions& opts);
};

}  // namespace milvus::index
