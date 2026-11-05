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

#include <string_view>
#include <optional>
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

template <typename T>
struct SortedIndexStorage;

/**
 * @brief Loads sorted indexes from legacy or packed V3 artifacts.
 *
 * Owns the opened source and fixed load options; each Load creates a new reader.
 * Create retains parsed family metadata without allocating payload targets.
 */
class SortedIndexLoader final : public IndexLoader {
 public:
    static constexpr std::string_view kFamily = families::kSort;

    ~SortedIndexLoader() override;

    /** @brief Derive capabilities from runtime parameters without I/O. */
    static ReaderCaps
    DeriveCaps(const Config& index_meta);

    /**
     * @brief Read and retain sorted counts and nesting before payload loading.
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

    SortedIndexLoader(OpenedIndexSource source, storage::LoadOptions options);

    /** @brief Reusable metadata, without per-load payload ownership. */
    struct PreparedState;
    std::unique_ptr<PreparedState> state_;

    template <typename T>
    IndexPtr
    LoadTyped(milvus::OpContext* context, const IndexCacheConfig& cache);

    template <typename T>
    static folly::coro::Task<std::unique_ptr<SortedIndexStorage<T>>>
    LoadStorage(OpenedIndexSource& source,
                const storage::LoadOptions& options,
                const PreparedState& prepared,
                milvus::OpContext* context);

    template <typename T>
    static folly::coro::Task<std::unique_ptr<SortedIndexStorage<T>>>
    FinishPackedStorage(IndexLoadPlan& plan,
                        const storage::LoadOptions& opts,
                        bool use_async);

    /**
     * @brief Validate packed metadata without allocating per-load destinations.
     */
    static PreparedState
    PreparePackedState(const storage::IndexEntryDirectory&,
                       const nlohmann::json&,
                       const storage::LoadOptions&);

    /**
     * @brief Allocate payload destinations and state for one packed Load.
     * @note Does not read payloads. RunPackedIndexLoad owns cleanup until commit.
     */
    static IndexLoadPlan
    PlanPacked(const storage::IndexEntryDirectory& directory,
               const nlohmann::json& metadata,
               const storage::LoadOptions& opts,
               const PreparedState* cached = nullptr);
};

}  // namespace milvus::index
