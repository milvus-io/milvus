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
#include <utility>

#include "index/Families.h"
#include "index/IndexLoader.h"
#include "index/IndexLoadInput.h"
#include "index/IndexLoadPlan.h"
#include "storage/IndexEntryFormat.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index {

/**
 * @brief Loads flat JSON readers from legacy or packed V3 artifacts.
 * @pre Legacy input uses V1SourceLayout::DiskFiles for Tantivy directory
 * slices. Packed input uses the file_names/has_null metadata instead.
 *
 * Owns the opened input and fixed load options; each Load creates a new reader.
 */
class JsonFlatIndexLoader final : public IndexLoader {
 public:
    static constexpr std::string_view kFamily = families::kJsonFlat;

    /** @brief Derive capabilities from runtime parameters without I/O. */
    static ReaderCaps
    DeriveCaps(const Config& index_meta);

    /**
     * @brief Validate runtime parameters and legacy inventory; packed targets
     * await Load.
     * @param request Input/options; borrowed op_ctx must outlive this task.
     * @return A ready loader with no retained opening context.
     * @note Failure also detaches the source's opening context before the
     * exception propagates.
     */
    static folly::coro::Task<std::unique_ptr<IndexLoader>>
    Open(IndexOpenRequest request);

    /** @copydoc IndexLoader::Load */
    folly::coro::Task<IIndexReaderBasePtr>
    Load(milvus::OpContext* context = nullptr) override;

 private:
    friend struct LoaderTestAccess;

    OpenedIndexInput input_;
    // Retained options never keep the Open caller's op_ctx.
    storage::LoadOptions options_;

    JsonFlatIndexLoader(OpenedIndexInput input, storage::LoadOptions options)
        : input_(std::move(input)), options_(std::move(options)) {
    }

    /**
     * @brief Materialize flat-JSON Tantivy files and construct the JSON reader.
     * @note Runs inside RunLegacyLoad; use_async controls source I/O only.
     */
    static folly::coro::Task<IIndexReaderBasePtr>
    LoadLegacy(storage::FileSource& source,
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

    /**
     * @brief Initialize a reader from populated targets without committing them.
     * @pre All planned reads and local writes have finished successfully.
     * @note Async offloads blocking file operations and resumes CPU work on
     * the loading executor. Sync executes inline on the caller thread.
     */
    static folly::coro::Task<IIndexReaderBasePtr>
    FinishPacked(IndexLoadPlan& plan,
                 const storage::LoadOptions& opts,
                 bool use_async);
};

}  // namespace milvus::index
