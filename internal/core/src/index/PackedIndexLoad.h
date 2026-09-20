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

#include "folly/CancellationToken.h"
#include "folly/coro/Task.h"
#include "index/contracts/Registry.h"
#include "index/IndexLoadInput.h"
#include "index/IndexLoadPlan.h"
#include <functional>
#include <variant>
#include "storage/AsyncIndexEntryReader.h"
#include "storage/IndexEntryReader.h"
#include "storage/FileManager.h"

namespace milvus::index {

/** @brief Allocate per-load destinations from the packed input's metadata. */
using PackedPlanFn =
    std::function<IndexLoadPlan(const storage::IndexEntryDirectory&,
                                const nlohmann::json&,
                                const storage::LoadOptions&)>;

/**
 * @brief Construct a reader from populated destinations, without committing
 * them.
 */
using PackedSyncFinishFn = IIndexReaderBasePtr (*)(IndexLoadPlan&,
                                                   const storage::LoadOptions&);
// Async families offload blocking file phases themselves; false executes inline.
using PackedAsyncFinishFn = folly::coro::Task<IIndexReaderBasePtr> (*)(
    IndexLoadPlan&, const storage::LoadOptions&, bool);
using PackedFinishFn = std::variant<PackedSyncFinishFn, PackedAsyncFinishFn>;

/**
 * @brief Plan, read, initialize and commit one packed reader generation.
 * @param input Opened packed source; legacy inputs cannot enter this operation.
 * @param options Fixed family configuration, copied with context for this call.
 * @param plan Family target allocation; does not read payloads.
 * @param finish Family reader initialization after all target writes finish.
 * @param context Borrowed through task completion; may be null.
 * @return A reader owning its resources.
 * @note Failure drains I/O, destroys any reader before its backing files, and
 * discards uncommitted targets.
 * @note Sync executes inline. Async uses the existing read and local-file
 * pools.
 */
folly::coro::Task<IIndexReaderBasePtr>
RunPackedIndexLoad(PackedIndexSource& input,
                   const storage::LoadOptions& options,
                   PackedPlanFn plan,
                   PackedFinishFn finish,
                   milvus::OpContext* context);

/**
 * @brief Inspect one packed file's directory and metadata without loading
 * payloads.
 * @note Blocking family-selection boundary; transport follows the pinned mode.
 * @return An owned metadata reader, including when stream opening is
 * synchronous.
 */
std::unique_ptr<storage::AsyncIndexEntryReader>
InspectPackedIndexFile(const std::vector<std::string>& files,
                       const storage::FileManagerContext& context,
                       bool is_index_file = true);

}  // namespace milvus::index
