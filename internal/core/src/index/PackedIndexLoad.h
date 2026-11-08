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
#include <type_traits>
#include "storage/AsyncIndexEntryReader.h"
#include "storage/IndexEntryReader.h"
#include "storage/FileManager.h"

namespace milvus::index {

/** @brief Allocate per-load destinations from the packed source's metadata. */
using PackedPlanFn =
    std::function<IndexLoadPlan(const storage::IndexEntryDirectory&,
                                const nlohmann::json&,
                                const storage::LoadOptions&)>;

/** @brief Finish initialization before the transaction commits its targets. */
template <typename Result>
using PackedSyncFinish = Result (*)(IndexLoadPlan&,
                                    const storage::LoadOptions&);
// Async families offload blocking file phases themselves; false executes inline.
template <typename Result>
using PackedAsyncFinish = folly::coro::Task<Result> (*)(
    IndexLoadPlan&, const storage::LoadOptions&, bool);
template <typename Result>
using PackedFinish =
    std::variant<PackedSyncFinish<Result>, PackedAsyncFinish<Result>>;
using PackedSyncFinishFn = PackedSyncFinish<IndexPtr>;
using PackedAsyncFinishFn = PackedAsyncFinish<IndexPtr>;
using PackedFinishFn = PackedFinish<IndexPtr>;

/**
 * @brief Plan, read, initialize and commit one packed storage or index generation.
 * @param source Opened packed source; legacy sources cannot enter this
 * operation.
 * @param options Fixed family configuration, owned by the task before it starts.
 * @param plan Family target allocation; does not read payloads.
 * @param finish Family storage/index initialization after all target writes finish.
 * @param context Borrowed through task completion; may be null.
 * @return A smart pointer owning the initialized storage or index.
 * @note Failure drains I/O, destroys the result before its backing files, and
 * discards uncommitted targets.
 * @note Sync executes inline. Async uses the existing read and local-file
 * pools.
 */
template <typename Result = IndexPtr>
folly::coro::Task<Result>
RunPackedIndexLoad(PackedIndexSource& source,
                   storage::LoadOptions options,
                   PackedPlanFn plan,
                   std::type_identity_t<PackedFinish<Result>> finish,
                   milvus::OpContext* context);

/**
 * @brief Inspect one packed file's directory and metadata without loading
 * payloads.
 * @note Blocking family-selection boundary; stream opening and metadata reads
 * follow the pinned mode, or the global configuration when no mode is pinned.
 * @param op_ctx Borrowed during inspection for cancellation and load priority.
 * @return A packed source retaining the selected reader.
 */
PackedIndexSource
InspectPackedIndexFile(const std::vector<std::string>& files,
                       const storage::FileManagerContext& context,
                       storage::ArtifactStorageNamespace storage_namespace =
                           storage::ArtifactStorageNamespace::Index,
                       milvus::OpContext* op_ctx = nullptr);

}  // namespace milvus::index

#include "index/PackedIndexLoad-inl.h"
