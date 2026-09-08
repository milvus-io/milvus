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

#include "index/ScalarIndex.h"

#include <algorithm>

#include "common/Types.h"
#include "folly/coro/WithCancellation.h"
#include "index/Utils.h"
#include "segcore/storagev2translator/AsyncLoadExecutor.h"
#include "storage/AsyncIndexEntryReader.h"
#include "storage/EntryStreamUtils.h"
#include "storage/IndexMaterializer.h"
#include "storage/LocalFileIOPool.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {

template <typename T>
folly::coro::Task<void>
ScalarIndex<T>::LoadUnifiedAsync(const std::string& packed_file,
                                 const Config& config,
                                 proto::common::LoadPriority load_priority,
                                 folly::CancellationToken cancellation_token) {
    cancellation_token = folly::cancellation_token_merge(
        cancellation_token,
        co_await folly::coro::co_current_cancellation_token);
    auto input = file_manager_->OpenInputStream(packed_file, is_index_file_);
    AssertInfo(
        input != nullptr, "Failed to open packed index file: {}", packed_file);
    const auto file_size = input->Size();
    const auto collection_id =
        GetValueFromConfig<int64_t>(config, COLLECTION_ID).value_or(0);
    auto reader =
        co_await storage::AsyncIndexEntryReader::Open(std::move(input),
                                                      file_size,
                                                      collection_id,
                                                      load_priority,
                                                      cancellation_token);
    auto plan = PlanLoad(reader->Catalog(), config);
    plan.priority = load_priority;
    auto artifact = co_await storage::MaterializeIndexAsync(
        *reader, std::move(plan), cancellation_token);
    const bool has_file_targets =
        std::any_of(artifact.Entries().begin(),
                    artifact.Entries().end(),
                    [](const auto& entry) {
                        return std::holds_alternative<storage::MmapEntryTarget>(
                            entry.target);
                    });
    // Keep the artifact local to this coroutine body so both finalization and
    // destructor cleanup run on the selected executor, including on failure.
    auto finalize = [&]() -> folly::coro::Task<void> {
        auto owned_artifact = std::move(artifact);
        storage::ThrowIfCancelled(cancellation_token,
                                  "ScalarIndex::FinalizeLoad");
        FinalizeLoad(std::move(owned_artifact), config);
        owned_artifact.CommitTargets();
        co_return;
    };
    if (has_file_targets) {
        // Acquire the local-file executor only after remote reads have drained,
        // so disabling that pool does not wait for unrelated network I/O.
        co_await folly::coro::co_withCancellation(
            folly::CancellationToken{},
            folly::coro::co_withExecutor(
                segcore::storagev2translator::ResolveAsyncLoadExecutor(
                    storage::LocalFileIOPool::GetInstance().GetExecutor(),
                    load_priority),
                finalize()));
    } else {
        co_await finalize();
    }
}

template folly::coro::Task<void>
ScalarIndex<bool>::LoadUnifiedAsync(const std::string&,
                                    const Config&,
                                    proto::common::LoadPriority,
                                    folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int8_t>::LoadUnifiedAsync(const std::string&,
                                      const Config&,
                                      proto::common::LoadPriority,
                                      folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int16_t>::LoadUnifiedAsync(const std::string&,
                                       const Config&,
                                       proto::common::LoadPriority,
                                       folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int32_t>::LoadUnifiedAsync(const std::string&,
                                       const Config&,
                                       proto::common::LoadPriority,
                                       folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int64_t>::LoadUnifiedAsync(const std::string&,
                                       const Config&,
                                       proto::common::LoadPriority,
                                       folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<uint64_t>::LoadUnifiedAsync(const std::string&,
                                        const Config&,
                                        proto::common::LoadPriority,
                                        folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<float>::LoadUnifiedAsync(const std::string&,
                                     const Config&,
                                     proto::common::LoadPriority,
                                     folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<double>::LoadUnifiedAsync(const std::string&,
                                      const Config&,
                                      proto::common::LoadPriority,
                                      folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<std::string>::LoadUnifiedAsync(const std::string&,
                                           const Config&,
                                           proto::common::LoadPriority,
                                           folly::CancellationToken);

}  // namespace milvus::index
