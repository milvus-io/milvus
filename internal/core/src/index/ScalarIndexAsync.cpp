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
#include "storage/AsyncLoadExecutor.h"
#include "storage/AsyncIndexEntryReader.h"
#include "storage/EntryStreamUtils.h"
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
    storage::ThrowIfCancelled(cancellation_token,
                              "ScalarIndex::OpenInputStream");
    auto input = co_await folly::coro::co_withCancellation(
        folly::CancellationToken{},
        file_manager_->OpenInputStreamAsync(packed_file, is_index_file_));
    storage::ThrowIfCancelled(cancellation_token,
                              "ScalarIndex::OpenInputStream");
    AssertInfo(
        input != nullptr, "Failed to open packed index file: {}", packed_file);
    const auto collection_id =
        GetValueFromConfig<int64_t>(config, COLLECTION_ID).value_or(0);
    auto reader = co_await storage::AsyncIndexEntryReader::Open(
        std::move(input), collection_id, load_priority, cancellation_token);
    auto plan = PlanLoad(reader->Catalog(), config);
    const bool has_file_targets = std::any_of(
        plan.entries.begin(), plan.entries.end(), [](const auto& entry) {
            return std::holds_alternative<storage::MmapEntryTarget>(
                entry.target);
        });
    storage::IndexLoadArtifact artifact;
    // Keep engine context alive through both reading and materialization. File
    // contexts can remove directories and must be released on the local executor.
    std::exception_ptr failure;
    try {
        artifact = co_await reader->ReadEntriesAsync(
            std::move(plan.entries), load_priority, cancellation_token);
        storage::ThrowIfCancelled(cancellation_token,
                                  "ScalarIndex::MaterializeAsync");
        co_await folly::coro::co_withCancellation(
            cancellation_token,
            MaterializeAsync(artifact, plan.materialization_context, config));
    } catch (...) {
        failure = std::current_exception();
    }
    auto release = [&] {
        // Destroy targets before engine context, so directory leases cover cleanup.
        auto context = std::move(plan.materialization_context);
        auto owned_artifact = std::move(artifact);
        if (!failure) {
            owned_artifact.CommitTargets();
        }
    };
    if (has_file_targets) {
        co_await storage::RunLocalFileIOAsync(release, load_priority);
    } else {
        release();
    }
    if (failure) {
        std::rethrow_exception(failure);
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
