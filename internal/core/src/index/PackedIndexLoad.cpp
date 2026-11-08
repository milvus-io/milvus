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

#include "index/PackedIndexLoad.h"

#include "folly/Try.h"

#include "folly/coro/WithCancellation.h"
#include "storage/EntryStreamUtils.h"

#include "folly/coro/BlockingWait.h"
#include "common/OpContext.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {
namespace {

// Read only directory/metadata for family selection; the caller owns the blocking boundary.
folly::coro::Task<PackedIndexSource>
InspectPackedScalarIndex(const std::vector<std::string>& files,
                         const storage::FileManagerContext& context,
                         storage::ArtifactStorageNamespace storage_namespace,
                         bool use_async_load,
                         proto::common::LoadPriority priority,
                         folly::CancellationToken token) {
    storage::ThrowIfCancelled(token, "inspect packed index metadata");
    AssertInfo(files.size() == 1 && context.Valid(),
               "Async scalar load requires one V3 file and a valid context");
    storage::MemFileManagerImpl manager(context);
    const bool is_index_file =
        storage_namespace == storage::ArtifactStorageNamespace::Index;
    folly::Try<std::shared_ptr<InputStream>> opened;
    if (use_async_load) {
        auto metadata_lease =
            co_await storage::AcquireMetadataLoadSlotAsync(priority, token);
        storage::ThrowIfCancelled(token, "open packed index metadata");
        opened =
            co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
                folly::CancellationToken{},
                manager.OpenInputStreamAsync(files.front(), is_index_file)));
    } else {
        opened = folly::makeTryWith([&] {
            return manager.OpenInputStream(files.front(), is_index_file);
        });
    }
    storage::ThrowIfCancelled(token, "open packed index metadata");
    auto input = std::move(opened).value();
    AssertInfo(input != nullptr, "Failed to open packed scalar index");
    if (!use_async_load) {
        co_return PackedIndexSource{std::shared_ptr<storage::IndexEntryReader>(
            storage::IndexEntryReader::Open(input,
                                            input->Size(),
                                            context.fieldDataMeta.collection_id,
                                            milvus::PriorityForLoad(priority),
                                            token))};
    }
    auto reader = co_await storage::AsyncIndexEntryReader::Open(
        std::move(input), context.fieldDataMeta.collection_id, priority, token);
    co_return PackedIndexSource{
        std::shared_ptr<storage::AsyncIndexEntryReader>(std::move(reader))};
}

}  // namespace

PackedIndexSource
InspectPackedIndexFile(const std::vector<std::string>& files,
                       const storage::FileManagerContext& context,
                       storage::ArtifactStorageNamespace storage_namespace,
                       milvus::OpContext* op_ctx) {
    const bool use_async = context.use_async_load.value_or(
        segcore::storagev2translator::StorageV2AsyncLoadEnabled());
    const auto priority =
        op_ctx && op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    auto task = InspectPackedScalarIndex(
        files,
        context,
        storage_namespace,
        use_async,
        priority,
        op_ctx ? op_ctx->cancellation_token : folly::CancellationToken{});
    return use_async ? folly::coro::blockingWait(folly::coro::co_withExecutor(
                           storage::ResolveAsyncLoadExecutor({}, priority),
                           std::move(task)))
                     : folly::coro::blockingWait(std::move(task));
}

}  // namespace milvus::index
