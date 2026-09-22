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
#include "folly/ScopeGuard.h"

#include <cstring>
#include <exception>
#include <optional>

#include "folly/coro/WithCancellation.h"
#include "storage/EntryStreamUtils.h"
#include "storage/LocalFileIOPool.h"

#include "folly/coro/BlockingWait.h"
#include "common/OpContext.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {
namespace {

// Families own executor switches within finalization; synchronous callbacks stay inline.
folly::coro::Task<IIndexReaderBasePtr>
FinishPackedLoad(PackedFinishFn finish,
                 IndexLoadPlan& plan,
                 const storage::LoadOptions& options,
                 bool use_async) {
    if (const auto* sync = std::get_if<PackedSyncFinishFn>(&finish)) {
        co_return (*sync)(plan, options);
    }
    co_return co_await std::get<PackedAsyncFinishFn>(finish)(
        plan, options, use_async);
}

// Read only directory/metadata for family selection; the caller owns the blocking boundary.
folly::coro::Task<std::unique_ptr<storage::AsyncIndexEntryReader>>
InspectPackedScalarIndex(const std::vector<std::string>& files,
                         const storage::FileManagerContext& context,
                         bool is_index_file,
                         bool use_async_load) {
    AssertInfo(files.size() == 1 && context.Valid(),
               "Async scalar load requires one V3 file and a valid context");
    storage::MemFileManagerImpl manager(context);
    std::shared_ptr<InputStream> input;
    if (use_async_load) {
        input = co_await folly::coro::co_withCancellation(
            folly::CancellationToken{},
            manager.OpenInputStreamAsync(files.front(), is_index_file));
    } else {
        input = manager.OpenInputStream(files.front(), is_index_file);
    }
    AssertInfo(input != nullptr, "Failed to open packed scalar index");
    co_return co_await storage::AsyncIndexEntryReader::Open(
        std::move(input),
        context.fieldDataMeta.collection_id,
        proto::common::LoadPriority::HIGH,
        {});
}

}  // namespace

std::unique_ptr<storage::AsyncIndexEntryReader>
InspectPackedIndexFile(const std::vector<std::string>& files,
                       const storage::FileManagerContext& context,
                       bool is_index_file) {
    const bool use_async = context.use_async_load.value_or(
        segcore::storagev2translator::StorageV2AsyncLoadEnabled());
    auto task =
        InspectPackedScalarIndex(files, context, is_index_file, use_async);
    return use_async ? folly::coro::blockingWait(folly::coro::co_withExecutor(
                           storage::ResolveAsyncLoadExecutor(
                               {}, proto::common::LoadPriority::HIGH),
                           std::move(task)))
                     : folly::coro::blockingWait(std::move(task));
}

folly::coro::Task<IIndexReaderBasePtr>
RunPackedIndexLoad(PackedIndexSource& input,
                   const storage::LoadOptions& fixed_options,
                   PackedPlanFn make_plan,
                   PackedFinishFn finish,
                   milvus::OpContext* context) {
    auto options = fixed_options;
    options.op_ctx = context;
    const auto priority =
        context && context->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    const auto operation_token =
        context ? context->cancellation_token : folly::CancellationToken{};
    const auto token = folly::cancellation_token_merge(
        operation_token, co_await folly::coro::co_current_cancellation_token);
    if (const auto* sync =
            std::get_if<std::shared_ptr<storage::IndexEntryReader>>(
                &input.reader)) {
        // Keep planning, streaming and finalization on the synchronous caller;
        // the Task interface alone must not introduce an executor hop.
        auto& source = **sync;
        source.SetLoadContext(priority == proto::common::LoadPriority::HIGH
                                  ? ThreadPoolPriority::HIGH
                                  : ThreadPoolPriority::LOW,
                              token);
        auto reset = folly::makeGuard(
            [&] { source.SetLoadContext(ThreadPoolPriority::HIGH, {}); });
        storage::ThrowIfCancelled(token, "plan packed index");
        auto plan = make_plan(source.Directory(), source.IndexMeta(), options);
        const auto files = storage::CollectIndexFileTargets(plan.entries);
        for (const auto& file : files) {
            file->Prepare(storage::io::GetPriorityFromLoadPriority(priority));
        }
        for (const auto& entry : plan.entries) {
            storage::ThrowIfCancelled(token, "read packed index");
            const auto expected =
                source.Directory().At(entry.name).plaintext_size;
            AssertInfo(expected <= storage::EntryTargetSize(entry.target),
                       "packed entry exceeds planned destination: {}",
                       entry.name);
            size_t offset = 0;
            source.ReadEntryStream(
                entry.name, [&](const uint8_t* bytes, size_t size) {
                    storage::ThrowIfCancelled(token, "read packed index");
                    AssertInfo(offset <= expected && size <= expected - offset,
                               "packed entry exceeds declared size: {}",
                               entry.name);
                    if (const auto* memory =
                            std::get_if<storage::MemoryEntryTarget>(
                                &entry.target)) {
                        if (size != 0) {
                            std::memcpy(memory->data + offset, bytes, size);
                        }
                    } else {
                        const auto& file =
                            std::get<storage::FileEntryTarget>(entry.target);
                        file.staging->WriteAt(
                            file.offset + offset, bytes, size);
                    }
                    offset += size;
                });
            AssertInfo(
                offset == expected, "short packed entry: {}", entry.name);
        }
        for (const auto& file : files) {
            file->Finish();
        }
        storage::ThrowIfCancelled(token, "initialize packed index");
        auto reader = co_await FinishPackedLoad(finish, plan, options, false);
        AssertInfo(reader != nullptr, "packed loader returned a null reader");
        storage::ThrowIfCancelled(token, "publish packed index");
        plan.Commit();
        co_return reader;
    }
    auto& source = *std::get<std::shared_ptr<storage::AsyncIndexEntryReader>>(
        input.reader);
    storage::ThrowIfCancelled(token, "plan packed index");
    std::optional<IndexLoadPlan> plan;
    IIndexReaderBasePtr reader;
    std::exception_ptr failure;
    try {
        // Planning can create directories. Await the complete local phase even
        // after cancellation so its captured references remain valid.
        co_await storage::RunLocalFileIOAsync(
            [&] {
                plan.emplace(
                    make_plan(source.Directory(), source.IndexMeta(), options));
            },
            priority);
        storage::ThrowIfCancelled(token, "read packed index");
        // The reader drains issued reads/writes before propagating failure;
        // plan targets must remain alive until this await completes.
        co_await source.ReadEntriesAsync(plan->entries, priority, token);
        storage::ThrowIfCancelled(token, "initialize packed index");
        // Each family switches only its blocking file phases; CPU conversion
        // resumes on the caller's loading executor.
        reader = co_await FinishPackedLoad(finish, *plan, options, true);
        AssertInfo(reader != nullptr, "packed loader returned a null reader");
        storage::ThrowIfCancelled(token, "publish packed index");
    } catch (...) {
        failure = std::current_exception();
    }
    // A failed reader may own mappings into planned files. Destroy it before
    // removing those files; release the plan's context on the local executor.
    co_await storage::RunLocalFileIOAsync(
        [&] {
            // Cancellation can arrive while this final local task is queued.
            if (!failure) {
                try {
                    storage::ThrowIfCancelled(token, "publish packed index");
                } catch (...) {
                    failure = std::current_exception();
                }
            }
            if (failure) {
                reader.reset();
            } else {
                plan->Commit();
            }
            plan.reset();
        },
        priority);
    if (failure) {
        std::rethrow_exception(failure);
    }
    co_return std::move(reader);
}

}  // namespace milvus::index
