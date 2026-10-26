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

#include <cstdlib>
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
    std::shared_ptr<InputStream> input;
    if (use_async_load) {
        auto metadata_lease =
            co_await storage::AcquireMetadataLoadSlotAsync(priority, token);
        storage::ThrowIfCancelled(token, "open packed index metadata");
        input = co_await folly::coro::co_withCancellation(
            folly::CancellationToken{},
            manager.OpenInputStreamAsync(files.front(), is_index_file));
    } else {
        input = manager.OpenInputStream(files.front(), is_index_file);
    }
    storage::ThrowIfCancelled(token, "open packed index metadata");
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

folly::coro::Task<IIndexReaderBasePtr>
RunPackedIndexLoad(PackedIndexSource& source,
                   storage::LoadOptions options,
                   PackedPlanFn make_plan,
                   PackedFinishFn finish,
                   milvus::OpContext* context) {
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
                &source.reader)) {
        // Keep planning and finalization on the synchronous caller. Stream
        // tasks consume their slices on the existing HIGH/LOW executor.
        auto& entry_reader = **sync;
        entry_reader.SetLoadContext(
            priority == proto::common::LoadPriority::HIGH
                ? ThreadPoolPriority::HIGH
                : ThreadPoolPriority::LOW,
            token);
        auto reset = folly::makeGuard(
            [&] { entry_reader.SetLoadContext(ThreadPoolPriority::HIGH, {}); });
        storage::ThrowIfCancelled(token, "plan packed index");
        auto plan = make_plan(
            entry_reader.Directory(), entry_reader.IndexMeta(), options);
        const auto files = storage::CollectIndexFileTargets(plan.entries);
        for (const auto& file : files) {
            file->Prepare(storage::io::GetPriorityFromLoadPriority(priority));
        }
        for (const auto& entry : plan.entries) {
            storage::ThrowIfCancelled(token, "read packed index");
            const auto expected =
                entry_reader.Directory().At(entry.name).plaintext_size;
            AssertInfo(expected <= storage::EntryTargetSize(entry.target),
                       "packed entry exceeds planned destination: {}",
                       entry.name);
            auto write = [&](size_t offset, const uint8_t* bytes, size_t size) {
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
                    AssertInfo(
                        file.offset <= file.staging->file_size &&
                            offset <= file.staging->file_size - file.offset &&
                            size <=
                                file.staging->file_size - file.offset - offset,
                        "packed file target exceeds file size: {}",
                        entry.name);
                    const auto absolute = file.offset + offset;
                    const auto full_padding =
                        (-size) & storage::FileWriter::ALIGNMENT_MASK;
                    const auto padding =
                        std::min(full_padding,
                                 file.staging->file_size - absolute - size);
                    if (plan.worker_writes && padding != 0 &&
                        absolute + size != file.staging->file_size &&
                        file.staging->write_mode !=
                            storage::FileWriter::WriteMode::BUFFERED) {
                        // Match the async plan's per-entry alignment region.
                        // Aligned scratch avoids another bounce buffer in WriteAt.
                        AssertInfo(offset <= file.bytes &&
                                       size <= file.bytes - offset &&
                                       padding <= file.bytes - offset - size &&
                                       padding <= file.staging->file_size -
                                                      absolute - size,
                                   "packed entry lacks alignment padding: {}",
                                   entry.name);
                        if (padding != full_padding) {
                            // Some plans reserve only a small EOF safety pad
                            // (FM-index), not a complete aligned block. Write
                            // the aligned prefix first, then a small EOF tail;
                            // do not retain two whole-slice bounce buffers.
                            const auto prefix =
                                size & ~storage::FileWriter::ALIGNMENT_MASK;
                            if (prefix != 0) {
                                file.staging->WriteAt(absolute, bytes, prefix);
                            }
                            std::vector<uint8_t> tail(size - prefix + padding,
                                                      0);
                            std::memcpy(
                                tail.data(), bytes + prefix, size - prefix);
                            file.staging->WriteAt(
                                absolute + prefix, tail.data(), tail.size());
                            return;
                        }
                        auto* padded = static_cast<uint8_t*>(std::aligned_alloc(
                            storage::FileWriter::ALIGNMENT_BYTES,
                            size + padding));
                        if (padded == nullptr) {
                            ThrowInfo(ErrorCode::MemAllocateFailed,
                                      "cannot allocate aligned packed slice");
                        }
                        auto release =
                            folly::makeGuard([&] { std::free(padded); });
                        std::memcpy(padded, bytes, size);
                        std::memset(padded + size, 0, padding);
                        file.staging->WriteAt(absolute, padded, size + padding);
                    } else {
                        file.staging->WriteAt(absolute, bytes, size);
                    }
                }
            };
            if (plan.worker_writes) {
                entry_reader.ReadEntryStreamAt(entry.name, write);
            } else {
                size_t offset = 0;
                entry_reader.ReadEntryStream(
                    entry.name, [&](const uint8_t* bytes, size_t size) {
                        write(offset, bytes, size);
                        offset += size;
                    });
                AssertInfo(
                    offset == expected, "short packed entry: {}", entry.name);
            }
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
    auto& entry_reader =
        *std::get<std::shared_ptr<storage::AsyncIndexEntryReader>>(
            source.reader);
    storage::ThrowIfCancelled(token, "plan packed index");
    std::optional<IndexLoadPlan> plan;
    IIndexReaderBasePtr reader;
    std::exception_ptr failure;
    try {
        // Planning can create directories. Await the complete local phase even
        // after cancellation so its captured references remain valid.
        co_await storage::RunLocalFileIOAsync(
            [&] {
                plan.emplace(make_plan(entry_reader.Directory(),
                                       entry_reader.IndexMeta(),
                                       options));
            },
            priority);
        storage::ThrowIfCancelled(token, "read packed index");
        // The reader drains issued reads/writes before propagating failure;
        // plan targets must remain alive until this await completes.
        co_await entry_reader.ReadEntriesAsync(plan->entries, priority, token);
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
