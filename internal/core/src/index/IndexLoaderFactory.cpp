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

#include "index/IndexLoaderFactory.h"
#include "index/contracts/Registry.h"
#include "common/OpContext.h"
#include "folly/ScopeGuard.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/WithCancellation.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/EntryStreamUtils.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {
namespace {

// Map the optional operation priority to storage admission priority.
proto::common::LoadPriority
Priority(milvus::OpContext* context) {
    return context && context->runtime_load_priority.value_or(0) != 0
               ? proto::common::LoadPriority::LOW
               : proto::common::LoadPriority::HIGH;
}

// Copy the operation token; inherited coroutine cancellation is merged by callers.
folly::CancellationToken
Token(milvus::OpContext* context) {
    return context ? context->cancellation_token : folly::CancellationToken{};
}

// Only remote legacy sources retain admission priority and cancellation state.
void
SetLegacyContext(storage::FileSource& source,
                 proto::common::LoadPriority priority,
                 folly::CancellationToken token) {
    if (auto* remote = dynamic_cast<storage::V1RemoteSource*>(&source)) {
        remote->SetLoadContext(priority, std::move(token));
    }
}

// Bind/reset retained source context; async packed reads take context per operation.
void
SetInputContext(OpenedIndexInput& input,
                proto::common::LoadPriority priority,
                folly::CancellationToken token) {
    if (auto* legacy = std::get_if<LegacyIndexSource>(&input)) {
        AssertInfo(legacy->source != nullptr, "Null legacy source");
        SetLegacyContext(*legacy->source, priority, std::move(token));
    } else {
        auto& packed = std::get<PackedIndexSource>(input);
        std::visit(
            [&](auto& reader) {
                AssertInfo(reader != nullptr, "Null packed source");
                if constexpr (std::is_same_v<
                                  std::decay_t<decltype(reader)>,
                                  std::shared_ptr<storage::IndexEntryReader>>) {
                    reader->SetLoadContext(
                        priority == proto::common::LoadPriority::HIGH
                            ? ThreadPoolPriority::HIGH
                            : ThreadPoolPriority::LOW,
                        token);
                }
            },
            packed.reader);
    }
}

// Open storage metadata or retain an existing source; family decoding happens later.
folly::coro::Task<OpenedIndexInput>
OpenIndexInput(const IndexOpenRequest& request) {
    const auto priority = Priority(request.options.op_ctx);
    const auto operation_token = Token(request.options.op_ctx);
    const auto token = folly::cancellation_token_merge(
        operation_token, co_await folly::coro::co_current_cancellation_token);
    storage::ThrowIfCancelled(token, "open index input");
    if (const auto* input = std::get_if<OpenedIndexInput>(&request.input)) {
        // Binding belongs to the complete opening scope below.
        auto result = *input;
        co_return result;
    }
    const auto& files = std::get<IndexFiles>(request.input);
    const bool use_async = files.context.use_async_load.value_or(
        segcore::storagev2translator::StorageV2AsyncLoadEnabled());
    if (const auto* legacy = std::get_if<LegacyIndexFiles>(&files.format)) {
        // The source retains only token/priority, never this temporary context.
        // Include inherited cancellation even during its synchronous constructor.
        milvus::OpContext open_context(token);
        open_context.runtime_load_priority =
            priority == proto::common::LoadPriority::HIGH ? 0 : 1;
        auto open_options = request.options;
        open_options.op_ctx = &open_context;
        std::shared_ptr<storage::FileSource> source;
        if (use_async) {
            source = co_await folly::coro::co_withCancellation(
                token,
                storage::V1RemoteSource::OpenAsync(files.context,
                                                   files.paths,
                                                   open_options,
                                                   legacy->path,
                                                   legacy->layout));
        } else {
            source = std::make_shared<storage::V1RemoteSource>(files.context,
                                                               files.paths,
                                                               open_options,
                                                               legacy->path,
                                                               legacy->layout);
        }
        storage::ThrowIfCancelled(token, "opened legacy source");
        SetLegacyContext(*source, priority, token);

        co_return OpenedIndexInput{
            LegacyIndexSource{std::move(source), use_async}};
    }
    AssertInfo(files.paths.size() == 1,
               "Packed scalar index requires one file");
    storage::MemFileManagerImpl manager(files.context);
    const auto is_index_file =
        std::get<PackedIndexFile>(files.format).is_index_file;
    if (use_async) {
        // Stream opening must drain before manager/input owners leave this frame.
        auto opened =
            co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
                folly::CancellationToken{},
                manager.OpenInputStreamAsync(files.paths.front(),
                                             is_index_file)));
        storage::ThrowIfCancelled(token, "open packed index");
        auto input = std::move(opened).value();
        auto source = co_await storage::AsyncIndexEntryReader::Open(
            std::move(input),
            files.context.fieldDataMeta.collection_id,
            priority,
            token);
        co_return OpenedIndexInput{
            PackedIndexSource{std::shared_ptr<storage::AsyncIndexEntryReader>(
                std::move(source))}};
    }
    auto input = manager.OpenInputStream(files.paths.front(), is_index_file);
    AssertInfo(input != nullptr, "Failed to open packed index");
    auto source = storage::IndexEntryReader::Open(
        input,
        input->Size(),
        files.context.fieldDataMeta.collection_id,
        priority == proto::common::LoadPriority::HIGH ? ThreadPoolPriority::HIGH
                                                      : ThreadPoolPriority::LOW,
        token);

    co_return OpenedIndexInput{PackedIndexSource{
        std::shared_ptr<storage::IndexEntryReader>(std::move(source))}};
}

}  // namespace

folly::coro::Task<std::unique_ptr<IndexLoader>>
OpenIndexLoader(IndexOpenRequest request, CreateIndexLoaderFn create) {
    auto* context = request.options.op_ctx;
    const auto priority = Priority(context);
    const auto operation_token = Token(context);
    const auto token = folly::cancellation_token_merge(
        operation_token, co_await folly::coro::co_current_cancellation_token);
    auto input = co_await OpenIndexInput(request);
    SetInputContext(input, priority, token);
    // The guard also detaches a borrowed source if family validation throws.
    auto detach = folly::makeGuard(
        [&] { SetInputContext(input, proto::common::LoadPriority::HIGH, {}); });
    // Family state may outlive this call; only inherited cancellation crosses
    // into construction, never the borrowed OpContext pointer.
    request.options.op_ctx = nullptr;
    storage::ThrowIfCancelled(token, "create index loader");
    auto loader = co_await folly::coro::co_withCancellation(
        token, create(input, std::move(request.options)));
    AssertInfo(loader != nullptr, "Index factory returned null loader");
    storage::ThrowIfCancelled(token, "opened index loader");
    co_return loader;
}

folly::coro::Task<IIndexReaderBasePtr>
LoadIndexAsync(const LoaderEntry& entry, IndexOpenRequest request) {
    AssertInfo(entry.open != nullptr, "Index family has no loader factory");
    auto* context = request.options.op_ctx;
    const auto operation_token =
        context ? context->cancellation_token : folly::CancellationToken{};
    const auto token = folly::cancellation_token_merge(
        operation_token, co_await folly::coro::co_current_cancellation_token);
    // Await Load inside the loader's scope; returning its lazy task would
    // destroy the loader before that task can use its retained input.
    auto run = [&]() -> folly::coro::Task<IIndexReaderBasePtr> {
        storage::ThrowIfCancelled(token, "open index loader");
        auto loader = co_await entry.open(std::move(request));
        AssertInfo(loader != nullptr, "Index factory returned null loader");
        storage::ThrowIfCancelled(token, "load index reader");
        co_return co_await loader->Load(context);
    };
    co_return co_await folly::coro::co_withCancellation(token, run());
}

IIndexReaderBasePtr
LoadIndex(const LoaderEntry& loader, IndexOpenRequest request) {
    bool use_async;
    if (auto* files = std::get_if<IndexFiles>(&request.input)) {
        use_async = files->context.use_async_load.value_or(
            segcore::storagev2translator::StorageV2AsyncLoadEnabled());
        // Executor selection and storage opening must use the same snapshot.
        files->context.use_async_load = use_async;
    } else {
        const auto& input = std::get<OpenedIndexInput>(request.input);
        const auto* source = std::get_if<LegacyIndexSource>(&input);
        use_async = source
                        ? source->use_async
                        : std::holds_alternative<
                              std::shared_ptr<storage::AsyncIndexEntryReader>>(
                              std::get<PackedIndexSource>(input).reader);
    }
    const auto priority = Priority(request.options.op_ctx);
    auto task = LoadIndexAsync(loader, std::move(request));
    if (!use_async)
        return folly::coro::blockingWait(std::move(task));
    return folly::coro::blockingWait(folly::coro::co_withExecutor(
        storage::ResolveAsyncLoadExecutor({}, priority), std::move(task)));
}

}  // namespace milvus::index
