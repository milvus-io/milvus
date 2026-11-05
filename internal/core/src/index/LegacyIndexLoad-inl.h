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

#include "folly/ScopeGuard.h"
#include "common/OpContext.h"
#include "folly/coro/WithCancellation.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/EntryStreamUtils.h"

namespace milvus::index {

template <typename Result>
folly::coro::Task<Result>
RunLegacyLoad(LegacyIndexSource& source,
              const storage::LoadOptions& fixed_options,
              std::type_identity_t<LegacyLoad<Result>> load,
              milvus::OpContext* context) {
    auto options = fixed_options;
    options.op_ctx = context;
    const auto priority =
        options.op_ctx && options.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    auto set_context = [](storage::FileSource& source,
                          proto::common::LoadPriority priority,
                          folly::CancellationToken token) {
        if (auto* remote = dynamic_cast<storage::V1RemoteSource*>(&source)) {
            remote->SetLoadContext(priority, std::move(token));
        }
    };
    const auto operation_token =
        context ? context->cancellation_token : folly::CancellationToken{};
    const auto token = folly::cancellation_token_merge(
        operation_token, co_await folly::coro::co_current_cancellation_token);
    auto run = [&]() -> folly::coro::Task<Result> {
        storage::ThrowIfCancelled(token, "load legacy index");
        set_context(*source.source, priority, token);
        // Detach this call's token on both success and failure so a cancelled
        // attempt cannot poison a later sequential Load on the same source.
        auto reset = folly::makeGuard([&] {
            set_context(*source.source, proto::common::LoadPriority::HIGH, {});
        });
        auto reader = co_await load(*source.source, options, source.use_async);
        AssertInfo(reader != nullptr, "Legacy loader returned null result");
        storage::ThrowIfCancelled(token, "publish legacy index");
        co_return reader;
    };
    // Sync stays on the caller. Families offload their blocking file phases;
    // orchestration and CPU decoding use the general loading executor.
    if (!source.use_async) {
        co_return co_await run();
    }
    co_return co_await folly::coro::co_withCancellation(
        token,
        folly::coro::co_withExecutor(
            storage::ResolveAsyncLoadExecutor({}, priority), run()));
}

}  // namespace milvus::index
