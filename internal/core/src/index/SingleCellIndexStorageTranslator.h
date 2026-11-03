// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <functional>
#include "folly/coro/BlockingWait.h"
#include "index/IndexCacheConfig.h"
#include "index/IndexLoadContext.h"
#include "storage/AsyncLoadExecutor.h"

namespace milvus::index {

/** @brief Optional adapter for indexes loaded as one whole storage cell.
 * The callback owns its source/options; indexes with their own loading bypass this.
 * @note The slot serializes reloads. Sources must not be shared with other slots.
 */
template <typename Storage>
class SingleCellIndexStorageTranslator final
    : public cachinglayer::Translator<Storage> {
 public:
    using LoadFn =
        std::function<folly::coro::Task<std::unique_ptr<Storage>>(OpContext*)>;

    SingleCellIndexStorageTranslator(IndexCacheConfig config,
                                     bool use_async,
                                     LoadFn load)
        : config_(std::move(config)),
          use_async_(use_async),
          load_(std::move(load)) {
    }

    size_t
    num_cells() const override {
        return 1;
    }
    cachinglayer::cid_t
    cell_id_of(cachinglayer::uid_t) const override {
        return 0;
    }
    cachinglayer::LoadingResourceUsage
    estimated_loading_usage(
        const std::vector<cachinglayer::cid_t>& cids) const override {
        if (cids.empty()) {
            return {};
        }
        return {config_.loaded_usage,
                config_.loading_overhead,
                config_.shared_loading_overhead};
    }
    const std::string&
    key() const override {
        return config_.key;
    }
    cachinglayer::Meta*
    meta() override {
        return &config_.meta;
    }
    int64_t
    cells_storage_bytes(
        const std::vector<cachinglayer::cid_t>&) const override {
        return config_.storage_bytes;
    }
    std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<Storage>>>
    get_cells(OpContext* parent,
              const std::vector<cachinglayer::cid_t>& cids) override {
        AssertInfo(cids.size() == 1 && cids.front() == 0,
                   "single-cell index received invalid cell ids");
        IndexLoadContext context(parent, config_.load_priority);
        auto task = load_(&context.ctx);
        auto storage =
            use_async_
                ? folly::coro::blockingWait(folly::coro::co_withExecutor(
                      storage::ResolveAsyncLoadExecutor(
                          {},
                          context.ctx.runtime_load_priority.value_or(0) != 0
                              ? proto::common::LoadPriority::LOW
                              : proto::common::LoadPriority::HIGH),
                      std::move(task)))
                : folly::coro::blockingWait(std::move(task));
        AssertInfo(storage != nullptr, "index storage loader returned null");
        std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<Storage>>>
            result;
        result.emplace_back(0, std::move(storage));
        return result;
    }

 private:
    IndexCacheConfig config_;
    const bool use_async_;
    LoadFn load_;
};

}  // namespace milvus::index
