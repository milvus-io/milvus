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
#include "storage/IndexMaterializer.h"
#include "storage/LocalFileIOPool.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {

template <typename T>
folly::coro::Task<void>
ScalarIndex<T>::LoadLegacyAsync(const Config& config,
                                folly::CancellationToken token) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    storage::ThrowIfCancelled(token, "ScalarIndex::LoadLegacy");
    AssertInfo(file_manager_ != nullptr, "Legacy load requires a file manager");
    const auto files = config.at(INDEX_FILES).get<std::vector<std::string>>();
    const auto priority = GetValueFromConfig<proto::common::LoadPriority>(
                              config, milvus::LOAD_PRIORITY)
                              .value_or(proto::common::LoadPriority::HIGH);
    auto binary =
        co_await file_manager_->LoadIndexBinarySetAsync(files, priority, token);
    co_await FinishLegacyLoadAsync(std::move(binary), config, token);
}

template <typename T>
folly::coro::Task<void>
ScalarIndex<T>::FinishLegacyLoadAsync(BinarySet binary,
                                      const Config& config,
                                      folly::CancellationToken token) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    storage::ThrowIfCancelled(token, "ScalarIndex::FinalizeLegacy");
    LoadWithoutAssemble(binary, config);
    storage::ThrowIfCancelled(token, "ScalarIndex::FinalizeLegacy");
    co_return;
}

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
    // Borrow the artifact during engine work. Its files and directory leases
    // survive every awaited operation, then leave scope on the local executor.
    std::exception_ptr failure;
    try {
        storage::ThrowIfCancelled(cancellation_token,
                                  "ScalarIndex::FinalizeLoad");
        co_await folly::coro::co_withCancellation(
            cancellation_token, FinalizeLoad(std::move(artifact), config));
    } catch (...) {
        failure = std::current_exception();
    }
    auto release = [&] {
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
ScalarIndex<bool>::LoadLegacyAsync(const Config&, folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<bool>::FinishLegacyLoadAsync(BinarySet,
                                         const Config&,
                                         folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int8_t>::LoadLegacyAsync(const Config&, folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int8_t>::FinishLegacyLoadAsync(BinarySet,
                                           const Config&,
                                           folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int16_t>::LoadLegacyAsync(const Config&, folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int16_t>::FinishLegacyLoadAsync(BinarySet,
                                            const Config&,
                                            folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int32_t>::LoadLegacyAsync(const Config&, folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int32_t>::FinishLegacyLoadAsync(BinarySet,
                                            const Config&,
                                            folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int64_t>::LoadLegacyAsync(const Config&, folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<int64_t>::FinishLegacyLoadAsync(BinarySet,
                                            const Config&,
                                            folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<uint64_t>::LoadLegacyAsync(const Config&, folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<uint64_t>::FinishLegacyLoadAsync(BinarySet,
                                             const Config&,
                                             folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<float>::LoadLegacyAsync(const Config&, folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<float>::FinishLegacyLoadAsync(BinarySet,
                                          const Config&,
                                          folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<double>::LoadLegacyAsync(const Config&, folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<double>::FinishLegacyLoadAsync(BinarySet,
                                           const Config&,
                                           folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<std::string>::LoadLegacyAsync(const Config&,
                                          folly::CancellationToken);
template folly::coro::Task<void>
ScalarIndex<std::string>::FinishLegacyLoadAsync(BinarySet,
                                                const Config&,
                                                folly::CancellationToken);

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
