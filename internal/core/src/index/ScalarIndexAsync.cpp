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

#include "common/Types.h"
#include "index/Utils.h"
#include "storage/AsyncIndexEntryReader.h"
#include "storage/IndexMaterializer.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {

template <typename T>
folly::coro::Task<void>
ScalarIndex<T>::LoadUnifiedAsync(const std::string& packed_file,
                                 const Config& config,
                                 proto::common::LoadPriority load_priority,
                                 folly::CancellationToken cancellation_token) {
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
    FinalizeLoad(std::move(artifact), config);
    artifact.CommitTargets();
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
