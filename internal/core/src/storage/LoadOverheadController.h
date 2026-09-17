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

#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>

#include "cachinglayer/LoadingOverhead.h"

namespace milvus::storage {

template <cachinglayer::LoadingOverheadDimension Dimension>
class LoadOverheadController {
 public:
    static LoadOverheadController&
    GetInstance();

    // Returns the async Group, bounded by admission bytes/slots.
    // Its identity survives configuration changes and starts no workers.
    cachinglayer::LoadingOverheadGroupHandle
    GetOrCreate();

    // Returns a separate sync Group bounded by HIGH + LOW workers. The initial
    // count only bootstraps the policy; later calls cannot overwrite a resize.
    cachinglayer::LoadingOverheadGroupHandle
    GetOrCreateForSync(int64_t initial_executor_workers);

    // Called by ThreadPools before expansion and after shrink; never updates
    // the async Group, including while sync and async translators coexist.
    bool
    UpdateExecutorWorkers(int64_t executor_workers);

    bool
    UpdateBudgetBytes(size_t bytes)
        requires(Dimension == cachinglayer::LoadingOverheadDimension::kMemory);

    // Updates the fallback concurrency bound. Zero means no slot bound.
    // Called by LoadAdmissionController with serialized capacity updates.
    bool
    UpdateAdmissionSlots(size_t slots);

 private:
    LoadOverheadController() = default;

    cachinglayer::LoadingOverheadPolicy
    CurrentPolicy() const;

    static cachinglayer::LoadingOverheadPolicy
    SlotPolicy(size_t slots);

    std::mutex mutex_;
    cachinglayer::LoadingOverheadGroupHandle group_handle_;
    cachinglayer::LoadingOverheadGroupHandle sync_group_handle_;
    int64_t executor_workers_{-1};
    size_t budget_bytes_{0};
    size_t admission_slots_{0};
};

extern template class LoadOverheadController<
    cachinglayer::LoadingOverheadDimension::kMemory>;
extern template class LoadOverheadController<
    cachinglayer::LoadingOverheadDimension::kFile>;

using LoadMemoryOverheadController =
    LoadOverheadController<cachinglayer::LoadingOverheadDimension::kMemory>;
using LoadFileOverheadController =
    LoadOverheadController<cachinglayer::LoadingOverheadDimension::kFile>;

}  // namespace milvus::storage
