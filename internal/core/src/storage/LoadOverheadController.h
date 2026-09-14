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

    // Returns the same Group across budget, slot and executor changes.
    // LoadAdmissionController supplies the limits; creation starts no workers.
    cachinglayer::LoadingOverheadGroupHandle
    GetOrCreate();

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
