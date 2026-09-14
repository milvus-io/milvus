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

#include "storage/LoadOverheadController.h"

#include <limits>

#include "cachinglayer/Manager.h"
#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus::storage {

namespace {

bool
UpdateGroupPolicy(const cachinglayer::LoadingOverheadGroupHandle& group_handle,
                  const cachinglayer::LoadingOverheadPolicy& policy,
                  const char* resource_name) {
    if (group_handle == nullptr) {
        return true;
    }
    auto result =
        cachinglayer::Manager::UpdateLoadingOverheadGroup(group_handle, policy);
    if (result != cachinglayer::LoadingOverheadUpdateResult::kApplied) {
        LOG_ERROR("Failed to update load {} loading overhead group",
                  resource_name);
        return false;
    }
    return true;
}

template <cachinglayer::LoadingOverheadDimension Dimension>
constexpr const char*
ResourceName() {
    if constexpr (Dimension ==
                  cachinglayer::LoadingOverheadDimension::kMemory) {
        return "memory";
    }
    return "file";
}

}  // namespace

template <cachinglayer::LoadingOverheadDimension Dimension>
LoadOverheadController<Dimension>&
LoadOverheadController<Dimension>::GetInstance() {
    static LoadOverheadController instance;
    return instance;
}

template <cachinglayer::LoadingOverheadDimension Dimension>
cachinglayer::LoadingOverheadPolicy
LoadOverheadController<Dimension>::CurrentPolicy() const {
    if constexpr (Dimension ==
                  cachinglayer::LoadingOverheadDimension::kMemory) {
        if (budget_bytes_ != 0) {
            return cachinglayer::LoadingOverheadPolicy::Budget(
                static_cast<int64_t>(budget_bytes_));
        }
    }
    return SlotPolicy(admission_slots_);
}

// The existing multiplicative policy counts admitted runtime units here, not
// CPU workers: suspended async reads keep their slot but release the worker.
template <cachinglayer::LoadingOverheadDimension Dimension>
cachinglayer::LoadingOverheadPolicy
LoadOverheadController<Dimension>::SlotPolicy(const size_t slots) {
    if (slots == 0 ||
        slots > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        return cachinglayer::LoadingOverheadPolicy::Passthrough();
    }
    return cachinglayer::LoadingOverheadPolicy::Executor(
        static_cast<int64_t>(slots));
}

template <cachinglayer::LoadingOverheadDimension Dimension>
cachinglayer::LoadingOverheadGroupHandle
LoadOverheadController<Dimension>::GetOrCreate() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (group_handle_ == nullptr) {
        group_handle_ = cachinglayer::Manager::CreateLoadingOverheadGroup(
            Dimension, CurrentPolicy());
        AssertInfo(group_handle_ != nullptr,
                   "Failed to create load {} overhead group",
                   ResourceName<Dimension>());
    }
    return group_handle_;
}

template <cachinglayer::LoadingOverheadDimension Dimension>
bool
LoadOverheadController<Dimension>::UpdateBudgetBytes(size_t bytes)
    requires(Dimension == cachinglayer::LoadingOverheadDimension::kMemory)
{
    std::lock_guard<std::mutex> lock(mutex_);
    AssertInfo(
        bytes <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "Load memory budget bytes exceed the loading-overhead policy range");
    if (bytes == budget_bytes_) {
        return true;
    }
    const auto policy = bytes == 0
                            ? SlotPolicy(admission_slots_)
                            : cachinglayer::LoadingOverheadPolicy::Budget(
                                  static_cast<int64_t>(bytes));
    if (!UpdateGroupPolicy(group_handle_, policy, ResourceName<Dimension>())) {
        return false;
    }
    budget_bytes_ = bytes;
    return true;
}

template <cachinglayer::LoadingOverheadDimension Dimension>
bool
LoadOverheadController<Dimension>::UpdateAdmissionSlots(const size_t slots) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (slots == admission_slots_) {
        return true;
    }
    if (budget_bytes_ == 0 &&
        !UpdateGroupPolicy(
            group_handle_, SlotPolicy(slots), ResourceName<Dimension>())) {
        return false;
    }
    admission_slots_ = slots;
    return true;
}

template class LoadOverheadController<
    cachinglayer::LoadingOverheadDimension::kMemory>;
template class LoadOverheadController<
    cachinglayer::LoadingOverheadDimension::kFile>;

}  // namespace milvus::storage
