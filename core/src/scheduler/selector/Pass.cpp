// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#ifdef MILVUS_GPU_VERSION
#include "src/scheduler/selector/Pass.h"
#include <iostream>
#include <string>
#include <vector>
#include "cache/GpuCacheMgr.h"
#include "scheduler/resource/Resource.h"
#include "scheduler/task/SearchTask.h"
#include "src/scheduler/SchedInst.h"
#include "src/utils/Log.h"

namespace milvus {
namespace scheduler {

int64_t
FindProperDevice(const std::vector<int64_t>& device_ids, const std::string& key) {
    for (auto& device_id : device_ids) {
        auto gpu_cache = milvus::cache::GpuCacheMgr::GetInstance(device_id);
        if (gpu_cache->ItemExists(key))
            return device_id;
    }
    return -1;
}

ResourcePtr
PickResource(const TaskPtr& task, const std::vector<int64_t>& device_ids, int64_t& idx, std::string name) {
    auto search_task = std::static_pointer_cast<XSearchTask>(task);
    auto did = FindProperDevice(device_ids, search_task->GetLocation());
    ResourcePtr res_ptr = nullptr;
    if (did < 0) {
        LOG_SERVER_DEBUG_ << "No cache hit on gpu devices";
        LOG_SERVER_DEBUG_ << LogOut("%s: nq >= gpu_search_threshold, specify gpu %d to search!", name.c_str(),
                                    device_ids[idx]);
        res_ptr = scheduler::ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, (uint64_t)device_ids[idx]);
        idx = (idx + 1) % device_ids.size();
    } else {
        LOG_SERVER_DEBUG_ << LogOut("Gpu cache hit on device %d", did);
        LOG_SERVER_DEBUG_ << LogOut("%s: nq >= gpu_search_threshold, specify gpu %d to search!", name.c_str(), did);
        res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, (uint64_t)did);
    }
    return res_ptr;
}

}  // namespace scheduler
}  // namespace milvus
#endif
