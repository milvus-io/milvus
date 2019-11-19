// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "scheduler/optimizer/LargeSQ8HPass.h"
#include "cache/GpuCacheMgr.h"
#include "scheduler/SchedInst.h"
#include "scheduler/Utils.h"
#include "scheduler/task/SearchTask.h"
#include "scheduler/tasklabel/SpecResLabel.h"
#include "server/Config.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

void
LargeSQ8HPass::Init() {
    server::Config& config = server::Config::GetInstance();
    Status s = config.GetEngineConfigGpuSearchThreshold(threshold_);
    if (!s.ok()) {
        threshold_ = std::numeric_limits<int32_t>::max();
    }
    s = config.GetGpuResourceConfigSearchResources(gpus);
}

bool
LargeSQ8HPass::Run(const TaskPtr& task) {
    if (task->Type() != TaskType::SearchTask) {
        return false;
    }

    auto search_task = std::static_pointer_cast<XSearchTask>(task);
    if (search_task->file_->engine_type_ != (int)engine::EngineType::FAISS_IVFSQ8H) {
        return false;
    }

    auto search_job = std::static_pointer_cast<SearchJob>(search_task->job_.lock());

    // TODO: future, Index::IVFSQ8H, if nq < threshold set cpu, else set gpu

    if (search_job->nq() < threshold_) {
        return false;
    }

    //    std::vector<int64_t> all_free_mem;
    //    for (auto& gpu : gpus) {
    //        auto cache = cache::GpuCacheMgr::GetInstance(gpu);
    //        auto free_mem = cache->CacheCapacity() - cache->CacheUsage();
    //        all_free_mem.push_back(free_mem);
    //    }
    //
    //    auto max_e = std::max_element(all_free_mem.begin(), all_free_mem.end());
    //    auto best_index = std::distance(all_free_mem.begin(), max_e);
    //    auto best_device_id = gpus[best_index];
    auto best_device_id = count_ % gpus.size();
    count_++;

    ResourcePtr res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, best_device_id);
    if (not res_ptr) {
        SERVER_LOG_ERROR << "GpuResource " << best_device_id << " invalid.";
        // TODO: throw critical error and exit
        return false;
    }

    auto label = std::make_shared<SpecResLabel>(std::weak_ptr<Resource>(res_ptr));
    task->label() = label;

    return true;
}

}  // namespace scheduler
}  // namespace milvus
