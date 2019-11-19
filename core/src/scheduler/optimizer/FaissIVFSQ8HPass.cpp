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

#include "scheduler/optimizer/FaissIVFSQ8HPass.h"
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
FaissIVFSQ8HPass::Init() {
    server::Config& config = server::Config::GetInstance();
    Status s = config.GetEngineConfigGpuSearchThreshold(threshold_);
    if (!s.ok()) {
        threshold_ = std::numeric_limits<int32_t>::max();
    }
    s = config.GetGpuResourceConfigSearchResources(gpus);
}

bool
FaissIVFSQ8HPass::Run(const TaskPtr& task) {
    if (task->Type() != TaskType::SearchTask) {
        return false;
    }

    auto search_task = std::static_pointer_cast<XSearchTask>(task);
    if (search_task->file_->engine_type_ != (int)engine::EngineType::FAISS_IVFSQ8H) {
        return false;
    }

    auto search_job = std::static_pointer_cast<SearchJob>(search_task->job_.lock());
    ResourcePtr res_ptr;
    if (search_job->nq() < threshold_) {
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else {
        auto best_device_id = count_ % gpus.size();
        count_++;
        res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, best_device_id);
    }
    auto label = std::make_shared<SpecResLabel>(res_ptr);
    task->label() = label;
    return true;
}

}  // namespace scheduler
}  // namespace milvus
