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
#include <fiu/fiu-local.h>

#include "knowhere/index/IndexType.h"
#include "scheduler/SchedInst.h"
#include "scheduler/Utils.h"
#include "scheduler/selector/BuildIndexPass.h"
#include "scheduler/task/BuildIndexTask.h"
#ifdef MILVUS_GPU_VERSION
namespace milvus {
namespace scheduler {

void
BuildIndexPass::Init() {
    gpu_enable_ = config.gpu.enable();
    build_gpus_ = ParseGPUDevices(config.gpu.build_index_devices());
    cpu_type_list_ = {knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP,
                      knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                      knowhere::IndexEnum::INDEX_NSG,
#ifdef MILVUS_SUPPORT_SPTAG
                      knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT,
                      knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT,
#endif
                      knowhere::IndexEnum::INDEX_HNSW,
                      knowhere::IndexEnum::INDEX_ANNOY,
                      knowhere::IndexEnum::INDEX_RHNSWFlat,
                      knowhere::IndexEnum::INDEX_RHNSWPQ,
                      knowhere::IndexEnum::INDEX_RHNSWSQ};
}

bool
BuildIndexPass::isCPUIndex(const std::string& type) {
    if (cpu_type_list_.find(type) == cpu_type_list_.end()) {
        /* if not found in cpu_list then gpu*/
        return false;
    } else {
        return true;
    }
}

bool
BuildIndexPass::Run(const TaskPtr& task) {
    try {
        if (task->Type() != TaskType::BuildIndexTask) {
            return false;
        }

        auto build_index_task = std::dynamic_pointer_cast<BuildIndexTask>(task);
        auto type = build_index_task->GetIndexType();
        LOG_SERVER_DEBUG_ << "BuildIndexPass Build index type is " << type;

        ResourcePtr res_ptr;
        if (!gpu_enable_ || isCPUIndex(type)) {
            LOG_SERVER_DEBUG_ << "Gpu disabled or isCPUIndex, specify cpu to build index!";
            res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
        } else {
            fiu_do_on("BuildIndexPass.Run.empty_gpu_ids", build_gpus_.clear());
            if (build_gpus_.empty()) {
                LOG_SERVER_WARNING_ << "BuildIndexPass cannot get build index gpu!";
                return false;
            }
            LOG_SERVER_DEBUG_ << "Specify gpu" << build_gpus_[idx_] << " to build index!";
            res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, build_gpus_[idx_]);
            idx_ = (idx_ + 1) % build_gpus_.size();
        }

        task->resource() = res_ptr;

        return true;
    } catch (std::exception ex) {
        std::string str = "Milvus server encounter exception: " + std::string(ex.what());
        return false;
    }
}

void
BuildIndexPass::ValueUpdate(const std::string& name) {
}

}  // namespace scheduler
}  // namespace milvus
#endif
