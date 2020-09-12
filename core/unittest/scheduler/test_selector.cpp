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
#include <fiu-local.h>
#include <fiu-control.h>
#include <gtest/gtest.h>

#include "scheduler/task/BuildIndexTask.h"
#include "scheduler/task/SearchTask.h"
#include "scheduler/SchedInst.h"
#include "scheduler/resource/CpuResource.h"
#include "scheduler/selector/BuildIndexPass.h"
#include "scheduler/selector/FaissFlatPass.h"
#include "scheduler/selector/FaissIVFFlatPass.h"
#include "scheduler/selector/FaissIVFPQPass.h"
#include "scheduler/selector/FaissIVFSQ8HPass.h"
#include "scheduler/selector/FaissIVFSQ8Pass.h"
#include "scheduler/selector/FallbackPass.h"

namespace milvus {
namespace scheduler {

#ifdef MILVUS_GPU_VERSION
TEST(OptimizerTest, TEST_OPTIMIZER) {
    BuildIndexPass build_index_pass;
    fiu_init(0);
    fiu_enable("BuildIndexPass.Init.get_config_fail", 1, NULL, 0);
    ASSERT_ANY_THROW(build_index_pass.Init(););
    fiu_disable("BuildIndexPass.Init.get_config_fail");

    auto build_index_task = std::make_shared<XBuildIndexTask>(nullptr, nullptr);
    fiu_enable("BuildIndexPass.Run.empty_gpu_ids", 1, NULL, 0);
    ASSERT_FALSE(build_index_pass.Run(build_index_task));
    fiu_disable("BuildIndexPass.Run.empty_gpu_ids");

    FaissFlatPass faiss_flat_pass;
    fiu_enable("check_config_gpu_search_threshold_fail", 1, NULL, 0);
    fiu_enable("get_gpu_config_search_resources.disable_gpu_resource_fail", 1, NULL, 0);
    ASSERT_ANY_THROW(faiss_flat_pass.Init(););
    fiu_disable("get_gpu_config_search_resources.disable_gpu_resource_fail");
    fiu_disable("check_config_gpu_search_threshold_fail");

    FaissIVFFlatPass faiss_ivf_flat_pass;
    fiu_enable("check_config_gpu_search_threshold_fail", 1, NULL, 0);
    fiu_enable("get_gpu_config_search_resources.disable_gpu_resource_fail", 1, NULL, 0);
    ASSERT_ANY_THROW(faiss_ivf_flat_pass.Init(););
    fiu_disable("get_gpu_config_search_resources.disable_gpu_resource_fail");
    fiu_disable("check_config_gpu_search_threshold_fail");

    FaissIVFPQPass faiss_ivf_pq_pass;
    fiu_enable("check_config_gpu_search_threshold_fail", 1, NULL, 0);
    fiu_enable("get_gpu_config_search_resources.disable_gpu_resource_fail", 1, NULL, 0);
    ASSERT_ANY_THROW(faiss_ivf_pq_pass.Init(););
    fiu_disable("get_gpu_config_search_resources.disable_gpu_resource_fail");
    fiu_disable("check_config_gpu_search_threshold_fail");

    auto file = std::make_shared<SegmentSchema>();
    file->engine_type_ = (int)engine::EngineType::FAISS_IVFFLAT;
    file->index_params_ = "{ \"nlist\": 100 }";
    file->dimension_ = 64;
    auto search_task = std::make_shared<XSearchTask>(nullptr, file, nullptr);
    ASSERT_FALSE(faiss_ivf_pq_pass.Run(search_task));

    FaissIVFSQ8HPass faiss_ivf_q8h_pass;
    fiu_enable("check_config_gpu_search_threshold_fail", 1, NULL, 0);
    faiss_ivf_q8h_pass.Init();
    fiu_disable("check_config_gpu_search_threshold_fail");

    auto search_task2 = std::make_shared<XSearchTask>(nullptr, file, nullptr);
    ASSERT_FALSE(faiss_ivf_q8h_pass.Run(build_index_task));
    ASSERT_FALSE(faiss_ivf_q8h_pass.Run(search_task2));

    FaissIVFSQ8Pass faiss_ivf_q8_pass;
    fiu_enable("check_config_gpu_search_threshold_fail", 1, NULL, 0);
    fiu_enable("get_gpu_config_search_resources.disable_gpu_resource_fail", 1, NULL, 0);
    ASSERT_ANY_THROW(faiss_ivf_q8_pass.Init(););
    fiu_disable("get_gpu_config_search_resources.disable_gpu_resource_fail");
    fiu_disable("check_config_gpu_search_threshold_fail");

    FallbackPass fall_back_pass;
    fall_back_pass.Init();
    auto task = std::make_shared<XBuildIndexTask>(nullptr, nullptr);
    ResMgrInst::GetInstance()->Add(std::make_shared<CpuResource>("name1", 1, true));
    fall_back_pass.Run(task);
}

#endif

}  // namespace scheduler
}  // namespace milvus
