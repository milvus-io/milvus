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

#pragma once

#include "BuildMgr.h"
#include "CPUBuilder.h"
#include "JobMgr.h"
#include "ResourceMgr.h"
#include "Scheduler.h"
#include "Utils.h"
#include "selector/BuildIndexPass.h"
#include "selector/FaissFlatPass.h"
#include "selector/FaissIVFFlatPass.h"
#include "selector/FaissIVFPQPass.h"
#include "selector/FaissIVFSQ8HPass.h"
#include "selector/FaissIVFSQ8Pass.h"
#include "selector/FallbackPass.h"
#include "selector/Optimizer.h"

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace milvus {
namespace scheduler {

class ResMgrInst {
 public:
    static ResourceMgrPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<ResourceMgr>();
            }
        }
        return instance;
    }

 private:
    static ResourceMgrPtr instance;
    static std::mutex mutex_;
};

class SchedInst {
 public:
    static SchedulerPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<Scheduler>(ResMgrInst::GetInstance());
            }
        }
        return instance;
    }

 private:
    static SchedulerPtr instance;
    static std::mutex mutex_;
};

class JobMgrInst {
 public:
    static scheduler::JobMgrPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<scheduler::JobMgr>(ResMgrInst::GetInstance());
            }
        }
        return instance;
    }

 private:
    static scheduler::JobMgrPtr instance;
    static std::mutex mutex_;
};

class OptimizerInst {
 public:
    static OptimizerPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                std::vector<PassPtr> pass_list;
#ifdef MILVUS_GPU_VERSION
                bool enable_gpu = false;
                server::Config& config = server::Config::GetInstance();
                config.GetGpuResourceConfigEnable(enable_gpu);
                if (enable_gpu) {
                    std::vector<int64_t> build_gpus;
                    std::vector<int64_t> search_gpus;
                    int64_t gpu_search_threshold;
                    config.GetGpuResourceConfigBuildIndexResources(build_gpus);
                    config.GetGpuResourceConfigSearchResources(search_gpus);
                    config.GetEngineConfigGpuSearchThreshold(gpu_search_threshold);
                    std::string build_msg = "Build index gpu:";
                    for (auto build_id : build_gpus) {
                        build_msg.append(" gpu" + std::to_string(build_id));
                    }
                    LOG_SERVER_DEBUG_ << LogOut("[%s][%d] %s", "search", 0, build_msg.c_str());

                    std::string search_msg = "Search gpu:";
                    for (auto search_id : search_gpus) {
                        search_msg.append(" gpu" + std::to_string(search_id));
                    }
                    search_msg.append(". gpu_search_threshold:" + std::to_string(gpu_search_threshold));
                    LOG_SERVER_DEBUG_ << LogOut("[%s][%d] %s", "search", 0, build_msg.c_str());

                    pass_list.push_back(std::make_shared<BuildIndexPass>());
                    pass_list.push_back(std::make_shared<FaissFlatPass>());
                    pass_list.push_back(std::make_shared<FaissIVFFlatPass>());
                    pass_list.push_back(std::make_shared<FaissIVFSQ8Pass>());
                    pass_list.push_back(std::make_shared<FaissIVFSQ8HPass>());
                    pass_list.push_back(std::make_shared<FaissIVFPQPass>());
                }
#endif
                pass_list.push_back(std::make_shared<FallbackPass>());
                instance = std::make_shared<Optimizer>(pass_list);
            }
        }
        return instance;
    }

 private:
    static scheduler::OptimizerPtr instance;
    static std::mutex mutex_;
};

class BuildMgrInst {
 public:
    static BuildMgrPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<BuildMgr>(4);
            }
        }
        return instance;
    }

 private:
    static BuildMgrPtr instance;
    static std::mutex mutex_;
};

class CPUBuilderInst {
 public:
    static CPUBuilderPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<CPUBuilder>();
            }
        }
        return instance;
    }

 private:
    static CPUBuilderPtr instance;
    static std::mutex mutex_;
};

void
StartSchedulerService();

void
StopSchedulerService();

}  // namespace scheduler
}  // namespace milvus
