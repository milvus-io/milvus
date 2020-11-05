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
#pragma once

#include <condition_variable>
#include <deque>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "scheduler/selector/Pass.h"
#include "value/config/ConfigMgr.h"

namespace milvus {
namespace scheduler {

class FaissFlatPass : public Pass, public ConfigObserver {
 public:
    FaissFlatPass();

    ~FaissFlatPass();

 public:
    void
    Init() override;

    bool
    Run(const TaskPtr& task) override;

 public:
    void
    ConfigUpdate(const std::string& name) override;

 private:
    int64_t idx_ = 0;
    bool gpu_enable_ = false;
    int64_t threshold_ = 0;
    std::vector<int64_t> search_gpus_;
};

using FaissFlatPassPtr = std::shared_ptr<FaissFlatPass>;

}  // namespace scheduler
}  // namespace milvus
#endif
