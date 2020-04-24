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

#include <condition_variable>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Pass.h"

namespace milvus {
namespace scheduler {

class Optimizer {
 public:
    explicit Optimizer(std::vector<PassPtr> pass_list) : pass_list_(std::move(pass_list)) {
    }

    void
    Init();

    bool
    Run(const TaskPtr& task);

 private:
    std::vector<PassPtr> pass_list_;
};

using OptimizerPtr = std::shared_ptr<Optimizer>;

}  // namespace scheduler
}  // namespace milvus
