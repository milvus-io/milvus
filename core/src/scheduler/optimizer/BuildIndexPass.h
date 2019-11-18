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
#include <vector>

#include "Pass.h"

namespace milvus {
namespace scheduler {

class BuildIndexPass : public Pass {
 public:
    explicit BuildIndexPass(std::vector<int64_t>& build_gpu_id);

 public:
    bool
    Run(const TaskPtr& task) override;

 private:
    uint64_t specified_gpu_id_ = 0;
    std::vector<int64_t> build_gpu_ids_;
};

using BuildIndexPassPtr = std::shared_ptr<BuildIndexPass>;

}  // namespace scheduler
}  // namespace milvus
