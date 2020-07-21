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
#include <vector>

#include "job/BuildIndexJob.h"
#include "job/DeleteJob.h"
#include "job/SSBuildIndexJob.h"
#include "job/SSSearchJob.h"
#include "job/SearchJob.h"
#include "task/Task.h"

namespace milvus {
namespace scheduler {

class TaskCreator {
 public:
    static std::vector<TaskPtr>
    Create(const JobPtr& job);

 public:
    static std::vector<TaskPtr>
    Create(const SearchJobPtr& job);

    static std::vector<TaskPtr>
    Create(const DeleteJobPtr& job);

    static std::vector<TaskPtr>
    Create(const BuildIndexJobPtr& job);

    static std::vector<TaskPtr>
    Create(const SSSearchJobPtr& job);

    static std::vector<TaskPtr>
    Create(const SSBuildIndexJobPtr& job);
};

}  // namespace scheduler
}  // namespace milvus
