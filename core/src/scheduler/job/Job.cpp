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

#include "scheduler/job/Job.h"

namespace milvus {
namespace scheduler {

namespace {
std::mutex unique_job_mutex;
uint64_t unique_job_id = 0;
}  // namespace

Job::Job(JobType type) : type_(type) {
    std::lock_guard<std::mutex> lock(unique_job_mutex);
    id_ = unique_job_id++;
}

json
Job::Dump() const {
    json ret{
        {"id", id_},
        {"type", type_},
    };
    return ret;
}

}  // namespace scheduler
}  // namespace milvus
