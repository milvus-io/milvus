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

#include "Task.h"
#include "scheduler/Definition.h"
#include "scheduler/job/SearchJob.h"

#include <vector>

namespace milvus {
namespace scheduler {

// TODO(wxyu): rewrite
class XSearchTask : public Task {
 public:
    explicit XSearchTask(TableFileSchemaPtr file, TaskLabelPtr label);

    void
    Load(LoadType type, uint8_t device_id) override;

    void
    Execute() override;

 public:
    static Status
    TopkResult(const std::vector<long> &input_ids,
               const std::vector<float> &input_distance,
               uint64_t input_k,
               uint64_t nq,
               uint64_t topk,
               bool ascending,
               scheduler::ResultSet &result);

 public:
    TableFileSchemaPtr file_;

    size_t index_id_ = 0;
    int index_type_ = 0;
    ExecutionEnginePtr index_engine_ = nullptr;
    bool metric_l2 = true;

    static std::mutex merge_mutex_;
};

}  // namespace scheduler
}  // namespace milvus
