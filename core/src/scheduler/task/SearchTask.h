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

#include <memory>
#include <vector>

#include "Task.h"
#include "scheduler/Definition.h"
#include "scheduler/job/SearchJob.h"

namespace milvus {
namespace scheduler {

// TODO(wxyu): rewrite
class XSearchTask : public Task {
 public:
    explicit XSearchTask(const std::shared_ptr<server::Context>& context, TableFileSchemaPtr file, TaskLabelPtr label);

    void
    Load(LoadType type, uint8_t device_id) override;

    void
    Execute() override;

 public:
    static void
    MergeTopkToResultSet(const scheduler::ResultIds& src_ids, const scheduler::ResultDistances& src_distances,
                         size_t src_k, size_t nq, size_t topk, bool ascending, scheduler::ResultIds& tar_ids,
                         scheduler::ResultDistances& tar_distances);

    //    static void
    //    MergeTopkArray(std::vector<int64_t>& tar_ids, std::vector<float>& tar_distance, uint64_t& tar_input_k,
    //                   const std::vector<int64_t>& src_ids, const std::vector<float>& src_distance, uint64_t
    //                   src_input_k, uint64_t nq, uint64_t topk, bool ascending);

 public:
    const std::shared_ptr<server::Context> context_;

    TableFileSchemaPtr file_;

    size_t index_id_ = 0;
    int index_type_ = 0;
    ExecutionEnginePtr index_engine_ = nullptr;
    bool metric_l2 = true;
};

}  // namespace scheduler
}  // namespace milvus
