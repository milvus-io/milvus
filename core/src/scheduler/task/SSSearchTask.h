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

#include <memory>
#include <string>
#include <vector>

#include "db/SnapshotVisitor.h"
#include "db/engine/SSExecutionEngine.h"
#include "scheduler/Definition.h"
#include "scheduler/job/SSSearchJob.h"
#include "scheduler/task/Task.h"

namespace milvus {
namespace scheduler {

class SSSearchTask : public Task {
 public:
    explicit SSSearchTask(const server::ContextPtr& context, const engine::DBOptions& options,
                          const query::QueryPtr& query_ptr, engine::snapshot::ID_TYPE segment_id, TaskLabelPtr label);

    void
    Load(LoadType type, uint8_t device_id) override;

    void
    Execute() override;

 private:
    void
    CreateExecEngine();

 public:
    const std::shared_ptr<server::Context> context_;

    const engine::DBOptions& options_;
    query::QueryPtr query_ptr_;
    engine::snapshot::ID_TYPE segment_id_;

    engine::SSExecutionEnginePtr execution_engine_;
};

}  // namespace scheduler
}  // namespace milvus
