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

#include <string>

#include "db/engine/ExecutionEngine.h"
#include "db/snapshot/ResourceTypes.h"
#include "scheduler/Definition.h"
#include "scheduler/job/BuildIndexJob.h"
#include "scheduler/task/Task.h"

namespace milvus {
namespace scheduler {

class BuildIndexTask : public Task {
 public:
    explicit BuildIndexTask(const engine::DBOptions& options, const std::string& collection_name,
                            engine::snapshot::ID_TYPE segment_id, const engine::TargetFields& target_fields,
                            TaskLabelPtr label);

    inline json
    Dump() const override {
        json ret{
            {"type", type_},
            {"collection_name", collection_name_},
            {"segment_id", segment_id_},
        };
        return ret;
    }

    Status
    OnLoad(LoadType type, uint8_t device_id) override;

    Status
    OnExecute() override;

    static void
    GroupFieldsForIndex(const std::string& collection_name, engine::TargetFieldGroups& groups);

 private:
    void
    CreateExecEngine();

 public:
    const engine::DBOptions& options_;
    std::string collection_name_;
    engine::snapshot::ID_TYPE segment_id_;

    // structured field could not be processed with vector field in a task
    // vector field could be build by cpu or gpu, so each task could only handle one field
    // the target_fields_ is passed to tell ExecutionEngine which field should be build by this task
    engine::TargetFields target_fields_;

    engine::ExecutionEnginePtr execution_engine_;
};

}  // namespace scheduler
}  // namespace milvus
