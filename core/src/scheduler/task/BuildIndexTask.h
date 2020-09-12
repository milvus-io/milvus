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

#include "Task.h"
#include "scheduler/Definition.h"
#include "scheduler/job/BuildIndexJob.h"

namespace milvus {
namespace scheduler {

class XBuildIndexTask : public Task {
 public:
    explicit XBuildIndexTask(SegmentSchemaPtr file, TaskLabelPtr label);

    void
    Load(LoadType type, uint8_t device_id) override;

    void
    Execute() override;

 public:
    SegmentSchemaPtr file_;
    SegmentSchema table_file_;
    size_t to_index_id_ = 0;
    int to_index_type_ = 0;
    ExecutionEnginePtr to_index_engine_ = nullptr;
};

}  // namespace scheduler
}  // namespace milvus
