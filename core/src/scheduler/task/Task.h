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

#include "Path.h"
#include "scheduler/job/Job.h"
#include "scheduler/tasklabel/TaskLabel.h"
#include "utils/Status.h"

#include <memory>
#include <string>
#include <utility>

namespace milvus {
namespace scheduler {

enum class LoadType {
    DISK2CPU,
    CPU2GPU,
    GPU2CPU,
    TEST,
};

enum class TaskType {
    SearchTask,
    DeleteTask,
    BuildIndexTask,
    TestTask,
};

class Task;

using TaskPtr = std::shared_ptr<Task>;

// TODO: re-design
class Task {
 public:
    explicit Task(TaskType type, TaskLabelPtr label) : type_(type), label_(std::move(label)) {
    }

    /*
     * Just Getter;
     */
    inline TaskType
    Type() const {
        return type_;
    }

    /*
     * Transport path;
     */
    inline Path&
    path() {
        return task_path_;
    }

    /*
     * Getter and Setter;
     */
    inline TaskLabelPtr&
    label() {
        return label_;
    }

 public:
    virtual void
    Load(LoadType type, uint8_t device_id) = 0;

    virtual void
    Execute() = 0;

 public:
    Path task_path_;
    scheduler::JobWPtr job_;
    TaskType type_;
    TaskLabelPtr label_ = nullptr;
};

}  // namespace scheduler
}  // namespace milvus
