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

#include "scheduler/TaskTable.h"
#include "scheduler/event/Event.h"

#include <memory>
#include <string>
#include <utility>

namespace milvus {
namespace scheduler {

class LoadCompletedEvent : public Event {
 public:
    LoadCompletedEvent(std::shared_ptr<Resource> resource, TaskTableItemPtr task_table_item)
        : Event(EventType::LOAD_COMPLETED, std::move(resource)), task_table_item_(std::move(task_table_item)) {
    }

    inline std::string
    Dump() const override {
        return "<LoadCompletedEvent>";
    }

    friend std::ostream&
    operator<<(std::ostream& out, const LoadCompletedEvent& event);

 public:
    TaskTableItemPtr task_table_item_;
};

}  // namespace scheduler
}  // namespace milvus
