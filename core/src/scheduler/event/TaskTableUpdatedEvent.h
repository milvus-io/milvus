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

#include "Event.h"

#include <memory>
#include <string>
#include <utility>

namespace milvus {
namespace scheduler {

class TaskTableUpdatedEvent : public Event {
 public:
    explicit TaskTableUpdatedEvent(std::shared_ptr<Resource> resource)
        : Event(EventType::TASK_TABLE_UPDATED, std::move(resource)) {
    }

    inline std::string
    Dump() const override {
        return "<TaskTableUpdatedEvent>";
    }

    friend std::ostream&
    operator<<(std::ostream& out, const TaskTableUpdatedEvent& event);
};

}  // namespace scheduler
}  // namespace milvus
