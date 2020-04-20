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

#include <gtest/gtest.h>
#include "scheduler/event/Event.h"
#include "scheduler/event/StartUpEvent.h"
#include "scheduler/resource/Resource.h"

namespace milvus {
namespace scheduler {

TEST(EventTest, START_UP_EVENT) {
    ResourcePtr res(nullptr);
    auto event = std::make_shared<StartUpEvent>(res);
    ASSERT_FALSE(event->Dump().empty());
    std::cout << *event;
    std::cout << *EventPtr(event);
}

TEST(EventTest, LOAD_COMPLETED_EVENT) {
    ResourcePtr res(nullptr);
    auto event = std::make_shared<LoadCompletedEvent>(res, nullptr);
    ASSERT_FALSE(event->Dump().empty());
    std::cout << *event;
    std::cout << *EventPtr(event);
}

TEST(EventTest, FINISH_TASK_EVENT) {
    ResourcePtr res(nullptr);
    auto event = std::make_shared<FinishTaskEvent>(res, nullptr);
    ASSERT_FALSE(event->Dump().empty());
    std::cout << *event;
    std::cout << *EventPtr(event);
}

TEST(EventTest, TASKTABLE_UPDATED_EVENT) {
    ResourcePtr res(nullptr);
    auto event = std::make_shared<TaskTableUpdatedEvent>(res);
    ASSERT_FALSE(event->Dump().empty());
    std::cout << *event;
    std::cout << *EventPtr(event);
}

}  // namespace scheduler
}  // namespace milvus
