/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/


#include <gtest/gtest.h>
#include "scheduler/resource/Resource.h"
#include "scheduler/event/Event.h"
#include "scheduler/event/StartUpEvent.h"


namespace zilliz {
namespace milvus {
namespace engine {

TEST(EventTest, START_UP_EVENT) {
    ResourceWPtr res(ResourcePtr(nullptr));
    auto event = std::make_shared<StartUpEvent>(res);
    ASSERT_FALSE(event->Dump().empty());
    std::cout << *event;
    std::cout << *EventPtr(event);
}

TEST(EventTest, LOAD_COMPLETED_EVENT) {
    ResourceWPtr res(ResourcePtr(nullptr));
    auto event = std::make_shared<LoadCompletedEvent>(res, nullptr);
    ASSERT_FALSE(event->Dump().empty());
    std::cout << *event;
    std::cout << *EventPtr(event);
}

TEST(EventTest, FINISH_TASK_EVENT) {
    ResourceWPtr res(ResourcePtr(nullptr));
    auto event = std::make_shared<FinishTaskEvent>(res, nullptr);
    ASSERT_FALSE(event->Dump().empty());
    std::cout << *event;
    std::cout << *EventPtr(event);
}


TEST(EventTest, TASKTABLE_UPDATED_EVENT) {
    ResourceWPtr res(ResourcePtr(nullptr));
    auto event = std::make_shared<TaskTableUpdatedEvent>(res);
    ASSERT_FALSE(event->Dump().empty());
    std::cout << *event;
    std::cout << *EventPtr(event);
}

}
}
}

