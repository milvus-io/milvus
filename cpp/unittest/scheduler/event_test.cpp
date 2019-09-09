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

TEST(EventTest, start_up_event) {
    ResourceWPtr res(ResourcePtr(nullptr));
    auto event = std::make_shared<StartUpEvent>(res);
    ASSERT_FALSE(event->Dump().empty());
    std::stringstream ss;
    ss << event;
    ASSERT_FALSE(ss.str().empty());
}

TEST(EventTest, load_completed_event) {
    ResourceWPtr res(ResourcePtr(nullptr));
    auto event = std::make_shared<LoadCompletedEvent>(res, nullptr);
    ASSERT_FALSE(event->Dump().empty());
    std::stringstream ss;
    ss << event;
    ASSERT_FALSE(ss.str().empty());
}

TEST(EventTest, finish_task_event) {
    ResourceWPtr res(ResourcePtr(nullptr));
    auto event = std::make_shared<FinishTaskEvent>(res, nullptr);
    ASSERT_FALSE(event->Dump().empty());
    std::stringstream ss;
    ss << event;
    ASSERT_FALSE(ss.str().empty());
}


TEST(EventTest, tasktable_updated_event) {
    ResourceWPtr res(ResourcePtr(nullptr));
    auto event = std::make_shared<TaskTableUpdatedEvent>(res);
    ASSERT_FALSE(event->Dump().empty());
    std::stringstream ss;
    ss << event;
    ASSERT_FALSE(ss.str().empty());
}

}
}
}

