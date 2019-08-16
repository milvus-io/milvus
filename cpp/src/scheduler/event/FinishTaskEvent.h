/*******************************************************************************
 * copyright 上海赜睿信息科技有限公司(zilliz) - all rights reserved
 * unauthorized copying of this file, via any medium is strictly prohibited.
 * proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Event.h"


namespace zilliz {
namespace milvus {
namespace engine {

class FinishTaskEvent : public Event {
public:
    FinishTaskEvent(std::weak_ptr<Resource> resource, TaskTableItem &task_table_item)
        : Event(EventType::FINISH_TASK, std::move(resource)),
          task_table_item_(task_table_item) {}

public:
    TaskTableItem &task_table_item_;
};

}
}
}
