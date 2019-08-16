/*******************************************************************************
 * copyright 上海赜睿信息科技有限公司(zilliz) - all rights reserved
 * unauthorized copying of this file, via any medium is strictly prohibited.
 * proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Event.h"
#include "../TaskTable.h"


namespace zilliz {
namespace milvus {
namespace engine {

class CopyCompletedEvent : public Event {
public:
    CopyCompletedEvent(std::weak_ptr<Resource> resource, TaskTableItem &task_table_item)
        : Event(EventType::COPY_COMPLETED, std::move(resource)),
          task_table_item_(task_table_item) {}
public:
    TaskTableItem &task_table_item_;
};

}
}
}
