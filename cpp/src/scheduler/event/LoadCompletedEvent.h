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

class LoadCompletedEvent : public Event {
public:
    LoadCompletedEvent(std::weak_ptr<Resource> resource, TaskTableItemPtr task_table_item)
        : Event(EventType::LOAD_COMPLETED, std::move(resource)),
          task_table_item_(std::move(task_table_item)) {}

    inline std::string
    Dump() const override {
        return "<LoadCompletedEvent>";
    }

    friend std::ostream &operator<<(std::ostream &out, const LoadCompletedEvent &event);

public:
    TaskTableItemPtr task_table_item_;
};

}
}
}
