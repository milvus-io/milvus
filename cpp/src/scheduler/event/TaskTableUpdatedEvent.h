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

class TaskTableUpdatedEvent : public Event {
public:
    explicit
    TaskTableUpdatedEvent(std::weak_ptr<Resource> resource)
        : Event(EventType::TASK_TABLE_UPDATED, std::move(resource)) {}

    inline std::string
    Dump() const override {
        return "<TaskTableUpdatedEvent>";
    }

    friend std::ostream &operator<<(std::ostream &out, const TaskTableUpdatedEvent &event);
};


}
}
}
