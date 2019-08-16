/*******************************************************************************
 * copyright 上海赜睿信息科技有限公司(zilliz) - all rights reserved
 * unauthorized copying of this file, via any medium is strictly prohibited.
 * proprietary and confidential.
 ******************************************************************************/
#pragma once

namespace zilliz {
namespace milvus {
namespace engine {

enum class EventType {
    START_UP,
    COPY_COMPLETED,
    FINISH_TASK,
    TASK_TABLE_UPDATED
};

class Resource;

class Event {
public:
    explicit
    Event(EventType type, std::weak_ptr<Resource> resource)
        : type_(type),
          resource_(std::move(resource)) {}

    inline EventType
    Type() const {
        return type_;
    }

public:
    EventType type_;
    std::weak_ptr<Resource> resource_;
};

using EventPtr = std::shared_ptr<Event>;

}
}
}
