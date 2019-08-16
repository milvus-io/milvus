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

class StartUpEvent : public Event {
public:
    explicit
    StartUpEvent(std::weak_ptr<Resource> resource)
        : Event(EventType::START_UP, std::move(resource)) {}
};

}
}
}