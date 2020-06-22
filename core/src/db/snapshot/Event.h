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

#include <memory>
#include <string>

#include "db/snapshot/Operations.h"

namespace milvus {
namespace engine {
namespace snapshot {

enum class EventType {
    EVENT_INVALID = 0,
    EVENT_GC = 1,
};

struct EventContext {
    ID_TYPE id_;
    std::string res_type_;
};

class Event {
 public:
    Event() = default;
    ~Event() = default;

    virtual void
    Process() = 0;

    std::string
    ToString() {
        return (context_.res_type_ + std::to_string(context_.id_));
    }

 protected:
    EventType type_;
    EventContext context_;
};

template <typename ResourceT>
class ResourceGCEvent : public Event {
 public:
    ResourceGCEvent(ID_TYPE id) {
        type_ = EventType::EVENT_GC;
        context_.id_ = id;
    }

    ~ResourceGCEvent() = default;

    void Process() override {
        auto op = std::make_shared<HardDeleteOperation<ResourceT>>(context_.id_);
        op->Push();
    }
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
