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

#include <map>
#include <memory>
#include <string>

#include "db/snapshot/MetaEvent.h"

namespace milvus {
namespace engine {
namespace snapshot {

class IEventHandler {
 public:
    using Ptr = std::shared_ptr<IEventHandler>;
    static constexpr const char* EventName = "";
    virtual const char*
    GetEventName() const {
        return EventName;
    }
};

class IEventHandlerRegistrar {
 public:
    using Ptr = std::shared_ptr<IEventHandlerRegistrar>;

    virtual IEventHandler::Ptr
    GetHandler() = 0;
};

template <typename T>
class HandlerFactory {
 public:
    using ThisT = HandlerFactory<T>;

    static ThisT&
    GetInstance() {
        static ThisT factory;
        return factory;
    }

    IEventHandler::Ptr
    GetHandler(const std::string& event_name) {
        auto it = registry_.find(event_name);
        if (it == registry_.end()) {
            return nullptr;
        }
        return it->second->GetHandler();
    }

    void
    Register(IEventHandlerRegistrar* registrar, const std::string& event_name) {
        auto it = registry_.find(event_name);
        if (it == registry_.end()) {
            registry_[event_name] = registrar;
        }
    }

 private:
    std::map<std::string, IEventHandlerRegistrar*> registry_;
};

template <typename T, typename HandlerT>
class EventHandlerRegistrar : public IEventHandlerRegistrar {
 public:
    using FactoryT = HandlerFactory<T>;
    using HandlerPtr = typename HandlerT::Ptr;
    explicit EventHandlerRegistrar(const std::string& event_name) : event_name_(event_name) {
        auto& factory = FactoryT::GetInstance();
        factory.Register(this, event_name_);
    }

    HandlerPtr
    GetHandler() {
        return std::make_shared<HandlerT>();
    }

 protected:
    std::string event_name_;
};

#define REGISTER_HANDLER(EXECUTOR, HANDLER)                                                                  \
    namespace {                                                                                              \
    static milvus::engine::snapshot::EventHandlerRegistrar<EXECUTOR, HANDLER> EXECUTOR##HANDLER##_registrar( \
        HANDLER ::EventName);                                                                                \
    }

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
