// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

#include "ResourceMgr.h"
#include "interface/interfaces.h"
#include "resource/Resource.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

class Scheduler : public interface::dumpable {
 public:
    explicit Scheduler(ResourceMgrPtr res_mgr);

    ~Scheduler();

    Scheduler(const Scheduler&) = delete;
    Scheduler(Scheduler&&) = delete;

    void
    Start();

    void
    Stop();

    void
    PostEvent(const EventPtr& event);

    json
    Dump() const override;

 private:
    void
    OnStartUp(const EventPtr& event);

    void
    OnFinishTask(const EventPtr& event);

    void
    OnLoadCompleted(const EventPtr& event);

    void
    OnTaskTableUpdated(const EventPtr& event);

 private:
    void
    process(const EventPtr& event);

    void
    worker_function();

 private:
    bool running_;

    std::unordered_map<uint64_t, std::function<void(EventPtr)>> event_register_;

    ResourceMgrPtr res_mgr_;
    std::queue<EventPtr> event_queue_;
    std::thread worker_thread_;
    std::mutex event_mutex_;
    std::condition_variable event_cv_;
};

using SchedulerPtr = std::shared_ptr<Scheduler>;

}  // namespace scheduler
}  // namespace milvus
