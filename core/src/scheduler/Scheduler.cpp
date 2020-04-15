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

#include "scheduler/Scheduler.h"
#include "Algorithm.h"
#include "action/Action.h"
#include "cache/GpuCacheMgr.h"
#include "event/LoadCompletedEvent.h"

#include <utility>

namespace milvus {
namespace scheduler {

Scheduler::Scheduler(ResourceMgrPtr res_mgr) : running_(false), res_mgr_(std::move(res_mgr)) {
    res_mgr_->RegisterSubscriber(std::bind(&Scheduler::PostEvent, this, std::placeholders::_1));
    event_register_.insert(std::make_pair(static_cast<uint64_t>(EventType::START_UP),
                                          std::bind(&Scheduler::OnStartUp, this, std::placeholders::_1)));
    event_register_.insert(std::make_pair(static_cast<uint64_t>(EventType::LOAD_COMPLETED),
                                          std::bind(&Scheduler::OnLoadCompleted, this, std::placeholders::_1)));
    event_register_.insert(std::make_pair(static_cast<uint64_t>(EventType::TASK_TABLE_UPDATED),
                                          std::bind(&Scheduler::OnTaskTableUpdated, this, std::placeholders::_1)));
    event_register_.insert(std::make_pair(static_cast<uint64_t>(EventType::FINISH_TASK),
                                          std::bind(&Scheduler::OnFinishTask, this, std::placeholders::_1)));
}

Scheduler::~Scheduler() {
    res_mgr_ = nullptr;
}

void
Scheduler::Start() {
    running_ = true;
    worker_thread_ = std::thread(&Scheduler::worker_function, this);
}

void
Scheduler::Stop() {
    {
        std::lock_guard<std::mutex> lock(event_mutex_);
        running_ = false;
        event_queue_.push(nullptr);
        event_cv_.notify_one();
    }
    worker_thread_.join();
}

void
Scheduler::PostEvent(const EventPtr& event) {
    {
        std::lock_guard<std::mutex> lock(event_mutex_);
        event_queue_.push(event);
    }
    event_cv_.notify_one();
}

json
Scheduler::Dump() const {
    json ret{
        {"running", running_},
        {"event_queue_length", event_queue_.size()},
    };
    return ret;
}

void
Scheduler::process(const EventPtr& event) {
    auto process_event = event_register_.at(static_cast<int>(event->Type()));
    process_event(event);
}

void
Scheduler::worker_function() {
    SetThreadName("schedevt_thread");
    while (running_) {
        std::unique_lock<std::mutex> lock(event_mutex_);
        event_cv_.wait(lock, [this] { return !event_queue_.empty(); });
        auto event = event_queue_.front();
        event_queue_.pop();
        if (event == nullptr) {
            break;
        }

        process(event);
    }
}

// TODO(wxyu): refactor the function
void
Scheduler::OnLoadCompleted(const EventPtr& event) {
    auto load_completed_event = std::static_pointer_cast<LoadCompletedEvent>(event);

    auto resource = event->resource_;
    resource->WakeupExecutor();

    auto task_table_type = load_completed_event->task_table_item_->task->label()->Type();
    switch (task_table_type) {
        case TaskLabelType::SPECIFIED_RESOURCE: {
            Action::SpecifiedResourceLabelTaskScheduler(res_mgr_, resource, load_completed_event);
            break;
        }
        case TaskLabelType::BROADCAST: {
            if (resource->HasExecutor() == false) {
                load_completed_event->task_table_item_->Move();
            }
            Action::PushTaskToAllNeighbour(load_completed_event->task_table_item_, resource);
            break;
        }
        default: { break; }
    }
    resource->WakeupLoader();
}

void
Scheduler::OnStartUp(const EventPtr& event) {
    event->resource_->WakeupLoader();
}

void
Scheduler::OnFinishTask(const EventPtr& event) {
    event->resource_->WakeupLoader();
}

void
Scheduler::OnTaskTableUpdated(const EventPtr& event) {
    event->resource_->WakeupLoader();
}

}  // namespace scheduler
}  // namespace milvus
