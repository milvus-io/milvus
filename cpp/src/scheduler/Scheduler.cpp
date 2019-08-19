/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "Scheduler.h"
#include "Cost.h"
#include "action/Action.h"


namespace zilliz {
namespace milvus {
namespace engine {

Scheduler::Scheduler(ResourceMgrWPtr res_mgr)
    : running_(false),
      res_mgr_(std::move(res_mgr)) {
    if (auto mgr = res_mgr_.lock()) {
        mgr->RegisterSubscriber(std::bind(&Scheduler::PostEvent, this, std::placeholders::_1));
    }
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
Scheduler::PostEvent(const EventPtr &event) {
    std::lock_guard<std::mutex> lock(event_mutex_);
    event_queue_.push(event);
    event_cv_.notify_one();
//    SERVER_LOG_DEBUG << "Scheduler post " << *event;
}

std::string
Scheduler::Dump() {
    return std::string();
}

void
Scheduler::worker_function() {
    while (running_) {
        std::unique_lock<std::mutex> lock(event_mutex_);
        event_cv_.wait(lock, [this] { return !event_queue_.empty(); });
        auto event = event_queue_.front();
        if (event == nullptr) {
            break;
        }

//        SERVER_LOG_DEBUG << "Scheduler process " << *event;
        event_queue_.pop();
        Process(event);
    }
}

void
Scheduler::Process(const EventPtr &event) {
    switch (event->Type()) {
        case EventType::START_UP: {
            OnStartUp(event);
            break;
        }
        case EventType::COPY_COMPLETED: {
            OnCopyCompleted(event);
            break;
        }
        case EventType::FINISH_TASK: {
            OnFinishTask(event);
            break;
        }
        case EventType::TASK_TABLE_UPDATED: {
            OnTaskTableUpdated(event);
            break;
        }
        default: {
            // TODO: logging
            break;
        }
    }
}


void
Scheduler::OnStartUp(const EventPtr &event) {
    if (auto resource = event->resource_.lock()) {
        resource->WakeupLoader();
    }
}

void
Scheduler::OnFinishTask(const EventPtr &event) {
    if (auto resource = event->resource_.lock()) {
        resource->WakeupExecutor();
    }
}

void
Scheduler::OnCopyCompleted(const EventPtr &event) {
    if (auto resource = event->resource_.lock()) {
        resource->WakeupLoader();
        resource->WakeupExecutor();
        if (resource->Type()== ResourceType::DISK) {
            Action::PushTaskToNeighbour(event->resource_);
        }
    }
}

void
Scheduler::OnTaskTableUpdated(const EventPtr &event) {
//    Action::PushTaskToNeighbour(event->resource_);
    if (auto resource = event->resource_.lock()) {
        resource->WakeupLoader();
    }
}

}
}
}
