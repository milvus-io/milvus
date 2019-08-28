/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include <src/cache/GpuCacheMgr.h>
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
    {
        std::lock_guard<std::mutex> lock(event_mutex_);
        event_queue_.push(event);
    }
    event_cv_.notify_one();
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
        event_queue_.pop();
        if (event == nullptr) {
            break;
        }

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
}

void
Scheduler::OnCopyCompleted(const EventPtr &event) {
    auto load_completed_event = std::static_pointer_cast<CopyCompletedEvent>(event);
    if (auto resource = event->resource_.lock()) {
        resource->WakeupExecutor();

        auto task_table_type = load_completed_event->task_table_item_->task->label()->Type();
        switch (task_table_type) {
            case TaskLabelType::DEFAULT: {
                if (not resource->HasExecutor() && load_completed_event->task_table_item_->Move()) {
                    auto task = load_completed_event->task_table_item_->task;
                    auto search_task = std::static_pointer_cast<XSearchTask>(task);
                    auto location = search_task->index_engine_->GetLocation();

                    for (auto i = 0; i < res_mgr_.lock()->GetNumGpuResource(); ++i) {
                        auto index = zilliz::milvus::cache::GpuCacheMgr::GetInstance(i)->GetIndex(location);
                        if (index != nullptr) {
                            auto dest_resource = res_mgr_.lock()->GetResource(ResourceType::GPU, i);
                            Action::PushTaskToResource(load_completed_event->task_table_item_->task, dest_resource);
                        }
                    }

                }
                break;
            }
            case TaskLabelType::BROADCAST: {
                Action::PushTaskToAllNeighbour(load_completed_event->task_table_item_->task, resource);
                break;
            }
            default: {
                break;
            }
        }
    }
}

void
Scheduler::OnTaskTableUpdated(const EventPtr &event) {
    if (auto resource = event->resource_.lock()) {
        resource->WakeupLoader();
    }
}

}
}
}
