/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include <src/cache/GpuCacheMgr.h>
#include "event/LoadCompletedEvent.h"
#include "Scheduler.h"
#include "action/Action.h"
#include "Algorithm.h"


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
        case EventType::LOAD_COMPLETED: {
            OnLoadCompleted(event);
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

// TODO: refactor the function
void
Scheduler::OnLoadCompleted(const EventPtr &event) {
    auto load_completed_event = std::static_pointer_cast<LoadCompletedEvent>(event);
    if (auto resource = event->resource_.lock()) {
        resource->WakeupExecutor();

        auto task_table_type = load_completed_event->task_table_item_->task->label()->Type();
        switch (task_table_type) {
            case TaskLabelType::DEFAULT: {
                if (not resource->HasExecutor() && load_completed_event->task_table_item_->Move()) {
                    auto task = load_completed_event->task_table_item_->task;
                    auto search_task = std::static_pointer_cast<XSearchTask>(task);
                    bool moved = false;

                    // to support test task, REFACTOR
                    if (auto index_engine = search_task->index_engine_) {
                        auto location = index_engine->GetLocation();

                        for (auto i = 0; i < res_mgr_.lock()->GetNumGpuResource(); ++i) {
                            auto index = zilliz::milvus::cache::GpuCacheMgr::GetInstance(i)->GetIndex(location);
                            if (index != nullptr) {
                                moved = true;
                                auto dest_resource = res_mgr_.lock()->GetResource(ResourceType::GPU, i);
                                Action::PushTaskToResource(load_completed_event->task_table_item_->task, dest_resource);
                                break;
                            }
                        }
                    }

                    if (not moved) {
                        Action::PushTaskToNeighbourRandomly(task, resource);
                    }
                }
                break;
            }
            case TaskLabelType::SPECIFIED_RESOURCE: {
                auto self = event->resource_.lock();
                auto task = load_completed_event->task_table_item_->task;

                // if this resource is disk, assign it to smallest cost resource
                if (self->type() == ResourceType::DISK) {
                    // step 1: calculate shortest path per resource, from disk to compute resource
                    auto compute_resources = res_mgr_.lock()->GetComputeResources();
                    std::vector<std::vector<std::string>> paths;
                    std::vector<uint64_t> transport_costs;
                    for (auto &res : compute_resources) {
                        std::vector<std::string> path;
                        uint64_t transport_cost = ShortestPath(self, res, res_mgr_.lock(), path);
                        transport_costs.push_back(transport_cost);
                        paths.emplace_back(path);
                    }

                    // step 2: select min cost, cost(resource) = avg_cost * task_to_do + transport_cost
                    uint64_t min_cost = std::numeric_limits<uint64_t>::max();
                    uint64_t min_cost_idx = 0;
                    for (uint64_t i = 0; i < compute_resources.size(); ++i) {
                        if (compute_resources[i]->TotalTasks() == 0) {
                            min_cost_idx = i;
                            break;
                        }
                        uint64_t cost = compute_resources[i]->TaskAvgCost() * compute_resources[i]->NumOfTaskToExec()
                            + transport_costs[i];
                        if (min_cost > cost) {
                            min_cost = cost;
                            min_cost_idx = i;
                        }
                    }

                    // step 3: set path in task
                    Path task_path(paths[min_cost_idx], paths[min_cost_idx].size() - 1);
                    task->path() = task_path;
                }

                if (self->name() == task->path().Last()) {
                    self->WakeupLoader();
                } else {
                    auto next_res_name = task->path().Next();
                    auto next_res = res_mgr_.lock()->GetResource(next_res_name);
                    load_completed_event->task_table_item_->Move();
                    next_res->task_table().Put(task);
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
