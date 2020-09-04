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

#include "scheduler/resource/Resource.h"
#include "scheduler/SchedInst.h"
#include "scheduler/Utils.h"
#include "scheduler/task/FinishedTask.h"

#include <iostream>
#include <limits>
#include <utility>

namespace milvus {
namespace scheduler {

std::ostream&
operator<<(std::ostream& out, const Resource& resource) {
    out << resource.Dump();
    return out;
}

std::string
ToString(ResourceType type) {
    switch (type) {
        case ResourceType::DISK: {
            return "DISK";
        }
        case ResourceType::CPU: {
            return "CPU";
        }
        case ResourceType::GPU: {
            return "GPU";
        }
        default: { return "UNKNOWN"; }
    }
}

Resource::Resource(std::string name, ResourceType type, uint64_t device_id, bool enable_executor)
    : device_id_(device_id), name_(std::move(name)), type_(type), enable_executor_(enable_executor) {
    // register subscriber in tasktable
    task_table_.RegisterSubscriber([&] {
        if (subscriber_) {
            auto event = std::make_shared<TaskTableUpdatedEvent>(shared_from_this());
            subscriber_(std::static_pointer_cast<Event>(event));
        }
    });
}

void
Resource::Start() {
    running_ = true;
    loader_thread_ = std::thread(&Resource::loader_function, this);
    if (enable_executor_) {
        executor_thread_ = std::thread(&Resource::executor_function, this);
    }
}

void
Resource::Stop() {
    running_ = false;
    WakeupLoader();
    loader_thread_.join();
    if (enable_executor_) {
        WakeupExecutor();
        executor_thread_.join();
    }
}

void
Resource::WakeupLoader() {
    {
        std::lock_guard<std::mutex> lock(load_mutex_);
        load_flag_ = true;
    }
    load_cv_.notify_one();
}

void
Resource::WakeupExecutor() {
    {
        std::lock_guard<std::mutex> lock(exec_mutex_);
        exec_flag_ = true;
    }
    exec_cv_.notify_one();
}

json
Resource::Dump() const {
    json ret{
        {"device_id", device_id_},
        {"name", name_},
        {"type", ToString(type_)},
        {"task_average_cost", TaskAvgCost()},
        {"task_total_cost", total_cost_},
        {"total_tasks", total_task_},
        {"running", running_},
        {"enable_executor", enable_executor_},
    };
    return ret;
}

uint64_t
Resource::NumOfTaskToExec() {
    return task_table_.TaskToExecute();
}

TaskTableItemPtr
Resource::pick_task_load() {
    auto indexes = task_table_.PickToLoad(10);
    for (auto index : indexes) {
        // try to set one task loading, then return
        if (task_table_.Load(index)) {
            return task_table_.at(index);
        }
        // else try next
    }
    return nullptr;
}

TaskTableItemPtr
Resource::pick_task_execute() {
    auto indexes = task_table_.PickToExecute(std::numeric_limits<uint64_t>::max());
    for (auto index : indexes) {
        // try to set one task executing, then return
        if (task_table_[index]->task->label()->Type() == TaskLabelType::SPECIFIED_RESOURCE) {
            if (task_table_[index]->task->path().Last() != name()) {
                continue;
            }
        }

        if (task_table_.Execute(index)) {
            return task_table_.at(index);
        }
        //        if (task_table_[index]->task->label()->Type() == TaskLabelType::SPECIFIED_RESOURCE) {
        //            if (task_table_.Get(index)->task->path().Current() == task_table_.Get(index)->task->path().Last()
        //            &&
        //                task_table_.Get(index)->task->path().Last() == name()) {
        //                if (task_table_.Execute(index)) {
        //                    return task_table_.Get(index);
        //                }
        //            }
        //        }
        // else try next
    }
    return nullptr;
}

void
Resource::loader_function() {
    SetThreadName("taskloader_th");
    while (running_) {
        std::unique_lock<std::mutex> lock(load_mutex_);
        load_cv_.wait(lock, [&] { return load_flag_; });
        load_flag_ = false;
        lock.unlock();
        while (true) {
            auto task_item = pick_task_load();
            if (task_item == nullptr) {
                break;
            }
            if (task_item->task->Type() == TaskType::BuildIndexTask && name() == "cpu") {
                BuildMgrInst::GetInstance()->Take();
                LOG_SERVER_DEBUG_ << name() << " load BuildIndexTask";
            }
            LoadFile(task_item->task);
            task_item->Loaded();
            if (task_item->from) {
                task_item->from->Moved();
                task_item->from->task = FinishedTask::Create(task_item->from->task);
                task_item->from = nullptr;
            }
            if (subscriber_) {
                auto event = std::make_shared<LoadCompletedEvent>(shared_from_this(), task_item);
                subscriber_(std::static_pointer_cast<Event>(event));
            }
        }
    }
}

void
Resource::executor_function() {
    SetThreadName("taskexecutor_th");
    if (subscriber_) {
        auto event = std::make_shared<StartUpEvent>(shared_from_this());
        subscriber_(std::static_pointer_cast<Event>(event));
    }
    while (running_) {
        std::unique_lock<std::mutex> lock(exec_mutex_);
        exec_cv_.wait(lock, [&] { return exec_flag_; });
        exec_flag_ = false;
        lock.unlock();
        while (true) {
            auto task_item = pick_task_execute();
            if (task_item == nullptr) {
                break;
            }
            auto start = get_current_timestamp();
            Process(task_item->task);
            task_item->task = FinishedTask::Create(task_item->task);
            auto finish = get_current_timestamp();
            ++total_task_;
            total_cost_ += finish - start;

            task_item->Executed();

            if (task_item->task->Type() == TaskType::BuildIndexTask) {
                BuildMgrInst::GetInstance()->Put();
                ResMgrInst::GetInstance()->GetResource("cpu")->WakeupLoader();
                ResMgrInst::GetInstance()->GetResource("disk")->WakeupLoader();
            }

            task_item->task = FinishedTask::Create(task_item->task);

            if (subscriber_) {
                auto event = std::make_shared<FinishTaskEvent>(shared_from_this(), task_item);
                subscriber_(std::static_pointer_cast<Event>(event));
            }
        }
    }
}

}  // namespace scheduler
}  // namespace milvus
