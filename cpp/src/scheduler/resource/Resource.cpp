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

#include "scheduler/resource/Resource.h"
#include "scheduler/Utils.h"

#include <iostream>
#include <utility>

namespace milvus {
namespace scheduler {

std::ostream&
operator<<(std::ostream& out, const Resource& resource) {
    out << resource.Dump();
    return out;
}

Resource::Resource(std::string name, ResourceType type, uint64_t device_id, bool enable_loader, bool enable_executor)
    : name_(std::move(name)),
      type_(type),
      device_id_(device_id),
      enable_loader_(enable_loader),
      enable_executor_(enable_executor) {
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
    if (enable_loader_) {
        loader_thread_ = std::thread(&Resource::loader_function, this);
    }
    if (enable_executor_) {
        executor_thread_ = std::thread(&Resource::executor_function, this);
    }
}

void
Resource::Stop() {
    running_ = false;
    if (enable_loader_) {
        WakeupLoader();
        loader_thread_.join();
    }
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

uint64_t
Resource::NumOfTaskToExec() {
    uint64_t count = 0;
    for (auto& task : task_table_) {
        if (task->state == TaskTableItemState::LOADED)
            ++count;
    }
    return count;
}

TaskTableItemPtr
Resource::pick_task_load() {
    auto indexes = task_table_.PickToLoad(10);
    for (auto index : indexes) {
        // try to set one task loading, then return
        if (task_table_.Load(index))
            return task_table_.Get(index);
        // else try next
    }
    return nullptr;
}

TaskTableItemPtr
Resource::pick_task_execute() {
    auto indexes = task_table_.PickToExecute(3);
    for (auto index : indexes) {
        // try to set one task executing, then return
        if (task_table_.Execute(index))
            return task_table_.Get(index);
        // else try next
    }
    return nullptr;
}

void
Resource::loader_function() {
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
            LoadFile(task_item->task);
            task_item->Loaded();
            if (subscriber_) {
                auto event = std::make_shared<LoadCompletedEvent>(shared_from_this(), task_item);
                subscriber_(std::static_pointer_cast<Event>(event));
            }
        }
    }
}

void
Resource::executor_function() {
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
            auto finish = get_current_timestamp();
            ++total_task_;
            total_cost_ += finish - start;

            task_item->Executed();
            if (subscriber_) {
                auto event = std::make_shared<FinishTaskEvent>(shared_from_this(), task_item);
                subscriber_(std::static_pointer_cast<Event>(event));
            }
        }
    }
}

}  // namespace scheduler
}  // namespace milvus
