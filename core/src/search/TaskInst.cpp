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
#if 0

#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "search/TaskInst.h"

namespace milvus {
namespace search {

void
TaskInst::Start() {
    running_ = true;
    load_thread_ = std::make_shared<std::thread>(&TaskInst::StartLoadTask, this);
    exec_thread_ = std::make_shared<std::thread>(&TaskInst::StartExecuteTask, this);
}

void
TaskInst::Stop() {
    running_ = false;
    StopExecuteTask();
    StopLoadTask();
}

std::queue<TaskPtr>&
TaskInst::load_queue() {
    return load_queue_;
}

std::queue<TaskPtr>&
TaskInst::exec_queue() {
    return exec_queue_;
}

std::condition_variable&
TaskInst::load_cv() {
    return load_cv_;
}

std::condition_variable&
TaskInst::exec_cv() {
    return exec_cv_;
}

void
TaskInst::StartLoadTask() {
    while (running_) {
        std::unique_lock<std::mutex> lock(load_mutex_);
        load_cv_.wait(lock, [this] { return !load_queue_.empty(); });
        while (!load_queue_.empty()) {
            auto task = load_queue_.front();
            task->Load();
            load_queue_.pop();
            exec_queue_.push(task);
            exec_cv_.notify_one();
        }
    }
}

void
TaskInst::StartExecuteTask() {
    while (running_) {
        std::unique_lock<std::mutex> lock(exec_mutex_);
        exec_cv_.wait(lock, [this] { return !exec_queue_.empty(); });
        while (!exec_queue_.empty()) {
            auto task = exec_queue_.front();
            task->Execute();
            exec_queue_.pop();
        }
    }
}

void
TaskInst::StopLoadTask() {
    {
        std::lock_guard<std::mutex> lock(load_mutex_);
        load_queue_.push(nullptr);
        load_cv_.notify_one();
        if (load_thread_->joinable()) {
            load_thread_->join();
        }
        load_thread_ = nullptr;
    }
}

void
TaskInst::StopExecuteTask() {
    {
        std::lock_guard<std::mutex> lock(exec_mutex_);
        exec_queue_.push(nullptr);
        exec_cv_.notify_one();
        if (exec_thread_->joinable()) {
            exec_thread_->join();
        }
        exec_thread_ = nullptr;
    }
}

}  // namespace search
}  // namespace milvus

#endif
