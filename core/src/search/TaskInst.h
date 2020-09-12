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

#include <condition_variable>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "Task.h"

namespace milvus {
namespace search {

class TaskInst {
 public:
    static TaskInst&
    GetInstance() {
        static TaskInst instance;
        return instance;
    }

    void
    Start();
    void
    Stop();

    std::queue<TaskPtr>&
    load_queue();
    std::queue<TaskPtr>&
    exec_queue();

    std::condition_variable&
    load_cv();
    std::condition_variable&
    exec_cv();

 private:
    TaskInst() = default;
    ~TaskInst() = default;

    void
    StartLoadTask();
    void
    StartExecuteTask();
    void
    StopLoadTask();
    void
    StopExecuteTask();

 private:
    bool running_;

    std::shared_ptr<std::thread> load_thread_;
    std::shared_ptr<std::thread> exec_thread_;

    std::queue<TaskPtr> load_queue_;
    std::queue<TaskPtr> exec_queue_;

    std::condition_variable load_cv_;
    std::condition_variable exec_cv_;
    std::mutex exec_mutex_;
    std::mutex load_mutex_;
};

}  // namespace search
}  // namespace milvus

#endif
