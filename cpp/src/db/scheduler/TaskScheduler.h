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

#include "context/IScheduleContext.h"
#include "task/IScheduleTask.h"
#include "TaskDispatchQueue.h"
#include "utils/BlockingQueue.h"

#include <thread>

namespace zilliz {
namespace milvus {
namespace engine {

class TaskScheduler {
private:
    TaskScheduler();
    virtual ~TaskScheduler();

public:
    static TaskScheduler& GetInstance();

    bool Schedule(ScheduleContextPtr context);

private:
    bool Start();
    bool Stop();

    bool TaskDispatchWorker();
    bool TaskWorker();

private:
    std::shared_ptr<std::thread> task_dispatch_thread_;
    std::shared_ptr<std::thread> task_thread_;

    TaskDispatchQueue task_dispatch_queue_;

    using TaskQueue = server::BlockingQueue<ScheduleTaskPtr>;
    TaskQueue task_queue_;

    bool stopped_ = true;
};


}
}
}
