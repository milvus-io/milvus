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


#include "server/ServerConfig.h"
#include "TaskScheduler.h"
#include "TaskDispatchQueue.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "db/engine/EngineFactory.h"
#include "scheduler/task/TaskConvert.h"
#include "scheduler/SchedInst.h"
#include "scheduler/ResourceFactory.h"

namespace zilliz {
namespace milvus {
namespace engine {

TaskScheduler::TaskScheduler()
    : stopped_(true) {
    Start();
}

TaskScheduler::~TaskScheduler() {
    Stop();
}

TaskScheduler& TaskScheduler::GetInstance() {
    static TaskScheduler s_instance;
    return s_instance;
}

bool
TaskScheduler::Start() {
    if(!stopped_) {
        SERVER_LOG_INFO << "Task Scheduler isn't started";
        return true;
    }

    stopped_ = false;

    task_queue_.SetCapacity(2);

    task_dispatch_thread_ = std::make_shared<std::thread>(&TaskScheduler::TaskDispatchWorker, this);
    task_thread_  = std::make_shared<std::thread>(&TaskScheduler::TaskWorker, this);

    return true;
}

bool
TaskScheduler::Stop() {
    if(stopped_) {
        SERVER_LOG_INFO << "Task Scheduler already stopped";
        return true;
    }

    if(task_dispatch_thread_) {
        task_dispatch_queue_.Put(nullptr);
        task_dispatch_thread_->join();
        task_dispatch_thread_ = nullptr;
    }

    if(task_thread_) {
        task_queue_.Put(nullptr);
        task_thread_->join();
        task_thread_ = nullptr;
    }

    stopped_ = true;

    return true;
}

bool
TaskScheduler::Schedule(ScheduleContextPtr context) {
    task_dispatch_queue_.Put(context);

    return true;
}

bool
TaskScheduler::TaskDispatchWorker() {
    while(true) {
        ScheduleTaskPtr task_ptr = task_dispatch_queue_.Take();
        if(task_ptr == nullptr) {
            SERVER_LOG_INFO << "Stop db task dispatch thread";
            return true;
        }

        // TODO: Put task into Disk-TaskTable
        auto task = TaskConvert(task_ptr);
        auto disk_list = ResMgrInst::GetInstance()->GetDiskResources();
        if (!disk_list.empty()) {
	    if (auto disk = disk_list[0].lock()) {
                disk->task_table().Put(task);
            }
        }
    }
}

bool
TaskScheduler::TaskWorker() {
    while(true) {
        // TODO: expected blocking forever
        ScheduleTaskPtr task_ptr = task_queue_.Take();
        if(task_ptr == nullptr) {
            SERVER_LOG_INFO << "Stop db task worker thread";
            return true;
        }

        //execute task
        ScheduleTaskPtr next_task = task_ptr->Execute();
        if(next_task != nullptr) {
            task_queue_.Put(next_task);
        }
    }
}

}
}
}
