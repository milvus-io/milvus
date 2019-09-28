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

#include "grpc/gen-status/status.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"
#include "utils/BlockingQueue.h"
#include "utils/Status.h"

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace zilliz {
namespace milvus {
namespace server {
namespace grpc {

class GrpcBaseTask {
 protected:
    explicit GrpcBaseTask(const std::string& task_group, bool async = false);

    virtual ~GrpcBaseTask();

 public:
    Status
    Execute();

    void
    Done();

    Status
    WaitToFinish();

    std::string
    TaskGroup() const {
        return task_group_;
    }

    const Status&
    status() const {
        return status_;
    }

    bool
    IsAsync() const {
        return async_;
    }

 protected:
    virtual Status
    OnExecute() = 0;

    Status
    SetStatus(ErrorCode error_code, const std::string& msg);

 protected:
    mutable std::mutex finish_mtx_;
    std::condition_variable finish_cond_;

    std::string task_group_;
    bool async_;
    bool done_;
    Status status_;
};

using BaseTaskPtr = std::shared_ptr<GrpcBaseTask>;
using TaskQueue = BlockingQueue<BaseTaskPtr>;
using TaskQueuePtr = std::shared_ptr<TaskQueue>;
using ThreadPtr = std::shared_ptr<std::thread>;

class GrpcRequestScheduler {
 public:
    static GrpcRequestScheduler&
    GetInstance() {
        static GrpcRequestScheduler scheduler;
        return scheduler;
    }

    void
    Start();

    void
    Stop();

    Status
    ExecuteTask(const BaseTaskPtr& task_ptr);

    static void
    ExecTask(BaseTaskPtr& task_ptr, ::milvus::grpc::Status* grpc_status);

 protected:
    GrpcRequestScheduler();

    virtual ~GrpcRequestScheduler();

    void
    TakeTaskToExecute(TaskQueuePtr task_queue);

    Status
    PutTaskToQueue(const BaseTaskPtr& task_ptr);

 private:
    mutable std::mutex queue_mtx_;

    std::map<std::string, TaskQueuePtr> task_groups_;

    std::vector<ThreadPtr> execute_threads_;

    bool stopped_;
};

}  // namespace grpc
}  // namespace server
}  // namespace milvus
}  // namespace zilliz
