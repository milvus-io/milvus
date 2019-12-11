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

#include "server/delivery/RequestScheduler.h"
#include "utils/Log.h"

#include <utility>

namespace milvus {
namespace server {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
RequestScheduler::RequestScheduler() : stopped_(false) {
    Start();
}

RequestScheduler::~RequestScheduler() {
    Stop();
}

void
RequestScheduler::ExecRequest(BaseRequestPtr& request_ptr) {
    if (request_ptr == nullptr) {
        return;
    }

    RequestScheduler& scheduler = RequestScheduler::GetInstance();
    scheduler.ExecuteRequest(request_ptr);

    if (!request_ptr->IsAsync()) {
        request_ptr->WaitToFinish();
    }
}

void
RequestScheduler::Start() {
    if (!stopped_) {
        return;
    }

    stopped_ = false;
}

void
RequestScheduler::Stop() {
    if (stopped_) {
        return;
    }

    SERVER_LOG_INFO << "Scheduler gonna stop...";
    {
        std::lock_guard<std::mutex> lock(queue_mtx_);
        for (auto iter : request_groups_) {
            if (iter.second != nullptr) {
                iter.second->Put(nullptr);
            }
        }
    }

    for (auto iter : execute_threads_) {
        if (iter == nullptr)
            continue;

        iter->join();
    }
    stopped_ = true;
    SERVER_LOG_INFO << "Scheduler stopped";
}

Status
RequestScheduler::ExecuteRequest(const BaseRequestPtr& request_ptr) {
    if (request_ptr == nullptr) {
        return Status::OK();
    }

    auto status = PutToQueue(request_ptr);
    if (!status.ok()) {
        SERVER_LOG_ERROR << "Put request to queue failed with code: " << status.ToString();
        return status;
    }

    if (request_ptr->IsAsync()) {
        return Status::OK();  // async execution, caller need to call WaitToFinish at somewhere
    }

    return request_ptr->WaitToFinish();  // sync execution
}

void
RequestScheduler::TakeToExecute(RequestQueuePtr request_queue) {
    if (request_queue == nullptr) {
        return;
    }

    while (true) {
        BaseRequestPtr request = request_queue->Take();
        if (request == nullptr) {
            SERVER_LOG_ERROR << "Take null from request queue, stop thread";
            break;  // stop the thread
        }

        try {
            auto status = request->Execute();
            if (!status.ok()) {
                SERVER_LOG_ERROR << "Request failed with code: " << status.ToString();
            }
        } catch (std::exception& ex) {
            SERVER_LOG_ERROR << "Request failed to execute: " << ex.what();
        }
    }
}

Status
RequestScheduler::PutToQueue(const BaseRequestPtr& request_ptr) {
    std::lock_guard<std::mutex> lock(queue_mtx_);

    std::string group_name = request_ptr->RequestGroup();
    if (request_groups_.count(group_name) > 0) {
        request_groups_[group_name]->Put(request_ptr);
    } else {
        RequestQueuePtr queue = std::make_shared<RequestQueue>();
        queue->Put(request_ptr);
        request_groups_.insert(std::make_pair(group_name, queue));

        // start a thread
        ThreadPtr thread = std::make_shared<std::thread>(&RequestScheduler::TakeToExecute, this, queue);
        execute_threads_.push_back(thread);
        SERVER_LOG_INFO << "Create new thread for request group: " << group_name;
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
