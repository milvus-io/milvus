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

#include "server/delivery/RequestScheduler.h"
#include "utils/Log.h"

#include <fiu-local.h>
#include <unistd.h>
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
    if (stopped_ && request_groups_.empty() && execute_threads_.empty()) {
        return;
    }

    LOG_SERVER_INFO_ << "Scheduler gonna stop...";
    {
        std::lock_guard<std::mutex> lock(queue_mtx_);
        for (auto& iter : request_groups_) {
            if (iter.second != nullptr) {
                iter.second->Put(nullptr);
            }
        }
    }

    for (auto& iter : execute_threads_) {
        if (iter == nullptr)
            continue;
        iter->join();
    }
    request_groups_.clear();
    execute_threads_.clear();
    stopped_ = true;
    LOG_SERVER_INFO_ << "Scheduler stopped";
}

Status
RequestScheduler::ExecuteRequest(const BaseRequestPtr& request_ptr) {
    if (request_ptr == nullptr) {
        return Status::OK();
    }

    auto status = request_ptr->PreExecute();
    if (!status.ok()) {
        request_ptr->Done();
        return status;
    }

    status = PutToQueue(request_ptr);
    fiu_do_on("RequestScheduler.ExecuteRequest.push_queue_fail", status = Status(SERVER_INVALID_ARGUMENT, ""));

    if (!status.ok()) {
        LOG_SERVER_ERROR_ << "Put request to queue failed with code: " << status.ToString();
        request_ptr->Done();
        return status;
    }

    if (request_ptr->IsAsync()) {
        return Status::OK();  // async execution, caller need to call WaitToFinish at somewhere
    }

    status = request_ptr->WaitToFinish();  // sync execution
    if (!status.ok()) {
        return status;
    }

    return request_ptr->PostExecute();
}

void
RequestScheduler::TakeToExecute(RequestQueuePtr request_queue) {
    SetThreadName("reqsched_thread");
    if (request_queue == nullptr) {
        return;
    }

    while (true) {
        BaseRequestPtr request = request_queue->TakeRequest();
        if (request == nullptr) {
            LOG_SERVER_ERROR_ << "Take null from request queue, stop thread";
            break;  // stop the thread
        }

        try {
            fiu_do_on("RequestScheduler.TakeToExecute.throw_std_exception1", throw std::exception());
            auto status = request->Execute();
            fiu_do_on("RequestScheduler.TakeToExecute.throw_std_exception", throw std::exception());
            fiu_do_on("RequestScheduler.TakeToExecute.execute_fail", status = Status(SERVER_INVALID_ARGUMENT, ""));
            if (!status.ok()) {
                LOG_SERVER_ERROR_ << "Request failed with code: " << status.ToString();
            }
        } catch (std::exception& ex) {
            LOG_SERVER_ERROR_ << "Request failed to execute: " << ex.what();
        }
    }
}

Status
RequestScheduler::PutToQueue(const BaseRequestPtr& request_ptr) {
    std::lock_guard<std::mutex> lock(queue_mtx_);

    std::string group_name = request_ptr->RequestGroup();
    if (request_groups_.count(group_name) > 0) {
        request_groups_[group_name]->PutRequest(request_ptr);
    } else {
        RequestQueuePtr queue = std::make_shared<RequestQueue>();
        queue->PutRequest(request_ptr);
        request_groups_.insert(std::make_pair(group_name, queue));
        fiu_do_on("RequestScheduler.PutToQueue.null_queue", queue = nullptr);

        // start a thread
        ThreadPtr thread = std::make_shared<std::thread>(&RequestScheduler::TakeToExecute, this, queue);

        fiu_do_on("RequestScheduler.PutToQueue.push_null_thread", execute_threads_.push_back(nullptr));
        execute_threads_.push_back(thread);
        LOG_SERVER_INFO_ << "Create new thread for request group: " << group_name;
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
