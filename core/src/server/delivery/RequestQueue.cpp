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

#include "server/delivery/RequestQueue.h"
#include "server/delivery/strategy/RequestStrategy.h"
#include "server/delivery/strategy/SearchReqStrategy.h"
#include "utils/Log.h"

#include <fiu-local.h>
#include <unistd.h>
#include <queue>
#include <utility>

namespace milvus {
namespace server {

namespace {
Status
ScheduleRequest(const BaseRequestPtr& request, std::queue<BaseRequestPtr>& queue) {
#if 1
    if (request == nullptr) {
        return Status(SERVER_NULL_POINTER, "request schedule cannot handle null object");
    }

    if (queue.empty()) {
        queue.push(request);
        return Status::OK();
    }

    static std::map<BaseRequest::RequestType, RequestStrategyPtr> s_schedulers = {
        {BaseRequest::kSearch, std::make_shared<SearchReqStrategy>()}};

    auto iter = s_schedulers.find(request->GetRequestType());
    if (iter == s_schedulers.end() || iter->second == nullptr) {
        queue.push(request);
    } else {
        iter->second->ReScheduleQueue(request, queue);
    }
#else
    queue.push(request);
#endif

    return Status::OK();
}
}  // namespace

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
RequestQueue::RequestQueue() {
}

RequestQueue::~RequestQueue() {
}

BaseRequestPtr
RequestQueue::TakeRequest() {
    return Take();
}

Status
RequestQueue::PutRequest(const BaseRequestPtr& request_ptr) {
    std::unique_lock<std::mutex> lock(mtx);
    full_.wait(lock, [this] { return (queue_.size() < capacity_); });
    auto status = ScheduleRequest(request_ptr, queue_);
    empty_.notify_all();
    return status;
}

}  // namespace server
}  // namespace milvus
