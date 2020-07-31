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

#include "server/delivery/ReqQueue.h"
#include "server/delivery/strategy/ReqStrategy.h"
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
ScheduleReq(const BaseReqPtr& req, std::queue<BaseReqPtr>& queue) {
    if (req == nullptr) {
        return Status(SERVER_NULL_POINTER, "request schedule cannot handle null object");
    }

    if (queue.empty()) {
        queue.push(req);
        return Status::OK();
    }

    static std::map<ReqType, ReqStrategyPtr> s_schedulers = {{ReqType::kSearch, std::make_shared<SearchReqStrategy>()}};

    auto iter = s_schedulers.find(req->type());
    if (iter == s_schedulers.end() || iter->second == nullptr) {
        queue.push(req);
    } else {
        iter->second->ReScheduleQueue(req, queue);
    }

    return Status::OK();
}
}  // namespace

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
ReqQueue::ReqQueue() {
}

ReqQueue::~ReqQueue() {
}

BaseReqPtr
ReqQueue::TakeReq() {
    return Take();
}

Status
ReqQueue::PutReq(const BaseReqPtr& req_ptr) {
    std::unique_lock<std::mutex> lock(mtx);
    full_.wait(lock, [this] { return (queue_.size() < capacity_); });
    auto status = ScheduleReq(req_ptr, queue_);
    empty_.notify_all();
    return status;
}

}  // namespace server
}  // namespace milvus
