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

#include <utility>

#include "utils/Log.h"
#include "utils/TimerManager.h"

namespace milvus {

TimerManager::TimerManager(unsigned int pool_size) : pool_size_(pool_size) {
}

TimerManager::~TimerManager() {
}

Status
TimerManager::SetPoolSize(unsigned int pool_size) {
    if (timer_exeutors_) {
        return Status(SERVER_UNEXPECTED_ERROR, "Cannot set pool size since it has been set already");
    }
    pool_size_ = pool_size;
    return Status::OK();
}

Status
TimerManager::Run() {
    boost::system::error_code ec;
    io_.run(ec);
    Status status;
    if (ec) {
        status = Status(SERVER_UNEXPECTED_ERROR, ec.message());
    }
    return status;
}

Status
TimerManager::Start() {
    for (auto& timer : timers_) {
        timer->timer_.async_wait(std::bind(&TimerContext::Reschedule, timer, std::placeholders::_1));
    }
    return Status::OK();
}

void
TimerManager::Stop() {
    boost::system::error_code ec;
    for (auto& timer : timers_) {
        timer->timer_.cancel(ec);
        if (ec) {
            LOG_SERVER_ERROR_ << "Fail to cancel timer: " << ec;
        }
    }
    if (timer_exeutors_) {
        timer_exeutors_->Stop();
    }
}

void
TimerManager::AddTimer(int interval_us, TimerContext::HandlerT handler) {
    if (!timer_exeutors_) {
        timer_exeutors_ = std::make_shared<ThreadPool>(pool_size_);
    }
    timers_.emplace_back(std::make_shared<TimerContext>(io_, interval_us, handler, timer_exeutors_));
}

void
TimerManager::AddTimer(const TimerContext::Context& ctx) {
    if (!timer_exeutors_) {
        timer_exeutors_ = std::make_shared<ThreadPool>(pool_size_);
    }
    TimerContext::Context context(ctx);
    context.pool = timer_exeutors_;
    timers_.emplace_back(std::make_shared<TimerContext>(io_, context));
}

}  // namespace milvus
