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

#pragma once

#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "utils/Log.h"
#include "utils/ThreadPool.h"

namespace milvus {

struct TimerContext {
    using HandlerT = std::function<void(const boost::system::error_code&)>;
    struct Context {
        /* Context(int interval_us, HandlerT& handler, ThreadPoolPtr pool = nullptr) */
        /*     : interval_(interval_us), handler_(handler), timer_(io, interval_), pool_(pool) { */
        /* } */
        int interval_us;
        HandlerT handler;
        ThreadPoolPtr pool = nullptr;
    };

    TimerContext(boost::asio::io_service& io, int interval_us, HandlerT& handler, ThreadPoolPtr pool)
        : io_(io), interval_(interval_us), handler_(handler), timer_(io, interval_), pool_(pool) {
    }
    TimerContext(boost::asio::io_service& io, Context& context)
        : io_(io),
          interval_(context.interval_us),
          handler_(context.handler),
          timer_(io, interval_),
          pool_(context.pool) {
    }

    void
    Reschedule(const boost::system::error_code& ec);

    boost::asio::io_service& io_;
    boost::posix_time::microseconds interval_;
    boost::asio::deadline_timer timer_;
    HandlerT handler_;
    ThreadPoolPtr pool_;
};

inline void
TimerContext::Reschedule(const boost::system::error_code& ec) {
    try {
        pool_->enqueue(handler_, ec);
    } catch (std::exception& ex) {
        LOG_SERVER_ERROR_ << "Fail to enqueue handler: " << std::string(ex.what());
    }
    boost::system::error_code e;
    auto new_expires = timer_.expires_at() + interval_;
    timer_.expires_at(new_expires, e);
    if (e) {
        LOG_SERVER_ERROR_ << "Fail to Reschedule: " << e;
    }
    timer_.async_wait(std::bind(&TimerContext::Reschedule, this, std::placeholders::_1));
}

using TimerContextPtr = std::shared_ptr<TimerContext>;

}  // namespace milvus
