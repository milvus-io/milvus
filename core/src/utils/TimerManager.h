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
#include <functional>
#include <memory>
#include <vector>

#include "utils/Status.h"
#include "utils/ThreadPool.h"
#include "utils/TimerContext.h"

namespace milvus {

class TimerManager {
 public:
    TimerManager() = default;
    explicit TimerManager(unsigned int pool_size);

    Status
    SetPoolSize(unsigned int pool_size);

    void
    AddTimer(int interval_us, TimerContext::HandlerT handler);
    void
    AddTimer(const TimerContext::Context& ctx);

    virtual Status
    Run();

    virtual Status
    Start();
    virtual void
    Stop();

    virtual ~TimerManager();

 protected:
    boost::asio::io_service io_;
    ThreadPoolPtr timer_exeutors_;
    unsigned int pool_size_;
    std::vector<TimerContextPtr> timers_;
};

using TimerManagerPtr = std::shared_ptr<TimerManager>;

}  // namespace milvus
