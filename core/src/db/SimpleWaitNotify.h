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

#include <condition_variable>
#include <mutex>

namespace milvus {
namespace engine {

struct SimpleWaitNotify {
    bool notified_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;

    void
    Wait() {
        std::unique_lock<std::mutex> lck(mutex_);
        if (!notified_) {
            cv_.wait(lck);
        }
        notified_ = false;
    }

    void
    Wait_Until(const std::chrono::system_clock::time_point& tm_pint) {
        std::unique_lock<std::mutex> lck(mutex_);
        if (!notified_) {
            cv_.wait_until(lck, tm_pint);
        }
        notified_ = false;
    }

    void
    Wait_For(const std::chrono::system_clock::duration& tm_dur) {
        std::unique_lock<std::mutex> lck(mutex_);
        if (!notified_) {
            cv_.wait_for(lck, tm_dur);
        }
        notified_ = false;
    }

    void
    Notify() {
        std::unique_lock<std::mutex> lck(mutex_);
        notified_ = true;
        lck.unlock();
        cv_.notify_one();
    }
};

}  // namespace engine
}  // namespace milvus
