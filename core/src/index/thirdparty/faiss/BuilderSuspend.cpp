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

#include "BuilderSuspend.h"

namespace faiss {

std::atomic<bool> BuilderSuspend::suspend_flag_(false);
std::mutex BuilderSuspend::mutex_;
std::condition_variable BuilderSuspend::cv_;

void BuilderSuspend::suspend() {
    suspend_flag_ = true;
}

void BuilderSuspend::resume() {
    suspend_flag_ = false;
}

void BuilderSuspend::check_wait() {
    while (suspend_flag_) {
        std::unique_lock<std::mutex> lck(mutex_);
        cv_.wait_for(lck, std::chrono::seconds(5));
    }
}

}  // namespace faiss
