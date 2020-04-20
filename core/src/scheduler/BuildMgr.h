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

#include <atomic>
#include <condition_variable>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace scheduler {

class BuildMgr {
 public:
    explicit BuildMgr(int64_t concurrent_limit) : available_(concurrent_limit) {
    }

 public:
    void
    Put() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++available_;
    }

    bool
    Take() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (available_ < 1) {
            return false;
        } else {
            --available_;
            return true;
        }
    }

    int64_t
    NumOfAvailable() {
        return available_;
    }

 private:
    std::int64_t available_;
    std::mutex mutex_;
};

using BuildMgrPtr = std::shared_ptr<BuildMgr>;

}  // namespace scheduler
}  // namespace milvus
