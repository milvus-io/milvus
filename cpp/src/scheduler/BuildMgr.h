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
    explicit BuildMgr(int64_t numoftasks) : numoftasks_(numoftasks) {
    }

 public:
    void
    Put() {
        ++numoftasks_;
    }

    void
    take() {
        --numoftasks_;
    }

    int64_t
    numoftasks() {
        return (int64_t)numoftasks_;
    }

 private:
    std::atomic_long numoftasks_;
};

using BuildMgrPtr = std::shared_ptr<BuildMgr>;

}  // namespace scheduler
}  // namespace milvus
