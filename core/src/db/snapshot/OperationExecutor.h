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
#include <memory>
#include <mutex>
#include <thread>
#include "Operations.h"
#include "Store.h"
#include "utils/BlockingQueue.h"

namespace milvus {
namespace engine {
namespace snapshot {

using ThreadPtr = std::shared_ptr<std::thread>;
using OperationQueue = server::BlockingQueue<OperationsPtr>;
using OperationQueuePtr = std::shared_ptr<OperationQueue>;

class OperationExecutor {
 public:
    using Ptr = std::shared_ptr<OperationExecutor>;

    OperationExecutor(const OperationExecutor&) = delete;

    static OperationExecutor&
    GetInstance();

    bool
    Submit(OperationsPtr operation, bool sync = true);

    void
    Start();

    void
    Stop();

    ~OperationExecutor();

 protected:
    OperationExecutor();

    void
    ThreadMain();

    void
    Enqueue(OperationsPtr operation);

 protected:
    mutable std::mutex mtx_;
    bool running_ = false;
    std::thread thread_;
    OperationQueue queue_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
