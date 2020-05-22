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
#include "Store.h"
#include "Operations.h"
#include "utils/BlockingQueue.h"
#include <thread>
#include <mutex>
#include <memory>

namespace milvus {
namespace engine {
namespace snapshot {

using ThreadPtr = std::shared_ptr<std::thread>;
using OperationQueueT = server::BlockingQueue<OperationsPtr>;
using OperationQueuePtr = std::shared_ptr<OperationQueueT>;

struct Executor {
    Executor(ThreadPtr t, OperationQueuePtr q) : execute_thread(t), execute_queue(q) {}
    ThreadPtr execute_thread;
    OperationQueuePtr execute_queue;
};

using ExecutorPtr = std::shared_ptr<Executor>;

class OperationExecutor {
public:
    using Ptr = std::shared_ptr<OperationExecutor>;

    OperationExecutor(const OperationExecutor&) = delete;

    static OperationExecutor& GetInstance();

    bool Submit(OperationsPtr operation);

    void Start();

    void Stop();

    ~OperationExecutor();

protected:
    OperationExecutor();

    void ThreadMain(OperationQueuePtr queue);

    void Enqueue(OperationsPtr operation);

    mutable std::mutex mtx_;
    bool stopped_ = false;
    ExecutorPtr executor_;
};

} // snapshot
} // engine
} // milvus
