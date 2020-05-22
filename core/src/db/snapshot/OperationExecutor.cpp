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

#include "OperationExecutor.h"
#include <iostream>

namespace milvus {
namespace engine {
namespace snapshot {


OperationExecutor::OperationExecutor() {}

OperationExecutor::~OperationExecutor() {
    Stop();
}

OperationExecutor&
OperationExecutor::GetInstance() {
    static OperationExecutor executor;
    return executor;
}

bool
OperationExecutor::Submit(OperationsPtr operation, bool sync) {
    if (!operation) return true;
    /* Store::GetInstance().Apply(*operation); */
    /* return true; */
    Enqueue(operation);
    if (sync)
        return operation->WaitToFinish();
    return true;
}

void
OperationExecutor::Start() {
    if (executor_) return;
    auto queue = std::make_shared<OperationQueueT>();
    auto t = std::make_shared<std::thread>(&OperationExecutor::ThreadMain, this, queue);
    executor_ = std::make_shared<Executor>(t, queue);
    stopped_ = false;
    /* std::cout << "OperationExecutor Started" << std::endl; */
}

void
OperationExecutor::Stop() {
    if (stopped_ || !executor_) return;

    executor_->execute_queue->Put(nullptr);
    executor_->execute_thread->join();
    stopped_ = true;
    executor_ = nullptr;
    std::cout << "OperationExecutor Stopped" << std::endl;
}

void
OperationExecutor::Enqueue(OperationsPtr operation) {
    /* std::cout << std::this_thread::get_id() << " Enqueue Operation " << operation->GetID() << std::endl; */
    executor_->execute_queue->Put(operation);
}

void
OperationExecutor::ThreadMain(OperationQueuePtr queue) {
    if (!queue) return;

    while (true) {
        OperationsPtr operation = queue->Take();
        if (!operation) {
            std::cout << "Stopping operation executor thread " << std::this_thread::get_id() << std::endl;
            break;
        }
        /* std::cout << std::this_thread::get_id() << " Dequeue Operation " << operation->GetID() << std::endl; */

        Store::GetInstance().Apply(*operation);
    }
}


} // snapshot
} // engine
} // milvus
