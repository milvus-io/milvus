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

namespace milvus::engine::snapshot {

using ThreadPtr = std::shared_ptr<std::thread>;
using OperationQueue = BlockingQueue<OperationsPtr>;

class OperationExecutor {
 public:
    ~OperationExecutor() {
        Stop();
    }

    static void
    Init(StorePtr store) {
        auto& instance = GetInstanceImpl();
        if (instance.initialized_) {
            return;
        }
        instance.store_ = store;
        instance.initialized_ = true;
    }

    static OperationExecutor&
    GetInstance() {
        auto& instance = GetInstanceImpl();
        if (!instance.initialized_) {
            throw std::runtime_error("OperationExecutor should be init");
        }
        return instance;
    }

    Status
    Submit(const OperationsPtr& operation, bool sync = true) {
        if (!operation) {
            return Status(SS_INVALID_ARGUMENT_ERROR, "Invalid Operation");
        }
        Enqueue(operation);
        if (sync) {
            return operation->WaitToFinish();
        }
        return Status::OK();
    }

    void
    Start() {
        if (thread_ptr_ == nullptr) {
            thread_ptr_ = std::make_shared<std::thread>(&OperationExecutor::ThreadMain, this);
        }
    }

    void
    Stop() {
        if (thread_ptr_ != nullptr) {
            Enqueue(nullptr);
            thread_ptr_->join();
            thread_ptr_ = nullptr;
            LOG_ENGINE_INFO_ << "OperationExecutor Stopped";
        }
    }

 private:
    OperationExecutor() = default;
    OperationExecutor(const OperationExecutor&) = delete;

    static OperationExecutor&
    GetInstanceImpl() {
        static OperationExecutor executor;
        return executor;
    }

    void
    ThreadMain() {
        while (true) {
            OperationsPtr operation = queue_.Take();
            if (!operation) {
                break;
            }
            store_->Apply(*operation);
        }
    }

    void
    Enqueue(const OperationsPtr& operation) {
        queue_.Put(operation);
    }

 private:
    ThreadPtr thread_ptr_ = nullptr;
    OperationQueue queue_;
    std::atomic_bool initialized_ = false;
    StorePtr store_;
};

}  // namespace milvus::engine::snapshot
