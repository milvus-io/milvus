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
#include "BaseResource.h"
#include "utils/BlockingQueue.h"

namespace milvus {
namespace engine {
namespace snapshot {

using ThreadPtr = std::shared_ptr<std::thread>;
using ResourceQueue = BlockingQueue<BaseResourcePtr>;
using ResourceQueuePtr = std::shared_ptr<ResourceQueue>;

class GarbageCollector {
 public:
    GarbageCollector() = default;
    GarbageCollector(const GarbageCollector&) = delete;

    ~GarbageCollector() {
        Stop();
    }

    static GarbageCollector&
    GetInstance() {
        static GarbageCollector inst;
        return inst;
    }

    Status
    Submit(BaseResourcePtr& res) {
        if (res == nullptr) {
            return Status(SS_INVALID_ARGUMENT_ERROR, "Invalid Resource");
        }
        Enqueue(res);
        return Status::OK();
    }

    void
    Start() {
        thread_ = std::thread(&GarbageCollector::ThreadMain, this);
    }

    void
    Stop() {
        Enqueue(nullptr);
        thread_.join();
        std::cout << "GarbageCollector Stopped" << std::endl;
    }

 private:
    void
    ThreadMain() {
        while (true) {
            BaseResourcePtr res = queue_.Take();
            if (res == nullptr) {
                std::cout << "Stopping GarbageCollector thread " << std::this_thread::get_id() << std::endl;
                break;
            }
            std::cout << std::this_thread::get_id() << " Dequeue Resource " << res->ToString() << std::endl;
            //Store::GetInstance().Apply(*operation);
        }
    }

    void
    Enqueue(BaseResourcePtr res) {
        //std::cout << std::this_thread::get_id() << " Enqueue Resource " << res->ToString() << std::endl;
        queue_.Put(res);
    }

 private:
    std::thread thread_;
    ResourceQueue queue_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
