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

#include "ResourceMgr.h"
#include "Scheduler.h"
#include "JobMgr.h"

#include <mutex>
#include <memory>


namespace zilliz {
namespace milvus {
namespace engine {

class ResMgrInst {
public:
    static ResourceMgrPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<ResourceMgr>();
            }
        }
        return instance;
    }

private:
    static ResourceMgrPtr instance;
    static std::mutex mutex_;
};

class SchedInst {
public:
    static SchedulerPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<Scheduler>(ResMgrInst::GetInstance());
            }
        }
        return instance;
    }

private:
    static SchedulerPtr instance;
    static std::mutex mutex_;
};

class JobMgrInst {
public:
    static scheduler::JobMgrPtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<scheduler::JobMgr>(ResMgrInst::GetInstance());
            }
        }
        return instance;
    }

private:
    static scheduler::JobMgrPtr instance;
    static std::mutex mutex_;
};

void
StartSchedulerService();

void
StopSchedulerService();

}
}
}
