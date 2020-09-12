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
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "interface/interfaces.h"
#include "resource/Resource.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

class ResourceMgr : public interface::dumpable {
 public:
    ResourceMgr() = default;

 public:
    /******** Management Interface ********/
    void
    Start();

    void
    Stop();

    ResourceWPtr
    Add(ResourcePtr&& resource);

    bool
    Connect(const std::string& name1, const std::string& name2, Connection& connection);

    void
    Clear();

    inline void
    RegisterSubscriber(std::function<void(EventPtr)> subscriber) {
        subscriber_ = std::move(subscriber);
    }

 public:
    /******** Management Interface ********/
    inline std::vector<ResourceWPtr>&
    GetDiskResources() {
        return disk_resources_;
    }

    inline std::vector<ResourceWPtr>&
    GetCpuResources() {
        return cpu_resources_;
    }

    inline std::vector<ResourceWPtr>&
    GetGpuResources() {
        return gpu_resources_;
    }

    inline std::vector<ResourcePtr>
    GetAllResources() {
        return resources_;
    }

    std::vector<ResourcePtr>
    GetComputeResources();

    ResourcePtr
    GetResource(ResourceType type, uint64_t device_id);

    ResourcePtr
    GetResource(const std::string& name);

    uint64_t
    GetNumOfResource() const;

    uint64_t
    GetNumOfComputeResource() const;

    uint64_t
    GetNumGpuResource() const;

 public:
    // TODO(wxyu): add stats interface(low)

 public:
    /******** Utility Functions ********/
    json
    Dump() const override;

    std::string
    DumpTaskTables();

 private:
    bool
    check_resource_valid();

    void
    post_event(const EventPtr& event);

    void
    event_process();

 private:
    bool running_ = false;

    std::vector<ResourceWPtr> disk_resources_;
    std::vector<ResourceWPtr> cpu_resources_;
    std::vector<ResourceWPtr> gpu_resources_;
    std::vector<ResourcePtr> resources_;
    mutable std::mutex resources_mutex_;

    std::queue<EventPtr> queue_;
    std::function<void(EventPtr)> subscriber_ = nullptr;
    std::mutex event_mutex_;
    std::condition_variable event_cv_;

    std::thread worker_thread_;
};

using ResourceMgrPtr = std::shared_ptr<ResourceMgr>;
using ResourceMgrWPtr = std::weak_ptr<ResourceMgr>;

}  // namespace scheduler
}  // namespace milvus
