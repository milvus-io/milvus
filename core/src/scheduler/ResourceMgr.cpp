
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

#include "scheduler/ResourceMgr.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

void
ResourceMgr::Start() {
    if (not check_resource_valid()) {
        LOG_ENGINE_ERROR_ << "Resources invalid, cannot start ResourceMgr.";
        LOG_ENGINE_ERROR_ << Dump();
        return;
    }

    std::lock_guard<std::mutex> lck(resources_mutex_);
    for (auto& resource : resources_) {
        resource->Start();
    }
    running_ = true;
    worker_thread_ = std::thread(&ResourceMgr::event_process, this);
}

void
ResourceMgr::Stop() {
    {
        std::lock_guard<std::mutex> lock(event_mutex_);
        running_ = false;
        queue_.push(nullptr);
        event_cv_.notify_one();
    }
    worker_thread_.join();

    std::lock_guard<std::mutex> lck(resources_mutex_);
    for (auto& resource : resources_) {
        resource->Stop();
    }
}

ResourceWPtr
ResourceMgr::Add(ResourcePtr&& resource) {
    ResourceWPtr ret(resource);

    std::lock_guard<std::mutex> lck(resources_mutex_);
    if (running_) {
        LOG_ENGINE_ERROR_ << "ResourceMgr is running, not allow to add resource";
        return ret;
    }

    resource->RegisterSubscriber(std::bind(&ResourceMgr::post_event, this, std::placeholders::_1));

    switch (resource->type()) {
        case ResourceType::DISK: {
            disk_resources_.emplace_back(ResourceWPtr(resource));
            break;
        }
        case ResourceType::CPU: {
            cpu_resources_.emplace_back(ResourceWPtr(resource));
            break;
        }
        case ResourceType::GPU: {
            gpu_resources_.emplace_back(ResourceWPtr(resource));
            break;
        }
        default: { break; }
    }
    resources_.emplace_back(resource);

    return ret;
}

bool
ResourceMgr::Connect(const std::string& name1, const std::string& name2, Connection& connection) {
    auto res1 = GetResource(name1);
    auto res2 = GetResource(name2);
    if (res1 && res2) {
        res1->AddNeighbour(std::static_pointer_cast<Node>(res2), connection);
        // TODO(wxyu): enable when task balance supported
        //        res2->AddNeighbour(std::static_pointer_cast<Node>(res1), connection);
        return true;
    }
    return false;
}

void
ResourceMgr::Clear() {
    std::lock_guard<std::mutex> lck(resources_mutex_);
    if (running_) {
        LOG_ENGINE_ERROR_ << "ResourceMgr is running, cannot clear.";
        return;
    }
    disk_resources_.clear();
    cpu_resources_.clear();
    gpu_resources_.clear();
    resources_.clear();
}

std::vector<ResourcePtr>
ResourceMgr::GetComputeResources() {
    std::vector<ResourcePtr> result;
    for (auto& resource : resources_) {
        if (resource->HasExecutor()) {
            result.emplace_back(resource);
        }
    }
    return result;
}

ResourcePtr
ResourceMgr::GetResource(ResourceType type, uint64_t device_id) {
    for (auto& resource : resources_) {
        if (resource->type() == type && resource->device_id() == device_id) {
            return resource;
        }
    }
    return nullptr;
}

ResourcePtr
ResourceMgr::GetResource(const std::string& name) {
    for (auto& resource : resources_) {
        if (resource->name() == name) {
            return resource;
        }
    }
    return nullptr;
}

uint64_t
ResourceMgr::GetNumOfResource() const {
    return resources_.size();
}

uint64_t
ResourceMgr::GetNumOfComputeResource() const {
    uint64_t count = 0;
    for (auto& res : resources_) {
        if (res->HasExecutor()) {
            ++count;
        }
    }
    return count;
}

uint64_t
ResourceMgr::GetNumGpuResource() const {
    uint64_t num = 0;
    for (auto& res : resources_) {
        if (res->type() == ResourceType::GPU) {
            num++;
        }
    }
    return num;
}

json
ResourceMgr::Dump() const {
    json resources{};
    for (auto& res : resources_) {
        resources.push_back(res->Dump());
    }
    json ret{
        {"number_of_resource", resources_.size()},
        {"number_of_disk_resource", disk_resources_.size()},
        {"number_of_cpu_resource", cpu_resources_.size()},
        {"number_of_gpu_resource", gpu_resources_.size()},
        {"resources", resources},
    };
    return ret;
}

std::string
ResourceMgr::DumpTaskTables() {
    std::stringstream ss;
    ss << ">>>>>>>>>>>>>>>ResourceMgr::DumpTaskTable<<<<<<<<<<<<<<<" << std::endl;
    for (auto& resource : resources_) {
        ss << resource->name() << std::endl;
        ss << resource->task_table().Dump().dump();
        ss << resource->name() << std::endl << std::endl;
    }
    return ss.str();
}

bool
ResourceMgr::check_resource_valid() {
    {
        // TODO: check one disk-resource, one cpu-resource, zero or more gpu-resource;
        if (GetDiskResources().size() != 1) {
            return false;
        }
        if (GetCpuResources().size() != 1) {
            return false;
        }
    }

    {
        // TODO: one compute-resource at least;
        if (GetNumOfComputeResource() < 1) {
            return false;
        }
    }

    {
        // TODO: check disk only connect with cpu
    }

    {
        // TODO: check gpu only connect with cpu
    }

    {
        // TODO: check if exists isolated node
    }

    return true;
}

void
ResourceMgr::post_event(const EventPtr& event) {
    {
        std::lock_guard<std::mutex> lock(event_mutex_);
        queue_.emplace(event);
    }
    event_cv_.notify_one();
}

void
ResourceMgr::event_process() {
    SetThreadName("resevt_thread");
    while (running_) {
        std::unique_lock<std::mutex> lock(event_mutex_);
        event_cv_.wait(lock, [this] { return !queue_.empty(); });

        auto event = queue_.front();
        queue_.pop();
        lock.unlock();
        if (event == nullptr) {
            break;
        }

        if (subscriber_) {
            subscriber_(event);
        }
    }
}

}  // namespace scheduler
}  // namespace milvus
