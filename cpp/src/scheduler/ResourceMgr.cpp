
/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ResourceMgr.h"
#include "db/Log.h"


namespace zilliz {
namespace milvus {
namespace engine {

ResourceMgr::ResourceMgr()
    : running_(false) {

}

uint64_t
ResourceMgr::GetNumOfComputeResource() {
    uint64_t count = 0;
    for (auto &res : resources_) {
        if (res->HasExecutor()) {
            ++count;
        }
    }
    return count;
}

ResourceWPtr
ResourceMgr::Add(ResourcePtr &&resource) {
    ResourceWPtr ret(resource);

    std::lock_guard<std::mutex> lck(resources_mutex_);
    if (running_) {
        ENGINE_LOG_ERROR << "ResourceMgr is running, not allow to add resource";
        return ret;
    }

    if (resource->Type() == ResourceType::DISK) {
        disk_resources_.emplace_back(ResourceWPtr(resource));
    }
    resources_.emplace_back(resource);

    size_t index = resources_.size() - 1;
    resource->RegisterSubscriber(std::bind(&ResourceMgr::PostEvent, this, std::placeholders::_1));
    return ret;
}

void
ResourceMgr::Connect(ResourceWPtr &res1, ResourceWPtr &res2, Connection &connection) {
    if (auto observe_a = res1.lock()) {
        if (auto observe_b = res2.lock()) {
            observe_a->AddNeighbour(std::static_pointer_cast<Node>(observe_b), connection);
            observe_b->AddNeighbour(std::static_pointer_cast<Node>(observe_a), connection);
        }
    }
}


void
ResourceMgr::Start() {
    std::lock_guard<std::mutex> lck(resources_mutex_);
    for (auto &resource : resources_) {
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
    for (auto &resource : resources_) {
        resource->Stop();
    }
}

void
ResourceMgr::PostEvent(const EventPtr &event) {
    std::lock_guard<std::mutex> lock(event_mutex_);
    queue_.emplace(event);
    event_cv_.notify_one();
}

std::string
ResourceMgr::Dump() {
    std::string str = "ResourceMgr contains " + std::to_string(resources_.size()) + " resources.\n";

    for (uint64_t i = 0; i < resources_.size(); ++i) {
        str += "Resource No." + std::to_string(i) + ":\n";
        //str += resources_[i]->Dump();
    }

    return str;
}

std::string
ResourceMgr::DumpTaskTables() {
    std::stringstream ss;
    ss << ">>>>>>>>>>>>>>>ResourceMgr::DumpTaskTable<<<<<<<<<<<<<<<" << std::endl;
    for (auto &resource : resources_) {
        ss << resource->Dump() << std::endl;
        ss << resource->task_table().Dump();
        ss << resource->Dump() << std::endl << std::endl;
    }
    return ss.str();
}

void
ResourceMgr::event_process() {
    while (running_) {
        std::unique_lock<std::mutex> lock(event_mutex_);
        event_cv_.wait(lock, [this] { return !queue_.empty(); });

        auto event = queue_.front();
        queue_.pop();
        lock.unlock();
        if (event == nullptr) {
            break;
        }

//        ENGINE_LOG_DEBUG << "ResourceMgr process " << *event;

        if (subscriber_) {
            subscriber_(event);
        }
    }
}

}
}
}
