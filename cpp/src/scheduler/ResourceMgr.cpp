
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
    resource->RegisterSubscriber([&](EventPtr event) {
        queue_.emplace(event);
        std::unique_lock<std::mutex> lock(event_mutex_);
        event_cv_.notify_one();
    });
    return ret;
}

void
ResourceMgr::Connect(ResourceWPtr &res1, ResourceWPtr &res2, Connection &connection) {
    if (auto observe_a = res1.lock()) {
        if (auto observe_b = res2.lock()) {
            observe_a->AddNeighbour(std::static_pointer_cast<Node>(observe_b), connection);
        }
    }
}

void
ResourceMgr::EventProcess() {
    while (running_) {
        std::unique_lock<std::mutex> lock(event_mutex_);
        event_cv_.wait(lock, [this] { return !queue_.empty(); });

        if (!running_) {
            break;
        }

        auto event = queue_.front();
        queue_.pop();
        if (subscriber_) {
            subscriber_(event);
        }
    }
}

void
ResourceMgr::Start() {
    std::lock_guard<std::mutex> lck(resources_mutex_);
    for (auto &resource : resources_) {
        resource->Start();
    }
    worker_thread_ = std::thread(&ResourceMgr::EventProcess, this);

    running_ = true;
}

void
ResourceMgr::Stop() {
    std::lock_guard<std::mutex> lck(resources_mutex_);

    running_ = false;
    worker_thread_.join();

    for (auto &resource : resources_) {
        resource->Stop();
    }
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

}
}
}
