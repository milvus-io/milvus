/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <queue>
#include <condition_variable>

#include "resource/Resource.h"
#include "utils/Log.h"


namespace zilliz {
namespace milvus {
namespace engine {

class ResourceMgr {
public:
    ResourceMgr() = default;

public:
    /******** Management Interface ********/
    void
    Start();

    void
    Stop();

    ResourceWPtr
    Add(ResourcePtr &&resource);

    bool
    Connect(const std::string &res1, const std::string &res2, Connection &connection);

    void
    Clear();

    inline void
    RegisterSubscriber(std::function<void(EventPtr)> subscriber) {
        subscriber_ = std::move(subscriber);
    }

public:
    /******** Management Interface ********/
    inline std::vector<ResourceWPtr> &
    GetDiskResources() {
        return disk_resources_;
    }

    // TODO: why return shared pointer
    inline std::vector<ResourcePtr>
    GetAllResources() {
        return resources_;
    }

    std::vector<ResourcePtr>
    GetComputeResources();

    ResourcePtr
    GetResource(ResourceType type, uint64_t device_id);

    ResourcePtr
    GetResource(const std::string &name);

    uint64_t
    GetNumOfResource() const;

    uint64_t
    GetNumOfComputeResource() const;

    uint64_t
    GetNumGpuResource() const;

public:
    // TODO: add stats interface(low)

public:
    /******** Utility Functions ********/
    std::string
    Dump();

    std::string
    DumpTaskTables();

private:
    void
    post_event(const EventPtr &event);

    void
    event_process();

private:
    bool running_ = false;

    std::vector<ResourceWPtr> disk_resources_;
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

}
}
}

