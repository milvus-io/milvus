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
    ResourceMgr();

    /******** Management Interface ********/
    inline void
    RegisterSubscriber(std::function<void(EventPtr)> subscriber) {
        subscriber_ = std::move(subscriber);
    }

    std::vector<ResourceWPtr> &
    GetDiskResources() {
        return disk_resources_;
    }

    uint64_t
    GetNumGpuResource() const;

    ResourcePtr
    GetResource(ResourceType type, uint64_t device_id);

    /*
     * Return account of resource which enable executor;
     */
    uint64_t
    GetNumOfComputeResource();

    /*
     * Add resource into Resource Management;
     * Generate functions on events;
     * Functions only modify bool variable, like event trigger;
     */
    ResourceWPtr
    Add(ResourcePtr &&resource);

    void
    Connect(const std::string &res1, const std::string &res2, Connection &connection);

    /*
     * Create connection between A and B;
     */
    void
    Connect(ResourceWPtr &res1, ResourceWPtr &res2, Connection &connection);

    /*
     * Synchronous start all resource;
     * Last, start event process thread;
     */
    void
    Start();

    void
    Stop();

    void
    Clear();

    void
    PostEvent(const EventPtr &event);

    // TODO: add stats interface(low)

public:
    /******** Utlitity Functions ********/

    std::string
    Dump();

    std::string
    DumpTaskTables();

private:
    ResourcePtr
    get_resource_by_name(const std::string &name);

    void
    event_process();

private:
    std::queue<EventPtr> queue_;
    std::function<void(EventPtr)> subscriber_ = nullptr;

    bool running_;

    std::vector<ResourceWPtr> disk_resources_;
    std::vector<ResourcePtr> resources_;
    mutable std::mutex resources_mutex_;
    std::thread worker_thread_;

    std::mutex event_mutex_;
    std::condition_variable event_cv_;

};

using ResourceMgrPtr = std::shared_ptr<ResourceMgr>;
using ResourceMgrWPtr = std::weak_ptr<ResourceMgr>;

}
}
}

