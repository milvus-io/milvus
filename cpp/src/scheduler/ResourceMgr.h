
/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <vector>
#include <memory>


namespace zilliz {
namespace milvus {
namespace engine {

class ResourceMgr {
public:
    ResourceMgr() : running_(false) {}

    /******** Management Interface ********/

    /*
     * Add resource into Resource Management;
     * Generate functions on events;
     * Functions only modify bool variable, like event trigger;
     */
    ResourceWPtr
    Add(ResourcePtr &&resource) {
        ResourceWPtr ret(resource);
        resources_.emplace_back(resource);

//        resource->RegisterOnStartUp([] {
//            start_up_event_[index] = true;
//        });
//        resource.RegisterOnFinishTask([] {
//            finish_task_event_[index] = true;
//        });
        return ret;
    }

    /*
     * Create connection between A and B;
     */
    void
    Connect(ResourceWPtr &A, ResourceWPtr &B, Connection &connection) {
        if (auto observe_a = A.lock()) {
            if (auto observe_b = B.lock()) {
                observe_a->AddNeighbour(std::static_pointer_cast<Node>(observe_b), connection);
            }
        }
    }

    /*
     * Synchronous start all resource;
     * Last, start event process thread;
     */
    void
    StartAll() {
        for (auto &resource : resources_) {
            resource->Start();
        }
        worker_thread_ = std::thread(&ResourceMgr::EventProcess, this);
    }

    // TODO: add stats interface(low)

public:
    /******** Event Register Interface ********/

    /*
     * Register on start up event;
     */
    void
    RegisterOnStartUp(std::function<void(ResourceWPtr)> &func) {
        on_start_up_ = func;
    }

    /*
     * Register on finish one task event;
     */
    void
    RegisterOnFinishTask(std::function<void(ResourceWPtr)> &func) {
        on_finish_task_ = func;
    }

    /*
     * Register on copy task data completed event;
     */
    void
    RegisterOnCopyCompleted(std::function<void(ResourceWPtr)> &func);

    /*
     * Register on task table updated event;
     */
    void
    RegisterOnTaskTableUpdated(std::function<void(ResourceWPtr)> &func);

public:
    /******** Utlitity Functions ********/

    std::string
    Dump();

private:
    void
    EventProcess() {
        while (running_) {
            for (uint64_t i = 0; i < resources_.size(); ++i) {
                if (start_up_event_[i]) {
                    on_start_up_(resources_[i]);
                }
            }
        }

    }

private:
    bool running_;

    std::vector<ResourcePtr> resources_;
    std::thread worker_thread_;

    std::vector<bool> start_up_event_;
    std::vector<bool> finish_task_event_;
    std::vector<bool> copy_completed_event_;
    std::vector<bool> task_table_updated_event_;

    std::function<void(ResourceWPtr)> on_start_up_;
    std::function<void(ResourceWPtr)> on_finish_task_;
    std::function<void(ResourceWPtr)> on_copy_completed_;
    std::function<void(ResourceWPtr)> on_task_table_updated_;
};

using ResourceMgrWPtr = std::weak_ptr<ResourceMgr>;

}
}
}

