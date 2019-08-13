
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
#include <condition_variable>

#include "resource/Resource.h"

namespace zilliz {
namespace milvus {
namespace engine {

class ResourceMgr {
public:
    ResourceMgr();

    /******** Management Interface ********/

    /*
     * Add resource into Resource Management;
     * Generate functions on events;
     * Functions only modify bool variable, like event trigger;
     */
    ResourceWPtr
    Add(ResourcePtr &&resource);

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
    RegisterOnCopyCompleted(std::function<void(ResourceWPtr)> &func) {
        on_copy_completed_ = func;
    }

    /*
     * Register on task table updated event;
     */
    void
    RegisterOnTaskTableUpdated(std::function<void(ResourceWPtr)> &func) {
        on_task_table_updated_ = func;
    }

public:
    /******** Utlitity Functions ********/

    std::string
    Dump();

private:
    void
    EventProcess();

private:
    bool running_;

    std::vector<ResourcePtr> resources_;
    mutable std::mutex resources_mutex_;
    std::thread worker_thread_;

    std::condition_variable event_cv_;
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

