/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <memory>
#include <string>
#include <mutex>
#include <thread>
#include <queue>

#include "resource/Resource.h"
#include "ResourceMgr.h"
#include "utils/Log.h"


namespace zilliz {
namespace milvus {
namespace engine {


class Scheduler {
public:
    explicit
    Scheduler(ResourceMgrWPtr res_mgr);

    Scheduler(const Scheduler &) = delete;
    Scheduler(Scheduler &&) = delete;

    /*
     * Start worker thread;
     */
    void
    Start();

    /*
     * Stop worker thread, join it;
     */
    void
    Stop();

    /*
     * Post event to scheduler event queue;
     */
    void
    PostEvent(const EventPtr &event);

    /*
     * Dump as string;
     */
    std::string
    Dump();

private:
    /******** Events ********/

    /*
     * Process start up events;
     *
     * Actions:
     * Pull task from neighbours;
     */
    void
    OnStartUp(const EventPtr &event);

    /*
     * Process finish task events;
     *
     * Actions:
     * Pull task from neighbours;
     */
    void
    OnFinishTask(const EventPtr &event);

    /*
     * Process copy completed events;
     *
     * Actions:
     * Mark task source MOVED;
     * Pull task from neighbours;
     */
    void
    OnCopyCompleted(const EventPtr &event);

    /*
     * Process task table updated events, which happened on task_table->put;
     *
     * Actions:
     * Push task to neighbours;
     */
    void
    OnTaskTableUpdated(const EventPtr &event);

private:
    /*
     * Dispatch event to event handler;
     */
    void
    Process(const EventPtr &event);

    /*
     * Called by worker_thread_;
     */
    void
    worker_function();

private:
    bool running_;

    ResourceMgrWPtr res_mgr_;
    std::queue<EventPtr> event_queue_;
    std::thread worker_thread_;
    std::mutex event_mutex_;
    std::condition_variable event_cv_;
};

using SchedulerPtr = std::shared_ptr<Scheduler>;

}
}
}

