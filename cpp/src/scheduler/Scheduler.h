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


namespace zilliz {
namespace milvus {
namespace engine {


class Scheduler {
public:
    explicit
    Scheduler(ResourceMgrWPtr res_mgr)
        : running_(false),
          res_mgr_(std::move(res_mgr)) {
//        res_mgr.Register();
//        res_mgr.Register();
//        res_mgr.Register();
//        res_mgr.Register();
    }

    void
    Start() {
        worker_thread_ = std::thread(&Scheduler::worker_thread_, this);
    }

    std::string
    Dump();

private:
    /******** Events ********/

    /*
     * Process start up events;
     */
    void
    OnStartUp(const EventPtr &event);

    /*
     * Process finish task events;
     */
    void
    OnFinishTask(const EventPtr &event);

    /*
     * Process copy completed events;
     */
    void
    OnCopyCompleted(const EventPtr &event);

    /*
     * Process task table updated events;
     */
    void
    OnTaskTableUpdated(const EventPtr &event);

private:
    /*
     * Dispatch event to event handler;
     */
    void
    Process(const EventPtr &event) {
        switch (event->Type()) {
            case EventType::START_UP: {
                OnStartUp(event);
                break;
            }
            case EventType::COPY_COMPLETED: {
                OnCopyCompleted(event);
                break;
            }
            case EventType::FINISH_TASK: {
                OnFinishTask(event);
                break;
            }
            case EventType::TASK_TABLE_UPDATED: {
                OnTaskTableUpdated(event);
                break;
            }
            default: {
                break;
            }
        }
    }

    /*
     * Called by worker_thread_;
     */
    void
    worker_function() {
        while (running_) {
            auto event = event_queue_.front();
            Process(event);
        }
    }

private:
    bool running_;

    ResourceMgrWPtr res_mgr_;
    std::queue<EventPtr> event_queue_;
    std::thread worker_thread_;
};

using SchedulerPtr = std::shared_ptr<Scheduler>;

}
}
}

