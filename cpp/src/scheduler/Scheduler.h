/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <mutex>
#include <thread>
#include <queue>

#include "resource/Resource.h"
#include "ResourceMgr.h"


namespace zilliz {
namespace milvus {
namespace engine {

class Event {
public:
    explicit
    Event(ResourceWPtr &resource) : resource_(resource) {}

public:
    virtual void
    Process() = 0;

private:
    ResourceWPtr resource_;
};

using EventPtr = std::shared_ptr<Event>;

class StartUpEvent : public Event {
public:
    explicit
    StartUpEvent(ResourceWPtr &resource) : Event(resource) {}

public:
    void
    Process() override;
};

class FinishTaskEvent : public Event {
public:
    explicit
    FinishTaskEvent(ResourceWPtr &resource) : Event(resource) {}

public:
    void
    Process() override;
};

class CopyCompletedEvent : public Event {
public:
    explicit
    CopyCompletedEvent(ResourceWPtr &resource) : Event(resource) {}

public:
    void
    Process() override;
};

class TaskTableUpdatedEvent : public Event {
public:
    explicit
    TaskTableUpdatedEvent(ResourceWPtr &resource) : Event(resource) {}

public:
    void
    Process() override;
};

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
    Start();

public:
    /******** Events ********/

    /*
     * Process start up events;
     */
    inline void
    OnStartUp(ResourceWPtr &resource) {
        auto event = std::make_shared<StartUpEvent>(resource);
        event_queue_.push(event);
    }

    /*
     * Process finish task events;
     */
    inline void
    OnFinishTask(ResourceWPtr &resource) {
        auto event = std::make_shared<FinishTaskEvent>(resource);
        event_queue_.push(event);
    }

    /*
     * Process copy completed events;
     */
    inline void
    OnCopyCompleted(ResourceWPtr &resource) {
        auto event = std::make_shared<CopyCompletedEvent>(resource);
        event_queue_.push(event);
    }

    /*
     * Process task table updated events;
     */
    inline void
    OnTaskTableUpdated(ResourceWPtr &resource) {
        auto event = std::make_shared<TaskTableUpdatedEvent>(resource);
        event_queue_.push(event);
    }


public:
    std::string
    Dump();


private:
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
};

}
}
}

