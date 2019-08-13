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


namespace zilliz {
namespace milvus {
namespace engine {

class Event {
public:
    explicit
    Event(ResourceWPtr &resource)
        : resource_(resource) {}

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
    StartUpEvent(ResourceWPtr &resource)
        : Event(resource) {}

public:
    void
    Process() override;
};

class FinishTaskEvent : public Event {
public:
    explicit
    FinishTaskEvent(ResourceWPtr &resource)
        : Event(resource) {}

public:
    void
    Process() override {
//        for (nei : res->neighbours) {
//            tasks = cost(nei->task_table(), nei->connection, limit = 3)
//            res->task_table()->PutTasks(tasks);
//        }
//        res->WakeUpExec();
    }
};

class CopyCompletedEvent : public Event {
public:
    explicit
    CopyCompletedEvent(ResourceWPtr &resource)
        : Event(resource) {}

public:
    void
    Process() override;
};

class TaskTableUpdatedEvent : public Event {
public:
    explicit
    TaskTableUpdatedEvent(ResourceWPtr &resource)
        : Event(resource) {}

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
    Start() {}

    /******** Events ********/

    /*
     * Process start up events;
     */
    void
    OnStartUp(ResourceWPtr &resource) {
        // call from res_mgr, non-blocking, if queue size over limit, exception!
        auto event = std::make_shared<StartUpEvent>(resource);
        event_queue_.push(event);
    }

    /*
     * Process finish task events;
     */
    void
    OnFinishTask(ResourceWPtr);

    /*
     * Process copy completed events;
     */
    void
    OnCopyCompleted(ResourceWPtr);

    /*
     * Process task table updated events;
     */
    void
    OnTaskTableUpdated(ResourceWPtr);


public:
    std::string
    Dump();


private:
    void
    worker_function() {
        while (running_) {
            auto event = event_queue_.front();
            event->Process();
        }
    }

private:
    bool running_;

    ResourceMgrWPtr res_mgr_;
    std::queue<EventPtr> event_queue_;
    std::thread worker_thread_;
};

}
}
}

