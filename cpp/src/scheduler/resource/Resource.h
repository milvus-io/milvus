/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <functional>
#include <condition_variable>

#include "../event/Event.h"
#include "../event/StartUpEvent.h"
#include "../event/LoadCompletedEvent.h"
#include "../event/FinishTaskEvent.h"
#include "../event/TaskTableUpdatedEvent.h"
#include "../TaskTable.h"
#include "../task/Task.h"
#include "Connection.h"
#include "Node.h"


namespace zilliz {
namespace milvus {
namespace engine {

// TODO(wxyu): Storage, Route, Executor
enum class ResourceType {
    DISK = 0,
    CPU = 1,
    GPU = 2
};

class Resource : public Node, public std::enable_shared_from_this<Resource> {
 public:
    /*
     * Start loader and executor if enable;
     */
    void
    Start();

    /*
     * Stop loader and executor, join it, blocking util thread exited;
     */
    void
    Stop();

    /*
     * wake up loader;
     */
    void
    WakeupLoader();

    /*
     * wake up executor;
     */
    void
    WakeupExecutor();

    inline void
    RegisterSubscriber(std::function<void(EventPtr)> subscriber) {
        subscriber_ = std::move(subscriber);
    }

    inline virtual std::string
    Dump() const {
        return "<Resource>";
    }

 public:
    inline std::string
    name() const {
        return name_;
    }

    inline ResourceType
    type() const {
        return type_;
    }

    inline uint64_t
    device_id() const {
        return device_id_;
    }

    TaskTable &
    task_table() {
        return task_table_;
    }

public:
    inline bool
    HasLoader() const {
        return enable_loader_;
    }

    inline bool
    HasExecutor() const {
        return enable_executor_;
    }

    // TODO: const
    uint64_t
    NumOfTaskToExec();

    // TODO: need double ?
    inline uint64_t
    TaskAvgCost() const {
        return total_cost_ / total_task_;
    }

    inline uint64_t
    TotalTasks() const {
        return total_task_;
    }

    friend std::ostream &operator<<(std::ostream &out, const Resource &resource);

 protected:
    Resource(std::string name,
             ResourceType type,
             uint64_t device_id,
             bool enable_loader,
             bool enable_executor);

    // TODO: SearchContextPtr to TaskPtr
    /*
     * Implementation by inherit class;
     * Blocking function;
     */
    virtual void
    LoadFile(TaskPtr task) = 0;

    /*
     * Implementation by inherit class;
     * Blocking function;
     */
    virtual void
    Process(TaskPtr task) = 0;

 private:
    /*
     * These function should move to cost.h ???
     * COST.H ???
     */

    /*
     * Pick one task to load;
     * Order by start time;
     */
    TaskTableItemPtr
    pick_task_load();

    /*
     * Pick one task to execute;
     * Pick by start time and priority;
     */
    TaskTableItemPtr
    pick_task_execute();

 private:
    /*
     * Only called by load thread;
     */
    void
    loader_function();

    /*
     * Only called by worker thread;
     */
    void
    executor_function();

 protected:
    uint64_t device_id_;
    std::string name_;

 private:
    ResourceType type_;

    TaskTable task_table_;

    uint64_t total_cost_ = 0;
    uint64_t total_task_ = 0;

    std::function<void(EventPtr)> subscriber_ = nullptr;

    bool running_ = false;
    bool enable_loader_ = true;
    bool enable_executor_ = true;
    std::thread loader_thread_;
    std::thread executor_thread_;

    bool load_flag_ = false;
    bool exec_flag_ = false;
    std::mutex load_mutex_;
    std::mutex exec_mutex_;
    std::condition_variable load_cv_;
    std::condition_variable exec_cv_;
};

using ResourcePtr = std::shared_ptr<Resource>;
using ResourceWPtr = std::weak_ptr<Resource>;

}
}
}

