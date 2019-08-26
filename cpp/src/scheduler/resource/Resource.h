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
#include "../event/CopyCompletedEvent.h"
#include "../event/FinishTaskEvent.h"
#include "../event/TaskTableUpdatedEvent.h"
#include "../TaskTable.h"
#include "../task/Task.h"
#include "../Cost.h"
#include "Connection.h"
#include "Node.h"
#include "RegisterHandler.h"


namespace zilliz {
namespace milvus {
namespace engine {

// TODO(wxyu): Storage, Route, Executor
enum class ResourceType {
    DISK = 0,
    CPU = 1,
    GPU = 2
};

enum class RegisterType {
    START_UP,
    ON_FINISH_TASK,
    ON_COPY_COMPLETED,
    ON_TASK_TABLE_UPDATED,
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

public:
    template<typename T>
    void Register_T(const RegisterType &type) {
        register_table_.emplace(type, [] { return std::make_shared<T>(); });
    }

    RegisterHandlerPtr
    GetRegisterFunc(const RegisterType &type);

    inline void
    RegisterSubscriber(std::function<void(EventPtr)> subscriber) {
        subscriber_ = std::move(subscriber);
    }

    inline std::string
    Name() const {
        return name_;
    }

    inline ResourceType
    Type() const {
        return type_;
    }

    // TODO: better name?
    inline bool
    HasLoader() {
        return enable_loader_;
    }

    // TODO: better name?
    inline bool
    HasExecutor() {
        return enable_executor_;
    }

    TaskTable &
    task_table();

    inline virtual std::string
    Dump() const {
        return "<Resource>";
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

private:
    std::string name_;
    ResourceType type_;

    TaskTable task_table_;

    std::map<RegisterType, std::function<RegisterHandlerPtr()>> register_table_;
    std::function<void(EventPtr)> subscriber_ = nullptr;

    bool running_;
    bool enable_loader_ = true;
    bool enable_executor_ = true;
    std::thread loader_thread_;
    std::thread executor_thread_;

    bool load_flag_;
    bool exec_flag_;
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

