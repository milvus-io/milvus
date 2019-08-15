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

#include "../TaskTable.h"
#include "../task/Task.h"
#include "../Cost.h"
#include "Node.h"
#include "Connection.h"


namespace zilliz {
namespace milvus {
namespace engine {

enum class ResourceType {
    DISK = 0,
    CPU = 1,
    GPU = 2
};

class Resource : public Node {
public:
    void
    Start() {
        loader_thread_ = std::thread(&Resource::loader_function, this);
        executor_thread_ = std::thread(&Resource::executor_function, this);
    }

    void
    Stop() {
        running_ = false;
        WakeupLoader();
        WakeupExecutor();
    }

    TaskTable &
    task_table() {
        return task_table_;
    }

public:
    /*
     * wake up executor;
     */
    void
    WakeupExecutor() {
        exec_cv_.notify_one();
    }

    /* 
     * wake up loader;
     */
    void
    WakeupLoader() {
        load_cv_.notify_one();
    }

public:
    /*
     * Event function MUST be a short function, never blocking;
     */

    /*
     * Register on start up event;
     */
    void
    RegisterOnStartUp(std::function<void(void)> func) {
        on_start_up_ = func;
    }

    /*
     * Register on finish one task event;
     */
    void
    RegisterOnFinishTask(std::function<void(void)> func) {
        on_finish_task_ = func;
    }

    /*
     * Register on copy task data completed event;
     */
    void
    RegisterOnCopyCompleted(std::function<void(void)> func) {
        on_copy_completed_ = func;
    }

    /*
     * Register on task table updated event;
     */
    void
    RegisterOnTaskTableUpdated(std::function<void(void)> func) {
        on_task_table_updated_ = func;
    }

protected:
    Resource(std::string name, ResourceType type)
        : name_(std::move(name)),
          type_(type),
          on_start_up_(nullptr),
          on_finish_task_(nullptr),
          on_copy_completed_(nullptr),
          on_task_table_updated_(nullptr),
          running_(false),
          load_flag_(false),
          exec_flag_(false) {
    }

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
    TaskPtr
    pick_task_load() {
        auto indexes = PickToLoad(task_table_, 3);
        for (auto index : indexes) {
            // try to set one task loading, then return
            if (task_table_.Load(index))
                return task_table_.Get(index).task;
            // else try next
        }
        return nullptr;
    }

    /*
     * Pick one task to execute;
     * Pick by start time and priority;
     */
    TaskPtr
    pick_task_execute() {
        auto indexes = PickToExecute(task_table_, 3);
        for (auto index : indexes) {
            // try to set one task executing, then return
            if (task_table_.Execute(index))
                return task_table_.Get(index).task;
            // else try next
        }
        return nullptr;
    }

private:
    /*
     * Only called by load thread;
     */
    void
    loader_function() {
        while (running_) {
            std::unique_lock<std::mutex> lock(load_mutex_);
            load_cv_.wait(lock, [&] { return load_flag_; });
            auto task = pick_task_load();
            if (task) {
                LoadFile(task);
                on_copy_completed_();
            }
        }

    }

    /*
     * Only called by worker thread;
     */
    void
    executor_function() {
        on_start_up_();
        while (running_) {
            std::unique_lock<std::mutex> lock(exec_mutex_);
            exec_cv_.wait(lock, [&] { return exec_flag_; });
            auto task = pick_task_execute();
            if (task) {
                Process(task);
                on_finish_task_();
            }
        }
    }


private:
    std::string name_;
    ResourceType type_;

    TaskTable task_table_;

    std::function<void(void)> on_start_up_;
    std::function<void(void)> on_finish_task_;
    std::function<void(void)> on_copy_completed_;
    std::function<void(void)> on_task_table_updated_;

    bool running_;
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

