/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <vector>
#include <deque>
#include <mutex>

#include "Task.h"


namespace zilliz {
namespace milvus {
namespace engine {

enum class TaskTableItemState {
    INVALID,
    START, // idle
    LOADING, // loading data from other resource
    LOADED, // ready to exec or move
    EXECUTING, // executing, locking util executed or failed
    EXECUTED, // executed, termination state
    MOVING, // moving to another resource, locking util executed or failed
    MOVED, // moved, termination state
};

struct TaskTableItem {
    TaskTableItem() : id(0), state(TaskTableItemState::INVALID), mutex(), priority(0) {}

    TaskTableItem(const TaskTableItem &src)
    : id(src.id), state(src.state), mutex(), priority(src.priority) {}

    uint64_t id; // auto increment from 0;
    // TODO: add tag into task
    TaskPtr task; // the task;
    TaskTableItemState state; // the state;
    std::mutex mutex;

    uint8_t priority; // just a number, meaningless;
};

class TaskTable {
public:
    TaskTable() = default;

    explicit
    TaskTable(std::vector<TaskPtr> &&tasks) {}

    /*
     * Put one task;
     */
    void
    Put(TaskPtr task) {}

    /*
     * Put tasks back of task table;
     * Called by DBImpl;
     */
    void
    Put(std::vector<TaskPtr> &tasks) {}

    /*
     * Return task table item reference;
     */
    TaskTableItem &
    Get(uint64_t index) {}

    /*
     * TODO
     * Remove sequence task which is DONE or MOVED from front;
     * Called by ?
     */
    void
    Clear() {
        // find first task is NOT (done or moved), erase from begin to it;
//        auto iterator = table_.begin();
//        while (iterator->state == TaskTableItemState::EXECUTED or
//            iterator->state == TaskTableItemState::MOVED)
//            iterator++;
//        table_.erase(table_.begin(), iterator);
    }


public:

    /******** Action ********/
    /*
     * Move a task;
     * Set state moving;
     * Called by scheduler;
     */

    // TODO: bool to Status
    bool
    Move(uint64_t index) {
        auto &task = table_[index];

        std::lock_guard<std::mutex> lock(task.mutex);
        if (task.state == TaskTableItemState::START) {
            task.state = TaskTableItemState::LOADING;
            return true;
        }
        return false;
    }

    /*
     * Move task finished;
     * Set state moved;
     * Called by scheduler;
     */
    bool
    Moved(uint64_t index) {}

    /*
     * Load a task;
     * Set state loading;
     * Called by loader;
     */
    bool
    Load(uint64_t index) {}

    /*
     * Load task finished;
     * Set state loaded;
     * Called by loader;
     */
    bool
    Loaded(uint64_t index) {}

    /*
     * Execute a task;
     * Set state executing;
     * Called by executor;
     */
    bool
    Execute(uint64_t index) {}

    /*
     * Execute task finished;
     * Set state executed;
     * Called by executor;
     */
    bool
    Executed(uint64_t index) {}

public:
    /*
     * Dump;
     */
    std::string
    Dump();

private:
    // TODO: map better ?
    std::deque<TaskTableItem> table_;
};


}
}
}
