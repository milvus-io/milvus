/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <vector>
#include <deque>
#include <mutex>

#include "task/SearchTask.h"


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

using TaskTableItemPtr = std::shared_ptr<TaskTableItem>;

class TaskTable {
public:
    TaskTable() = default;

    explicit
    TaskTable(std::vector<TaskPtr> &&tasks);

    /*
     * Put one task;
     */
    void
    Put(TaskPtr task);

    /*
     * Put tasks back of task table;
     * Called by DBImpl;
     */
    void
    Put(std::vector<TaskPtr> &tasks);

    /*
     * Return task table item reference;
     */
    TaskTableItemPtr
    Get(uint64_t index);

    /*
     * TODO
     * Remove sequence task which is DONE or MOVED from front;
     * Called by ?
     */
    void
    Clear();

    /*
     * Return true if task table empty, otherwise false;
     */
    inline bool
    Empty() {
        return table_.empty();
    }

    /*
     * Return size of task table;
     */
    inline size_t
    Size() {
        return table_.size();
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
    Move(uint64_t index);

    /*
     * Move task finished;
     * Set state moved;
     * Called by scheduler;
     */
    bool
    Moved(uint64_t index);

    /*
     * Load a task;
     * Set state loading;
     * Called by loader;
     */
    bool
    Load(uint64_t index);

    /*
     * Load task finished;
     * Set state loaded;
     * Called by loader;
     */
    bool
    Loaded(uint64_t index);

    /*
     * Execute a task;
     * Set state executing;
     * Called by executor;
     */
    bool
    Execute(uint64_t index);

    /*
     * Execute task finished;
     * Set state executed;
     * Called by executor;
     */
    bool
    Executed(uint64_t index);

public:
    /*
     * Dump;
     */
    std::string
    Dump();

private:
    // TODO: map better ?
    std::deque<TaskTableItemPtr> table_;
};


}
}
}
