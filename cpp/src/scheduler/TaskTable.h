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
#include "event/Event.h"


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

struct TaskTimestamp {
    uint64_t start = 0;
    uint64_t move = 0;
    uint64_t moved = 0;
    uint64_t load = 0;
    uint64_t loaded = 0;
    uint64_t execute = 0;
    uint64_t executed = 0;
    uint64_t finish = 0;
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
    TaskTimestamp timestamp;

    uint8_t priority; // just a number, meaningless;

    bool
    Load();

    bool
    Loaded();

    bool
    Execute();

    bool
    Executed();

    bool
    Move();

    bool
    Moved();

    std::string
    Dump();
};

using TaskTableItemPtr = std::shared_ptr<TaskTableItem>;

class TaskTable {
public:
    TaskTable() = default;

    TaskTable(const TaskTable &) = delete;
    TaskTable(TaskTable &&) = delete;

    inline void
    RegisterSubscriber(std::function<void(void)> subscriber) {
        subscriber_ = std::move(subscriber);
    }

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
    TaskTableItemPtr &
    operator[](uint64_t index) {
        return table_[index];
    }

    std::deque<TaskTableItemPtr>::iterator begin() { return table_.begin(); }
    std::deque<TaskTableItemPtr>::iterator end() { return table_.end(); }

public:

    /******** Action ********/

    // TODO: bool to Status
    /*
     * Load a task;
     * Set state loading;
     * Called by loader;
     */
    inline bool
    Load(uint64_t index) {
        return table_[index]->Load();
    }

    /*
     * Load task finished;
     * Set state loaded;
     * Called by loader;
     */
    inline bool
    Loaded(uint64_t index) {
        return table_[index]->Loaded();
    }

    /*
     * Execute a task;
     * Set state executing;
     * Called by executor;
     */
    inline bool
    Execute(uint64_t index) {
        return table_[index]->Execute();
    }

    /*
     * Execute task finished;
     * Set state executed;
     * Called by executor;
     */
    inline bool
    Executed(uint64_t index){
        return table_[index]->Executed();
    }

    /*
     * Move a task;
     * Set state moving;
     * Called by scheduler;
     */

    inline bool
    Move(uint64_t index){
        return table_[index]->Move();
    }

    /*
     * Move task finished;
     * Set state moved;
     * Called by scheduler;
     */
    inline bool
    Moved(uint64_t index){
        return table_[index]->Moved();
    }

public:
    /*
     * Dump;
     */
    std::string
    Dump();

private:
    // TODO: map better ?
    std::uint64_t id_ = 0;
    mutable std::mutex id_mutex_;
    std::deque<TaskTableItemPtr> table_;
    std::function<void(void)> subscriber_ = nullptr;
};


}
}
}
