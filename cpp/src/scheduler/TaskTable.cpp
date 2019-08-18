/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "TaskTable.h"
#include "event/TaskTableUpdatedEvent.h"
#include <vector>
#include <sstream>


namespace zilliz {
namespace milvus {
namespace engine {


void
TaskTable::Put(TaskPtr task) {
    std::lock_guard<std::mutex> lock(id_mutex_);
    auto item = std::make_shared<TaskTableItem>();
    item->id = id_++;
    item->task = std::move(task);
    item->state = TaskTableItemState::START;
    table_.push_back(item);
    if (subscriber_) {
        subscriber_();
    }
}

void
TaskTable::Put(std::vector<TaskPtr> &tasks) {
    std::lock_guard<std::mutex> lock(id_mutex_);
    for (auto &task : tasks) {
        auto item = std::make_shared<TaskTableItem>();
        item->id = id_++;
        item->task = std::move(task);
        item->state = TaskTableItemState::START;
        table_.push_back(item);
    }
    if (subscriber_) {
        subscriber_();
    }
}


TaskTableItemPtr
TaskTable::Get(uint64_t index) {
    return table_[index];
}

void
TaskTable::Clear() {
// find first task is NOT (done or moved), erase from begin to it;
//        auto iterator = table_.begin();
//        while (iterator->state == TaskTableItemState::EXECUTED or
//            iterator->state == TaskTableItemState::MOVED)
//            iterator++;
//        table_.erase(table_.begin(), iterator);
}

bool
TaskTable::Move(uint64_t index) {
    auto &task = table_[index];

    std::lock_guard<std::mutex> lock(task->mutex);
    if (task->state == TaskTableItemState::LOADED) {
        task->state = TaskTableItemState::MOVING;
        return true;
    }
    return false;
}

bool
TaskTable::Moved(uint64_t index) {
    auto &task = table_[index];

    std::lock_guard<std::mutex> lock(task->mutex);
    if (task->state == TaskTableItemState::MOVING) {
        task->state = TaskTableItemState::MOVED;
        return true;
    }
    return false;
}

bool
TaskTable::Load(uint64_t index) {
    auto &task = table_[index];

    std::lock_guard<std::mutex> lock(task->mutex);
    if (task->state == TaskTableItemState::START) {
        task->state = TaskTableItemState::LOADING;
        return true;
    }
    return false;
}

bool
TaskTable::Loaded(uint64_t index) {
    auto &task = table_[index];

    std::lock_guard<std::mutex> lock(task->mutex);
    if (task->state == TaskTableItemState::LOADING) {
        task->state = TaskTableItemState::LOADED;
        return true;
    }
    return false;
}

bool
TaskTable::Execute(uint64_t index) {
    auto &task = table_[index];

    std::lock_guard<std::mutex> lock(task->mutex);
    if (task->state == TaskTableItemState::LOADED) {
        task->state = TaskTableItemState::EXECUTING;
        return true;
    }
    return false;
}

bool
TaskTable::Executed(uint64_t index) {
    auto &task = table_[index];

    std::lock_guard<std::mutex> lock(task->mutex);
    if (task->state == TaskTableItemState::EXECUTING) {
        task->state = TaskTableItemState::EXECUTED;
        return true;
    }
    return false;
}

std::string
ToString(TaskTableItemState state) {
    switch (state) {
        case TaskTableItemState::INVALID: return "INVALID";
        case TaskTableItemState::START: return "START";
        case TaskTableItemState::LOADING: return "LOADING";
        case TaskTableItemState::LOADED: return "LOADED";
        case TaskTableItemState::EXECUTING: return "EXECUTING";
        case TaskTableItemState::EXECUTED: return "EXECUTED";
        case TaskTableItemState::MOVING: return "MOVING";
        case TaskTableItemState::MOVED: return "MOVED";
        default: return "";
    }
}

std::string
TaskTable::Dump() {
    std::stringstream ss;
    for (auto &item : table_) {
        ss << "<" << item->id;
        ss << ", " << ToString(item->state);
        ss << ">" << std::endl;
    }
    return ss.str();
}

}
}
}
