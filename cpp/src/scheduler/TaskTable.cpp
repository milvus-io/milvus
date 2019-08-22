/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "TaskTable.h"
#include "event/TaskTableUpdatedEvent.h"
#include <vector>
#include <sstream>
#include <ctime>


namespace zilliz {
namespace milvus {
namespace engine {

uint64_t
get_now_timestamp() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    return millis;
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
ToString(const TaskTimestamp &timestamp) {
    std::stringstream ss;
    ss << "<start=" << timestamp.start;
    ss << ", load=" << timestamp.load;
    ss << ", loaded=" << timestamp.loaded;
    ss << ", execute=" << timestamp.execute;
    ss << ", executed=" << timestamp.executed;
    ss << ", move=" << timestamp.move;
    ss << ", moved=" << timestamp.moved;
    ss << ">";
    return ss.str();
}

bool
TaskTableItem::Load() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::START) {
        state = TaskTableItemState::LOADING;
        lock.unlock();
        timestamp.load = get_now_timestamp();
        return true;
    }
    return false;
}
bool
TaskTableItem::Loaded() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::LOADING) {
        state = TaskTableItemState::LOADED;
        lock.unlock();
        timestamp.loaded = get_now_timestamp();
        return true;
    }
    return false;
}
bool
TaskTableItem::Execute() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::LOADED) {
        state = TaskTableItemState::EXECUTING;
        lock.unlock();
        timestamp.execute = get_now_timestamp();
        return true;
    }
    return false;
}
bool
TaskTableItem::Executed() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::EXECUTING) {
        state = TaskTableItemState::EXECUTED;
        lock.unlock();
        timestamp.executed = get_now_timestamp();
        return true;
    }
    return false;
}
bool
TaskTableItem::Move() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::LOADED) {
        state = TaskTableItemState::MOVING;
        lock.unlock();
        timestamp.move = get_now_timestamp();
        return true;
    }
    return false;
}
bool
TaskTableItem::Moved() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::MOVING) {
        state = TaskTableItemState::MOVED;
        lock.unlock();
        timestamp.moved = get_now_timestamp();
        return true;
    }
    return false;
}

std::string
TaskTableItem::Dump() {
    std::stringstream ss;
    ss << "<id=" << id;
    ss << ", task=" << task;
    ss << ", state=" << ToString(state);
    ss << ", timestamp=" << ToString(timestamp);
    ss << ">";
    return ss.str();
}

void
TaskTable::Put(TaskPtr task) {
    std::lock_guard<std::mutex> lock(id_mutex_);
    auto item = std::make_shared<TaskTableItem>();
    item->id = id_++;
    item->task = std::move(task);
    item->state = TaskTableItemState::START;
    item->timestamp.start = get_now_timestamp();
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
        item->timestamp.start = get_now_timestamp();
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


std::string
TaskTable::Dump() {
    std::stringstream ss;
    for (auto &item : table_) {
        ss << item->Dump() << std::endl;
    }
    return ss.str();
}

}
}
}
