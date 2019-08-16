/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "TaskTable.h"
#include <vector>


namespace zilliz {
namespace milvus {
namespace engine {

TaskTable::TaskTable(std::vector<TaskPtr> &&tasks) {

}

void
TaskTable::Put(TaskPtr task) {

}

void
TaskTable::Put(std::vector<TaskPtr> &tasks) {

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
    if (task->state == TaskTableItemState::START) {
        task->state = TaskTableItemState::LOADING;
        return true;
    }
    return false;
}

bool
TaskTable::Moved(uint64_t index) {
    return false;
}

bool
TaskTable::Load(uint64_t index) {
    return false;
}

bool
TaskTable::Loaded(uint64_t index) {
    return false;
}

bool
TaskTable::Execute(uint64_t index) {
    return false;
}

bool
TaskTable::Executed(uint64_t index) {
    return false;
}

std::string
TaskTable::Dump() {
    return std::string();
}

}
}
}
