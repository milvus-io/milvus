/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <vector>
#include "Task.h"
#include "CacheMgr.h"

namespace zilliz {
namespace milvus {
namespace engine {

// TODO: Policy interface
// TODO: collect statistics

/*
 * select tasks to move;
 * call from scheduler;
 */
std::vector<TaskPtr>
PickToMove(const TaskTable &task_table, const CacheMgr &cache_mgr, double limit) {}


/*
 * select task to load
 * call from resource;
 * I DONT SURE NEED THIS;
 */
std::vector<TaskPtr>
PickToLoad(TaskTable task_table, uint64_t limit) {}

/*
 * select task to execute;
 * call from resource;
 * I DONT SURE NEED THIS;
 */
std::vector<TaskPtr>
PickToExecute(TaskTable task_table, uint64_t limit) {}


}
}
}
