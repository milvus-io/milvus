/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <vector>
#include "task/Task.h"
#include "TaskTable.h"
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
std::vector<uint64_t>
PickToMove(const TaskTable &task_table, const CacheMgr &cache_mgr, uint64_t limit);


/*
 * select task to load
 * call from resource;
 * I DONT SURE NEED THIS;
 */
std::vector<uint64_t>
PickToLoad(const TaskTable &task_table, uint64_t limit);

/*
 * select task to execute;
 * call from resource;
 * I DONT SURE NEED THIS;
 */
std::vector<uint64_t>
PickToExecute(const TaskTable &task_table, uint64_t limit);


}
}
}
