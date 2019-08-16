/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "Cost.h"


namespace zilliz {
namespace milvus {
namespace engine {

std::vector<uint64_t>
PickToMove(TaskTable &task_table, const CacheMgr &cache_mgr, uint64_t limit) {
    std::vector<uint64_t> indexes;
    for (uint64_t i = 0, count = 0; i < task_table.Size() && count < limit; ++i) {
        if (task_table[i]->state == TaskTableItemState::LOADED) {
            indexes.push_back(i);
            ++count;
        }
    }
    return indexes;
}


std::vector<uint64_t>
PickToLoad(TaskTable &task_table, uint64_t limit) {
    std::vector<uint64_t> indexes;
    for (uint64_t i = 0, count = 0; i < task_table.Size() && count < limit; ++i) {
        if (task_table[i]->state == TaskTableItemState::START) {
            indexes.push_back(i);
            ++count;
        }
    }
    return indexes;
}


std::vector<uint64_t>
PickToExecute(TaskTable &task_table, uint64_t limit) {
    std::vector<uint64_t> indexes;
    for (uint64_t i = 0, count = 0; i < task_table.Size() && count < limit; ++i) {
        if (task_table[i]->state == TaskTableItemState::LOADED) {
            indexes.push_back(i);
            ++count;
        }
    }
    return indexes;
}

}
}
}
