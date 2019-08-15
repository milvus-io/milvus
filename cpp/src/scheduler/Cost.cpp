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
PickToMove(const TaskTable &task_table, const CacheMgr &cache_mgr, uint64_t limit) {
    std::vector<uint64_t> indexes;
    return indexes;
}


std::vector<uint64_t>
PickToLoad(const TaskTable &task_table, uint64_t limit) {
    std::vector<uint64_t> indexes;
    return indexes;
}


std::vector<uint64_t>
PickToExecute(const TaskTable &task_table, uint64_t limit) {
    std::vector<uint64_t> indexes;
    return indexes;
}

}
}
}
