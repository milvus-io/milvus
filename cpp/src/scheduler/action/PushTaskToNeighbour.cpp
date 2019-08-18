/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "Action.h"


namespace zilliz {
namespace milvus {
namespace engine {

void
push_task(ResourcePtr &self, ResourcePtr &other) {
    auto self_task_table = self->task_table();
    auto other_task_table = other->task_table();
    if (!other_task_table.Empty()) {
        CacheMgr cache;
        auto indexes = PickToMove(self_task_table, cache, 1);
        for (auto index : indexes) {
            if (self_task_table.Move(index)) {
                auto task = self_task_table.Get(index)->task;
                other_task_table.Put(task);
                // TODO: mark moved future
                other->WakeupLoader();
                other->WakeupExecutor();
            }
        }
    }
}

void
Action::PushTaskToNeighbour(const ResourceWPtr &res) {
    if (auto self = res.lock()) {
        for (auto &neighbour : self->GetNeighbours()) {
            if (auto n = neighbour.neighbour_node.lock()) {
                auto neighbour = std::static_pointer_cast<Resource>(n);
                push_task(self, neighbour);
            }
        }
    }
}


}
}
}

