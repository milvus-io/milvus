/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include <list>
#include "Action.h"


namespace zilliz {
namespace milvus {
namespace engine {

void
next(std::list<ResourcePtr> &neighbours, std::list<ResourcePtr>::iterator &it) {
    it++;
    if (neighbours.end() == it) {
        it = neighbours.begin();
    }
}


void
push_task_round_robin(TaskTable &self_task_table, std::list<ResourcePtr> &neighbours) {
    CacheMgr cache;
    auto it = neighbours.begin();
    if (it == neighbours.end()) return;
    auto indexes = PickToMove(self_task_table, cache, self_task_table.Size());

    for (auto index : indexes) {
        if (self_task_table.Move(index)) {
            auto task = self_task_table.Get(index)->task;
            task = task->Clone();
            (*it)->task_table().Put(task);
            next(neighbours, it);
        }
    }
}

void
Action::PushTaskToNeighbour(const ResourceWPtr &res) {
    auto self = res.lock();
    if (not self) return;

    std::list<ResourcePtr> neighbours;
    for (auto &neighbour_node : self->GetNeighbours()) {
        auto node = neighbour_node.neighbour_node.lock();
        if (not node) continue;

        auto resource = std::static_pointer_cast<Resource>(node);
        neighbours.emplace_back(resource);
    }

    push_task_round_robin(self->task_table(), neighbours);
}

void
Action::PushTaskToNeighbourHasExecutor(const ResourceWPtr &res) {
    auto self = res.lock();
    if (not self) return;

    std::list<ResourcePtr> neighbours;
    for (auto &neighbour_node : self->GetNeighbours()) {
        auto node = neighbour_node.neighbour_node.lock();
        if (not node) continue;

        auto resource = std::static_pointer_cast<Resource>(node);
        if (resource->HasExecutor()) {
            neighbours.emplace_back(resource);
        }
    }

    push_task_round_robin(self->task_table(), neighbours);
}


}
}
}

