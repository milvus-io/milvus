/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include <list>
#include <random>
#include "Action.h"


namespace zilliz {
namespace milvus {
namespace engine {

std::vector<ResourcePtr>
get_neighbours(const ResourcePtr &self) {
    std::vector<ResourcePtr> neighbours;
    for (auto &neighbour_node : self->GetNeighbours()) {
        auto node = neighbour_node.neighbour_node.lock();
        if (not node) continue;

        auto resource = std::static_pointer_cast<Resource>(node);
        neighbours.emplace_back(resource);
    }
    return neighbours;
}


void
Action::PushTaskToNeighbourRandomly(const TaskPtr &task,
                                    const ResourcePtr &self) {
    auto neighbours = get_neighbours(self);
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<uint64_t> dist(0, neighbours.size() - 1);

    neighbours[dist(mt)]->task_table().Put(task);
}

void
Action::PushTaskToAllNeighbour(const TaskPtr &task, const ResourcePtr &self) {
    auto neighbours = get_neighbours(self);
    for (auto &neighbour : neighbours) {
        neighbour->task_table().Put(task);
    }
}


}
}
}

