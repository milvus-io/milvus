// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


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
//        if (not resource->HasExecutor()) continue;

        neighbours.emplace_back(resource);
    }
    return neighbours;
}

std::vector<std::pair<ResourcePtr, Connection>>
get_neighbours_with_connetion(const ResourcePtr &self) {
    std::vector<std::pair<ResourcePtr, Connection>> neighbours;
    for (auto &neighbour_node : self->GetNeighbours()) {
        auto node = neighbour_node.neighbour_node.lock();
        if (not node) continue;

        auto resource = std::static_pointer_cast<Resource>(node);
//        if (not resource->HasExecutor()) continue;
        Connection conn = neighbour_node.connection;
        neighbours.emplace_back(std::make_pair(resource, conn));
    }
    return neighbours;
}


void
Action::PushTaskToNeighbourRandomly(const TaskPtr &task,
                                    const ResourcePtr &self) {
    auto neighbours = get_neighbours_with_connetion(self);
    if (not neighbours.empty()) {
        std::vector<uint64_t > speeds;
        uint64_t total_speed = 0;
        for (auto &neighbour : neighbours) {
            uint64_t speed = neighbour.second.speed();
            speeds.emplace_back(speed);
            total_speed += speed;
        }

        unsigned seed1 = std::chrono::system_clock::now().time_since_epoch().count();
        std::mt19937 mt(seed1);
        std::uniform_int_distribution<int> dist(0, total_speed);
        uint64_t index = 0;
        int64_t rd_speed = dist(mt);
        for (uint64_t i = 0; i < speeds.size(); ++i) {
            rd_speed -= speeds[i];
            if (rd_speed <= 0) {
                neighbours[i].first->task_table().Put(task);
                return;
            }
        }

    } else {
        //TODO: process
    }

}

void
Action::PushTaskToAllNeighbour(const TaskPtr &task, const ResourcePtr &self) {
    auto neighbours = get_neighbours(self);
    for (auto &neighbour : neighbours) {
        neighbour->task_table().Put(task);
    }
}

void
Action::PushTaskToResource(const TaskPtr& task, const ResourcePtr& dest) {
    dest->task_table().Put(task);
}

}
}
}

