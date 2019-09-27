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
#include "../Algorithm.h"
#include "src/cache/GpuCacheMgr.h"
#include "Action.h"

namespace zilliz {
namespace milvus {
namespace scheduler {

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
        std::vector<uint64_t> speeds;
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
Action::PushTaskToResource(const TaskPtr &task, const ResourcePtr &dest) {
    dest->task_table().Put(task);
}

void
Action::DefaultLabelTaskScheduler(ResourceMgrWPtr res_mgr,
                                  ResourcePtr resource,
                                  std::shared_ptr<LoadCompletedEvent> event) {
    if (not resource->HasExecutor() && event->task_table_item_->Move()) {
        auto task = event->task_table_item_->task;
        auto search_task = std::static_pointer_cast<XSearchTask>(task);
        bool moved = false;

        //to support test task, REFACTOR
        if (auto index_engine = search_task->index_engine_) {
            auto location = index_engine->GetLocation();

            for (auto i = 0; i < res_mgr.lock()->GetNumGpuResource(); ++i) {
                auto index = zilliz::milvus::cache::GpuCacheMgr::GetInstance(i)->GetIndex(location);
                if (index != nullptr) {
                    moved = true;
                    auto dest_resource = res_mgr.lock()->GetResource(ResourceType::GPU, i);
                    PushTaskToResource(event->task_table_item_->task, dest_resource);
                    break;
                }
            }
        }

        if (not moved) {
            PushTaskToNeighbourRandomly(task, resource);
        }
    }
}

void
Action::SpecifiedResourceLabelTaskScheduler(ResourceMgrWPtr res_mgr,
                                            ResourcePtr resource,
                                            std::shared_ptr<LoadCompletedEvent> event) {
    auto task = event->task_table_item_->task;
    if (resource->type() == ResourceType::DISK) {
        // step 1: calculate shortest path per resource, from disk to compute resource
        auto compute_resources = res_mgr.lock()->GetComputeResources();
        std::vector<std::vector<std::string>> paths;
        std::vector<uint64_t> transport_costs;
        for (auto &res : compute_resources) {
            std::vector<std::string> path;
            uint64_t transport_cost = ShortestPath(resource, res, res_mgr.lock(), path);
            transport_costs.push_back(transport_cost);
            paths.emplace_back(path);
        }

        // step 2: select min cost, cost(resource) = avg_cost * task_to_do + transport_cost
        uint64_t min_cost = std::numeric_limits<uint64_t>::max();
        uint64_t min_cost_idx = 0;
        for (uint64_t i = 0; i < compute_resources.size(); ++i) {
            if (compute_resources[i]->TotalTasks() == 0) {
                min_cost_idx = i;
                break;
            }
            uint64_t cost = compute_resources[i]->TaskAvgCost() * compute_resources[i]->NumOfTaskToExec()
                + transport_costs[i];
            if (min_cost > cost) {
                min_cost = cost;
                min_cost_idx = i;
            }
        }

        // step 3: set path in task
        Path task_path(paths[min_cost_idx], paths[min_cost_idx].size() - 1);
        task->path() = task_path;
    }

    if (resource->name() == task->path().Last()) {
        resource->WakeupLoader();
    } else {
        auto next_res_name = task->path().Next();
        auto next_res = res_mgr.lock()->GetResource(next_res_name);
        event->task_table_item_->Move();
        next_res->task_table().Put(task);
    }
}

} // namespace scheduler
} // namespace milvus
} // namespace zilliz
