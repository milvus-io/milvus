// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <list>
#include <random>
#include "cache/GpuCacheMgr.h"
#include "scheduler/Algorithm.h"
#include "scheduler/action/Action.h"

namespace milvus {
namespace scheduler {

std::vector<ResourcePtr>
get_neighbours(const ResourcePtr& self) {
    std::vector<ResourcePtr> neighbours;
    for (auto& neighbour_node : self->GetNeighbours()) {
        auto node = neighbour_node.neighbour_node;
        if (not node) {
            continue;
        }

        auto resource = std::static_pointer_cast<Resource>(node);
        //        if (not resource->HasExecutor()) continue;

        neighbours.emplace_back(resource);
    }
    return neighbours;
}

void
Action::SpecifiedResourceLabelTaskScheduler(const ResourceMgrPtr& res_mgr, ResourcePtr resource,
                                            std::shared_ptr<LoadCompletedEvent> event) {
    auto task_item = event->task_table_item_;
    auto task = event->task_table_item_->task;

    if (resource->name() == task->path().Last()) {
        resource->WakeupExecutor();
    } else {
        auto next_res_name = task->path().Next();
        auto next_res = res_mgr->GetResource(next_res_name);
        //        if (event->task_table_item_->Move()) {
        //            next_res->task_table().Put(task);
        //        }
        event->task_table_item_->Move();
        next_res->task_table().Put(task, task_item);
    }
}

}  // namespace scheduler
}  // namespace milvus
