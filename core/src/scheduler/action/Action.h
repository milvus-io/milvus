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

#pragma once

#include "scheduler/ResourceMgr.h"
#include "scheduler/resource/Resource.h"

#include <memory>

namespace milvus {
namespace scheduler {

class Action {
 public:
    static void
    PushTaskToNeighbourRandomly(TaskTableItemPtr task_item, const ResourcePtr& self);

    static void
    PushTaskToAllNeighbour(TaskTableItemPtr task_item, const ResourcePtr& self);

    static void
    PushTaskToResource(TaskTableItemPtr task_item, const ResourcePtr& dest);

    static void
    SpecifiedResourceLabelTaskScheduler(const ResourceMgrPtr& res_mgr, ResourcePtr resource,
                                        std::shared_ptr<LoadCompletedEvent> event);
};

}  // namespace scheduler
}  // namespace milvus
