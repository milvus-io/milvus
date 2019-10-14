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

#pragma once

#include "scheduler/ResourceMgr.h"
#include "scheduler/resource/Resource.h"

#include <memory>

namespace milvus {
namespace scheduler {

class Action {
 public:
    static void
    PushTaskToNeighbourRandomly(const TaskPtr& task, const ResourcePtr& self);

    static void
    PushTaskToAllNeighbour(const TaskPtr& task, const ResourcePtr& self);

    static void
    PushTaskToResource(const TaskPtr& task, const ResourcePtr& dest);

    static void
    DefaultLabelTaskScheduler(ResourceMgrWPtr res_mgr, ResourcePtr resource, std::shared_ptr<LoadCompletedEvent> event);

    static void
    SpecifiedResourceLabelTaskScheduler(ResourceMgrWPtr res_mgr, ResourcePtr resource,
                                        std::shared_ptr<LoadCompletedEvent> event);
};

}  // namespace scheduler
}  // namespace milvus
