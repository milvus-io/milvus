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

#include "cpp/src/scheduler/optimizer/HybridPass.h"
#include "scheduler/task/SearchTask.h"

namespace milvus {
namespace scheduler {

bool
HybridPass::Run(const TaskPtr& task) {
    // TODO: Index::IVFSQ8Hybrid, if nq < threshold set cpu, else set gpu
    if (task->Type() != TaskType::SearchTask)
        return false;
    auto search_task = std::static_pointer_cast<XSearchTask>(task);
    // if (search_task->file_->engine_type_ == engine::EngineType::FAISS_IVFSQ8Hybrid)
    return false;
}

}  // namespace scheduler
}  // namespace milvus
