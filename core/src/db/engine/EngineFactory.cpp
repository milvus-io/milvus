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

#include "db/engine/EngineFactory.h"
#include "db/engine/ExecutionEngineImpl.h"
#include "db/snapshot/Snapshots.h"
#include "utils/Log.h"

#include <memory>

namespace milvus {
namespace engine {

SSExecutionEnginePtr
EngineFactory::Build(const std::string& dir_root, const std::string& collection_name, int64_t segment_id) {
    snapshot::ScopedSnapshotT ss;
    snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    auto seg_visitor = engine::SegmentVisitor::Build(ss, segment_id);

    SSExecutionEnginePtr execution_engine_ptr = std::make_shared<ExecutionEngineImpl>(dir_root, seg_visitor);

    return execution_engine_ptr;
}

}  // namespace engine
}  // namespace milvus
