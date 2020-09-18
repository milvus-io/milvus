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

#include <memory>
#include <string>
#include <vector>

#include "db/SnapshotVisitor.h"
#include "db/engine/ExecutionEngine.h"
#include "scheduler/Definition.h"
#include "scheduler/job/SearchJob.h"
#include "scheduler/task/Task.h"

namespace milvus {
namespace scheduler {

class SearchTask : public Task {
 public:
    explicit SearchTask(const server::ContextPtr& context, engine::snapshot::ScopedSnapshotT snapshot,
                        const engine::DBOptions& options, const query::QueryPtr& query_ptr,
                        engine::snapshot::ID_TYPE segment_id, TaskLabelPtr label);

    inline json
    Dump() const override {
        json ret{
            {"type", type_},
            {"segment_id", segment_id_},
        };
        return ret;
    }

    Status
    OnLoad(LoadType type, uint8_t device_id) override;

    Status
    OnExecute() override;

    static void
    MergeTopkToResultSet(const engine::ResultIds& src_ids, const engine::ResultDistances& src_distances, size_t src_k,
                         size_t nq, size_t topk, bool ascending, engine::ResultIds& tar_ids,
                         engine::ResultDistances& tar_distances);

    int64_t
    nq();

    milvus::json
    ExtraParam();

    std::string
    IndexType();

 private:
    void
    CreateExecEngine();

 public:
    const std::shared_ptr<server::Context> context_;
    engine::snapshot::ScopedSnapshotT snapshot_;

    const engine::DBOptions& options_;
    query::QueryPtr query_ptr_;
    engine::snapshot::ID_TYPE segment_id_;
    std::string index_type_;

    engine::ExecutionEnginePtr execution_engine_;

    // distance -- value 0 means two vectors equal, ascending reduce, L2/HAMMING/JACCARD/TONIMOTO ...
    // similarity -- infinity value means two vectors equal, descending reduce, IP
    bool ascending_reduce_ = true;
};

}  // namespace scheduler
}  // namespace milvus
