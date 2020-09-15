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

#include <condition_variable>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Job.h"
#include "db/SnapshotVisitor.h"
#include "db/Types.h"

#include "server/context/Context.h"

namespace milvus {
namespace scheduler {

// struct SearchTimeStat {
//    double query_time = 0.0;
//    double map_uids_time = 0.0;
//    double reduce_time = 0.0;
//};

class SearchJob : public Job {
 public:
    SearchJob(const server::ContextPtr& context, const engine::snapshot::ScopedSnapshotT& snapshot,
              engine::DBOptions options, const query::QueryPtr& query_ptr,
              const engine::snapshot::IDS_TYPE& segment_ids);

 public:
    json
    Dump() const override;

    const server::ContextPtr&
    GetContext() const {
        return context_;
    }

    engine::DBOptions
    options() const {
        return options_;
    }

    const query::QueryPtr
    query_ptr() const {
        return query_ptr_;
    }

    engine::QueryResultPtr&
    query_result() {
        return query_result_;
    }

    const engine::snapshot::IDS_TYPE&
    segment_ids() {
        return segment_ids_;
    }

    std::mutex&
    mutex() {
        return mutex_;
    }

 protected:
    void
    OnCreateTasks(JobTasks& tasks) override;

 private:
    const server::ContextPtr context_;
    engine::snapshot::ScopedSnapshotT snapshot_;
    engine::DBOptions options_;

    query::QueryPtr query_ptr_;
    engine::QueryResultPtr query_result_;
    engine::snapshot::IDS_TYPE segment_ids_;
};

using SearchJobPtr = std::shared_ptr<SearchJob>;

}  // namespace scheduler
}  // namespace milvus
