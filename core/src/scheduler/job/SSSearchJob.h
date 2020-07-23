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
#include "db/meta/MetaTypes.h"
#include "query/GeneralQuery.h"

#include "server/context/Context.h"

namespace milvus {
namespace scheduler {

using engine::meta::SegmentSchemaPtr;

using Id2IndexMap = std::unordered_map<size_t, SegmentSchemaPtr>;

using ResultIds = engine::ResultIds;
using ResultDistances = engine::ResultDistances;

// struct SearchTimeStat {
//    double query_time = 0.0;
//    double map_uids_time = 0.0;
//    double reduce_time = 0.0;
//};

class SSSearchJob : public Job {
 public:
    SSSearchJob(const server::ContextPtr& context, engine::DBOptions options, const query::QueryPtr& query_ptr);

 public:
    void
    AddSegmentVisitor(const engine::SegmentVisitorPtr& visitor);

    void
    WaitResult();

    void
    SearchDone(const engine::snapshot::ID_TYPE seg_id);

    ResultIds&
    GetResultIds();

    ResultDistances&
    GetResultDistances();

    void
    SetVectors(engine::VectorsData& vectors) {
        vectors_ = vectors;
    }

    Status&
    GetStatus();

    json
    Dump() const override;

 public:
    const server::ContextPtr&
    GetContext() const;

    int64_t
    topk() {
        return topk_;
    }

    int64_t
    nq() const {
        return vectors_.vector_count_;
    }

    engine::DBOptions
    options() const {
        return options_;
    }

    std::mutex&
    mutex() {
        return mutex_;
    }

    query::GeneralQueryPtr
    general_query() {
        return general_query_;
    }

    const engine::snapshot::IDS_TYPE&
    segment_ids() {
        return segment_ids_;
    }

    Status&
    status() {
        return status_;
    }

    std::unordered_map<std::string, engine::meta::hybrid::DataType>&
    attr_type() {
        return attr_type_;
    }

    int64_t
    vector_count() {
        return vector_count_;
    }

 private:
    void
    GetSegmentsFromQuery(const query::QueryPtr& query_ptr, engine::snapshot::IDS_TYPE& segment_ids);

 private:
    const server::ContextPtr context_;

    engine::DBOptions options_;

    query::QueryPtr query_ptr_;
    engine::QueryResultPtr query_result_;
    engine::snapshot::IDS_TYPE segment_ids_;

    Status status_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

using SSSearchJobPtr = std::shared_ptr<SSSearchJob>;

}  // namespace scheduler
}  // namespace milvus
