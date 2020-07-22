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
//#include "db/meta/MetaTypes.h"

#include "server/context/Context.h"

namespace milvus {
namespace scheduler {

// struct SearchTimeStat {
//    double query_time = 0.0;
//    double map_uids_time = 0.0;
//    double reduce_time = 0.0;
//};

class SSSearchJob : public Job {
 public:
    SSSearchJob(const server::ContextPtr& context, const std::string& dir_root, const query::QueryPtr& query_ptr);

 public:
    void
    AddSegmentVisitor(const engine::SegmentVisitorPtr& visitor);

    void
    WaitFinish();

    void
    SearchDone(const engine::snapshot::ID_TYPE seg_id);

    json
    Dump() const override;

 public:
    const server::ContextPtr&
    GetContext() const {
        return context_;
    }

    const milvus::json&
    extra_params() const {
        return extra_params_;
    }

    const std::string&
    dir_root() const {
        return dir_root_;
    }

    const SegmentVisitorMap&
    segment_visitor_map() const {
        return segment_visitor_map_;
    }

    const query::QueryPtr
    query_ptr() const {
        return query_ptr_;
    }

    engine::QueryResultPtr&
    query_result() {
        return query_result_;
    }

    Status&
    status() {
        return status_;
    }

    std::mutex&
    mutex() {
        return mutex_;
    }

    // SearchTimeStat&
    // time_stat() {
    //     return time_stat_;
    // }

 private:
    const server::ContextPtr context_;

    milvus::json extra_params_;

    std::string dir_root_;
    SegmentVisitorMap segment_visitor_map_;

    query::QueryPtr query_ptr_;

    engine::QueryResultPtr query_result_;
    Status status_;

    std::mutex mutex_;
    std::condition_variable cv_;

    // SearchTimeStat time_stat_;
};

using SSSearchJobPtr = std::shared_ptr<SSSearchJob>;

}  // namespace scheduler
}  // namespace milvus
