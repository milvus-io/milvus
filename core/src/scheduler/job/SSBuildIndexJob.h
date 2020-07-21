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
#include <vector>

#include "config/handler/CacheConfigHandler.h"
//#include "db/meta/Meta.h"
#include "scheduler/Definition.h"
#include "scheduler/job/Job.h"

namespace milvus {
namespace scheduler {

// using engine::meta::SegmentSchemaPtr;

// using Id2ToIndexMap = std::unordered_map<size_t, SegmentSchemaPtr>;
// using Id2ToTableFileMap = std::unordered_map<size_t, SegmentSchema>;

class SSBuildIndexJob : public Job, public server::CacheConfigHandler {
 public:
    explicit SSBuildIndexJob(engine::DBOptions options);

    ~SSBuildIndexJob() = default;

 public:
    //    bool
    //    AddToIndexFiles(const SegmentSchemaPtr& to_index_file);

    void
    AddSegmentVisitor(const engine::SegmentVisitorPtr& visitor);

    void
    WaitBuildIndexFinish();

    void
    BuildIndexDone(const engine::snapshot::ID_TYPE seg_id);

    json
    Dump() const override;

 public:
    Status&
    GetStatus() {
        return status_;
    }

    //    Id2ToIndexMap&
    //    to_index_files() {
    //        return to_index_files_;
    //    }

    //    engine::meta::MetaPtr
    //    meta() const {
    //        return meta_ptr_;
    //    }

    const SegmentVisitorMap&
    segment_visitor_map() {
        return segment_visitor_map_;
    }

    engine::DBOptions
    options() const {
        return options_;
    }

 protected:
    void
    OnCacheInsertDataChanged(bool value) override;

 private:
    //    Id2ToIndexMap to_index_files_;
    //    engine::meta::MetaPtr meta_ptr_;
    engine::DBOptions options_;
    SegmentVisitorMap segment_visitor_map_;

    Status status_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

using SSBuildIndexJobPtr = std::shared_ptr<SSBuildIndexJob>;

}  // namespace scheduler
}  // namespace milvus
