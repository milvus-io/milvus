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

#include "config/ConfigMgr.h"
#include "db/meta/Meta.h"
#include "scheduler/Definition.h"
#include "scheduler/job/Job.h"

namespace milvus {
namespace scheduler {

using engine::meta::SegmentSchemaPtr;

using Id2ToIndexMap = std::unordered_map<size_t, SegmentSchemaPtr>;
using Id2ToTableFileMap = std::unordered_map<size_t, SegmentSchema>;

class BuildIndexJob : public Job, public ConfigObserver {
 public:
    explicit BuildIndexJob(engine::meta::MetaPtr meta_ptr, engine::DBOptions options);

    ~BuildIndexJob();

 public:
    bool
    AddToIndexFiles(const SegmentSchemaPtr& to_index_file);

    void
    WaitBuildIndexFinish();

    void
    BuildIndexDone(size_t to_index_id);

    json
    Dump() const override;

 public:
    Status&
    GetStatus() {
        return status_;
    }

    Id2ToIndexMap&
    to_index_files() {
        return to_index_files_;
    }

    engine::meta::MetaPtr
    meta() const {
        return meta_ptr_;
    }

    engine::DBOptions
    options() const {
        return options_;
    }

 public:
    void
    ConfigUpdate(const std::string& name) override;

 private:
    Id2ToIndexMap to_index_files_;
    engine::meta::MetaPtr meta_ptr_;
    engine::DBOptions options_;

    Status status_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

using BuildIndexJobPtr = std::shared_ptr<BuildIndexJob>;

}  // namespace scheduler
}  // namespace milvus
