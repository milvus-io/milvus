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

#include "db/snapshot/ResourceTypes.h"
#include "scheduler/Definition.h"
#include "scheduler/job/Job.h"

namespace milvus {
namespace scheduler {

class BuildIndexJob : public Job {
 public:
    explicit BuildIndexJob(engine::DBOptions options, const std::string& collection_name,
                           const engine::snapshot::IDS_TYPE& segment_ids);

    ~BuildIndexJob() = default;

 public:
    void
    WaitFinish();

    void
    BuildIndexDone(const engine::snapshot::ID_TYPE seg_id);

    json
    Dump() const override;

 public:
    engine::DBOptions
    options() const {
        return options_;
    }

    const std::string&
    collection_name() {
        return collection_name_;
    }

    const engine::snapshot::IDS_TYPE&
    segment_ids() {
        return segment_ids_;
    }

    Status&
    status() {
        return status_;
    }

 private:
    engine::DBOptions options_;
    std::string collection_name_;
    engine::snapshot::IDS_TYPE segment_ids_;

    Status status_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

using BuildIndexJobPtr = std::shared_ptr<BuildIndexJob>;

}  // namespace scheduler
}  // namespace milvus
