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

#include <string>
#include <vector>
#include <list>
#include <queue>
#include <deque>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>

#include "Job.h"
#include "db/meta/Meta.h"
#include "scheduler/Definition.h"


namespace milvus {
namespace scheduler {

using engine::meta::TableFileSchemaPtr;

using Id2ToIndexMap = std::unordered_map<size_t, TableFileSchemaPtr>;
using Id2ToTableFileMap = std::unordered_map<size_t, TableFileSchema>;

class BuildIndexJob : public Job {
 public:
    explicit BuildIndexJob(JobId id, engine::meta::MetaPtr meta_ptr, engine::DBOptions options);

 public:
    bool
    AddToIndexFiles(const TableFileSchemaPtr &to_index_file);

    Status &
    WaitBuildIndexFinish();

    void
    BuildIndexDone(size_t to_index_id);

 public:
    Status &
    GetStatus() {
        return status_;
    }

    Id2ToIndexMap &
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

 private:
    Id2ToIndexMap to_index_files_;
    engine::meta::MetaPtr meta_ptr_;
    engine::DBOptions options_;

    Status status_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

using BuildIndexJobPtr = std::shared_ptr<BuildIndexJob>;

}
}
