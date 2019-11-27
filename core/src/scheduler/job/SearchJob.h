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
#include "db/Types.h"
#include "db/meta/MetaTypes.h"

#include "server/context/Context.h"

namespace milvus {
namespace scheduler {

using engine::meta::TableFileSchemaPtr;

using Id2IndexMap = std::unordered_map<size_t, TableFileSchemaPtr>;

using ResultIds = engine::ResultIds;
using ResultDistances = engine::ResultDistances;

class SearchJob : public Job {
 public:
    SearchJob(const std::shared_ptr<server::Context>& context, uint64_t topk, uint64_t nq, uint64_t nprobe,
              const float* vectors);

 public:
    bool
    AddIndexFile(const TableFileSchemaPtr& index_file);

    void
    WaitResult();

    void
    SearchDone(size_t index_id);

    ResultIds&
    GetResultIds();

    ResultDistances&
    GetResultDistances();

    Status&
    GetStatus();

    json
    Dump() const override;

 public:
    const std::shared_ptr<server::Context>&
    GetContext() const;

    uint64_t
    topk() const {
        return topk_;
    }

    uint64_t
    nq() const {
        return nq_;
    }

    uint64_t
    nprobe() const {
        return nprobe_;
    }

    const float*
    vectors() const {
        return vectors_;
    }

    Id2IndexMap&
    index_files() {
        return index_files_;
    }

    std::mutex&
    mutex() {
        return mutex_;
    }

 private:
    const std::shared_ptr<server::Context> context_;

    uint64_t topk_ = 0;
    uint64_t nq_ = 0;
    uint64_t nprobe_ = 0;
    // TODO: smart pointer
    const float* vectors_ = nullptr;

    Id2IndexMap index_files_;
    // TODO: column-base better ?
    ResultIds result_ids_;
    ResultDistances result_distances_;
    Status status_;

    std::mutex mutex_;
    std::condition_variable cv_;
};

using SearchJobPtr = std::shared_ptr<SearchJob>;

}  // namespace scheduler
}  // namespace milvus
