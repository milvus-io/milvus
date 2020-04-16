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

class SearchJob : public Job {
 public:
    SearchJob(const std::shared_ptr<server::Context>& context, uint64_t topk, const milvus::json& extra_params,
              const engine::VectorsData& vectors);

    SearchJob(const std::shared_ptr<server::Context>& context, query::GeneralQueryPtr general_query,
              std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type,
              const engine::VectorsData& vectorsData);

 public:
    bool
    AddIndexFile(const SegmentSchemaPtr& index_file);

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
        return vectors_.vector_count_;
    }

    const milvus::json&
    extra_params() const {
        return extra_params_;
    }

    const engine::VectorsData&
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

    query::GeneralQueryPtr
    general_query() {
        return general_query_;
    }

    std::unordered_map<std::string, engine::meta::hybrid::DataType>&
    attr_type() {
        return attr_type_;
    }

    uint64_t&
    vector_count() {
        return vector_count_;
    }

 private:
    const std::shared_ptr<server::Context> context_;

    uint64_t topk_ = 0;
    milvus::json extra_params_;
    // TODO: smart pointer
    const engine::VectorsData& vectors_;

    Id2IndexMap index_files_;
    // TODO: column-base better ?
    ResultIds result_ids_;
    ResultDistances result_distances_;
    Status status_;

    query::GeneralQueryPtr general_query_;
    std::unordered_map<std::string, engine::meta::hybrid::DataType> attr_type_;
    uint64_t vector_count_;

    std::mutex mutex_;
    std::condition_variable cv_;
};

using SearchJobPtr = std::shared_ptr<SearchJob>;

}  // namespace scheduler
}  // namespace milvus
