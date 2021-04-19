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

// #include "db/Types.h"
#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"
#include "query/GeneralQuery.h"
#include "utils/Json.h"
#include "utils/Status.h"

#include <condition_variable>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace milvus {
namespace server {

struct FieldSchema {
    engine::DataType field_type_;
    milvus::json field_params_;
    milvus::json index_params_;
};

using FieldsType = std::unordered_map<std::string, FieldSchema>;

struct CollectionSchema {
    std::string collection_name_;
    FieldsType fields_;
    milvus::json extra_params_;
};

struct TopKQueryResult {
    int64_t row_num_;
    engine::ResultIds id_list_;
    engine::ResultDistances distance_list_;

    TopKQueryResult() {
        row_num_ = 0;
    }

    TopKQueryResult(int64_t row_num, const engine::ResultIds& id_list, const engine::ResultDistances& distance_list) {
        row_num_ = row_num;
        id_list_ = id_list;
        distance_list_ = distance_list;
    }
};

struct HybridQueryResult {
    int64_t row_num_;
    engine::ResultIds id_list_;
    engine::ResultDistances distance_list_;

    std::vector<engine::VectorsData> vectors_;
    std::vector<engine::AttrsData> attrs_;
};

struct IndexParam {
    std::string collection_name_;
    int64_t index_type_;
    std::string index_name_;
    std::string extra_params_;

    IndexParam() {
        index_type_ = 0;
    }

    IndexParam(const std::string& collection_name, int64_t index_type) {
        collection_name_ = collection_name;
        index_type_ = index_type;
    }
};

enum class ReqType {
    // general operations
    kCmd = 0,

    /* collection operations */
    kCreateCollection = 100,
    kDropCollection,
    kHasCollection,
    kListCollections,
    kGetCollectionInfo,
    kGetCollectionStats,
    kCountEntities,

    /* partition operations */
    kCreatePartition = 200,
    kDropPartition,
    kHasPartition,
    kListPartitions,

    /* index operations */
    kCreateIndex = 300,
    kDropIndex,
    kDescribeIndex,

    /* data operations */
    kInsert = 400,
    kGetEntityByID,
    kDeleteEntityByID,
    kSearch,
    kListIDInSegment,

    /* other operations */
    kLoadCollection = 500,
    kFlush,
    kCompact,
};

extern std::string
GetReqGroup(ReqType type);

}  // namespace server
}  // namespace milvus
