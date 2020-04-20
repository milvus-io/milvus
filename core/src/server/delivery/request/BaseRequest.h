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

#include "db/Types.h"
#include "db/meta/MetaTypes.h"
#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"
#include "query/GeneralQuery.h"
#include "server/context/Context.h"
#include "utils/Json.h"
#include "utils/Status.h"

#include <condition_variable>
//#include <gperftools/profiler.h>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace milvus {
namespace server {

struct CollectionSchema {
    std::string collection_name_;
    int64_t dimension_;
    int64_t index_file_size_;
    int64_t metric_type_;

    CollectionSchema() {
        dimension_ = 0;
        index_file_size_ = 0;
        metric_type_ = 0;
    }

    CollectionSchema(const std::string& collection_name, int64_t dimension, int64_t index_file_size,
                     int64_t metric_type) {
        collection_name_ = collection_name;
        dimension_ = dimension;
        index_file_size_ = index_file_size;
        metric_type_ = metric_type;
    }
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
    engine::Entity entities_;
};

struct IndexParam {
    std::string collection_name_;
    int64_t index_type_;
    std::string extra_params_;

    IndexParam() {
        index_type_ = 0;
    }

    IndexParam(const std::string& collection_name, int64_t index_type) {
        collection_name_ = collection_name;
        index_type_ = index_type;
    }
};

struct PartitionParam {
    std::string collection_name_;
    std::string tag_;

    PartitionParam() = default;

    PartitionParam(const std::string& collection_name, const std::string& tag) {
        collection_name_ = collection_name;
        tag_ = tag;
    }
};

struct SegmentStat {
    std::string name_;
    int64_t row_num_ = 0;
    std::string index_name_;
    int64_t data_size_ = 0;
};

struct PartitionStat {
    std::string tag_;
    int64_t total_row_num_ = 0;
    std::vector<SegmentStat> segments_stat_;
};

struct CollectionInfo {
    int64_t total_row_num_ = 0;
    std::vector<PartitionStat> partitions_stat_;
};

class BaseRequest {
 public:
    enum RequestType {
        // general operations
        kCmd = 100,

        // data operations
        kInsert = 200,
        kCompact,
        kFlush,
        kDeleteByID,
        kGetVectorByID,
        kGetVectorIDs,
        kInsertEntity,

        // collection operations
        kShowCollections = 300,
        kCreateCollection,
        kHasCollection,
        kDescribeCollection,
        kCountCollection,
        kShowCollectionInfo,
        kDropCollection,
        kPreloadCollection,
        kCreateHybridCollection,
        kHasHybridCollection,
        kDescribeHybridCollection,

        // partition operations
        kCreatePartition = 400,
        kShowPartitions,
        kDropPartition,

        // index operations
        kCreateIndex = 500,
        kDescribeIndex,
        kDropIndex,

        // search operations
        kSearchByID = 600,
        kSearch,
        kSearchCombine,
        kHybridSearch,
    };

 protected:
    BaseRequest(const std::shared_ptr<milvus::server::Context>& context, BaseRequest::RequestType type,
                bool async = false);

    virtual ~BaseRequest();

 public:
    Status
    PreExecute();

    Status
    Execute();

    Status
    PostExecute();

    void
    Done();

    Status
    WaitToFinish();

    RequestType
    GetRequestType() const {
        return type_;
    }

    std::string
    RequestGroup() const {
        return request_group_;
    }

    const Status&
    status() const {
        return status_;
    }

    void
    set_status(const Status& status);

    bool
    IsAsync() const {
        return async_;
    }

 protected:
    virtual Status
    OnPreExecute();

    virtual Status
    OnExecute() = 0;

    virtual Status
    OnPostExecute();

    std::string
    CollectionNotExistMsg(const std::string& collection_name);

 protected:
    const std::shared_ptr<milvus::server::Context> context_;

    mutable std::mutex finish_mtx_;
    std::condition_variable finish_cond_;

    RequestType type_;
    std::string request_group_;
    bool async_;
    bool done_;
    Status status_;

 public:
    const std::shared_ptr<milvus::server::Context>&
    Context() const {
        return context_;
    }
};

using BaseRequestPtr = std::shared_ptr<BaseRequest>;

}  // namespace server
}  // namespace milvus
