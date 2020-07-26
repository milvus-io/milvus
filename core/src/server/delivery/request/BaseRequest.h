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
#include "utils/Json.h"
#include "utils/Status.h"

#include <condition_variable>
//#include <gperftools/profiler.h>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
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

struct HybridCollectionSchema {
    std::string collection_name_;
    std::unordered_map<std::string, engine::meta::hybrid::DataType> field_types_;
    std::unordered_map<std::string, milvus::json> index_params_;
    std::unordered_map<std::string, milvus::json> field_params_;
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

class Context;

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
        kGetEntityByID,

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
        kCreateHybridIndex,

        // search operations
        kSearch,
        kSearchCombine,
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

    RequestType type_;
    std::string request_group_;
    bool async_;
    Status status_;

 private:
    mutable std::mutex finish_mtx_;
    std::condition_variable finish_cond_;
    bool done_;

 public:
    const std::shared_ptr<milvus::server::Context>&
    Context() const {
        return context_;
    }
};

using BaseRequestPtr = std::shared_ptr<BaseRequest>;

}  // namespace server
}  // namespace milvus
