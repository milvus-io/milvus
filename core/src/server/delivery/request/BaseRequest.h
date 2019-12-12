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

#include "db/Types.h"
#include "db/meta/MetaTypes.h"
#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"
#include "server/context/Context.h"
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

static const char* DQL_REQUEST_GROUP = "dql";
static const char* DDL_DML_REQUEST_GROUP = "ddl_dml";
static const char* INFO_REQUEST_GROUP = "info";

using DB_DATE = milvus::engine::meta::DateT;

Status
ConvertTimeRangeToDBDates(const std::vector<std::pair<std::string, std::string>>& range_array,
                          std::vector<DB_DATE>& dates);

struct TableSchema {
    std::string table_name_;
    int64_t dimension_;
    int64_t index_file_size_;
    int64_t metric_type_;

    TableSchema() {
        dimension_ = 0;
        index_file_size_ = 0;
        metric_type_ = 0;
    }

    TableSchema(const std::string& table_name, int64_t dimension, int64_t index_file_size, int64_t metric_type) {
        table_name_ = table_name;
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

struct IndexParam {
    std::string table_name_;
    int64_t index_type_;
    int64_t nlist_;

    IndexParam() {
        index_type_ = 0;
        nlist_ = 0;
    }

    IndexParam(const std::string& table_name, int64_t index_type, int64_t nlist) {
        table_name_ = table_name;
        index_type_ = index_type;
        nlist_ = nlist;
    }
};

struct PartitionParam {
    std::string table_name_;
    std::string partition_name_;
    std::string tag_;

    PartitionParam() = default;

    PartitionParam(const std::string& table_name, const std::string& partition_name, const std::string& tag) {
        table_name_ = table_name;
        partition_name_ = partition_name;
        tag_ = tag;
    }
};

class BaseRequest {
 protected:
    BaseRequest(const std::shared_ptr<Context>& context, const std::string& request_group, bool async = false);

    virtual ~BaseRequest();

 public:
    Status
    Execute();

    void
    Done();

    Status
    WaitToFinish();

    std::string
    RequestGroup() const {
        return request_group_;
    }

    const Status&
    status() const {
        return status_;
    }

    bool
    IsAsync() const {
        return async_;
    }

 protected:
    virtual Status
    OnExecute() = 0;

    Status
    SetStatus(ErrorCode error_code, const std::string& error_msg);

    std::string
    TableNotExistMsg(const std::string& table_name);

 protected:
    const std::shared_ptr<Context>& context_;

    mutable std::mutex finish_mtx_;
    std::condition_variable finish_cond_;

    std::string request_group_;
    bool async_;
    bool done_;
    Status status_;
};

using BaseRequestPtr = std::shared_ptr<BaseRequest>;
using Range = std::pair<std::string, std::string>;

}  // namespace server
}  // namespace milvus
