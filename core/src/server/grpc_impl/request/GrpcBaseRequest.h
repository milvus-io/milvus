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

#include "db/meta/MetaTypes.h"
#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"
#include "server/context/Context.h"
#include "utils/Status.h"
//#include <gperftools/profiler.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace milvus {
namespace server {
namespace grpc {

static const char* DQL_REQUEST_GROUP = "dql";
static const char* DDL_DML_REQUEST_GROUP = "ddl_dml";
static const char* INFO_REQUEST_GROUP = "info";

using DB_DATE = milvus::engine::meta::DateT;

Status
ConvertTimeRangeToDBDates(const std::vector<::milvus::grpc::Range>& range_array, std::vector<DB_DATE>& dates);

class GrpcBaseRequest {
 protected:
    explicit GrpcBaseRequest(const std::shared_ptr<Context>& context, const std::string& request_group,
                             bool async = false);

    virtual ~GrpcBaseRequest();

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

using BaseRequestPtr = std::shared_ptr<GrpcBaseRequest>;

}  // namespace grpc
}  // namespace server
}  // namespace milvus
