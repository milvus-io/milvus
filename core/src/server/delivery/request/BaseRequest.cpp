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

#include "server/delivery/request/BaseRequest.h"

#include <map>

#include "server/context/Context.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace server {

static const char* DQL_REQUEST_GROUP = "dql";
static const char* DDL_DML_REQUEST_GROUP = "ddl_dml";
static const char* INFO_REQUEST_GROUP = "info";

namespace {
std::string
RequestGroup(BaseRequest::RequestType type) {
    static std::map<BaseRequest::RequestType, std::string> s_map_type_group = {
        // general operations
        {BaseRequest::kCmd, INFO_REQUEST_GROUP},

        // data operations
        {BaseRequest::kInsert, DDL_DML_REQUEST_GROUP},
        {BaseRequest::kCompact, DDL_DML_REQUEST_GROUP},
        {BaseRequest::kFlush, DDL_DML_REQUEST_GROUP},
        {BaseRequest::kDeleteByID, DDL_DML_REQUEST_GROUP},
        {BaseRequest::kGetVectorByID, INFO_REQUEST_GROUP},
        {BaseRequest::kGetVectorIDs, INFO_REQUEST_GROUP},
        {BaseRequest::kInsertEntity, DDL_DML_REQUEST_GROUP},

        // collection operations
        {BaseRequest::kShowCollections, INFO_REQUEST_GROUP},
        {BaseRequest::kCreateCollection, DDL_DML_REQUEST_GROUP},
        {BaseRequest::kHasCollection, INFO_REQUEST_GROUP},
        {BaseRequest::kDescribeCollection, INFO_REQUEST_GROUP},
        {BaseRequest::kCountCollection, INFO_REQUEST_GROUP},
        {BaseRequest::kShowCollectionInfo, INFO_REQUEST_GROUP},
        {BaseRequest::kDropCollection, DDL_DML_REQUEST_GROUP},
        {BaseRequest::kPreloadCollection, DQL_REQUEST_GROUP},
        {BaseRequest::kCreateHybridCollection, DDL_DML_REQUEST_GROUP},
        {BaseRequest::kDescribeHybridCollection, INFO_REQUEST_GROUP},

        // partition operations
        {BaseRequest::kCreatePartition, DDL_DML_REQUEST_GROUP},
        {BaseRequest::kShowPartitions, INFO_REQUEST_GROUP},
        {BaseRequest::kDropPartition, DDL_DML_REQUEST_GROUP},

        // index operations
        {BaseRequest::kCreateIndex, DDL_DML_REQUEST_GROUP},
        {BaseRequest::kDescribeIndex, INFO_REQUEST_GROUP},
        {BaseRequest::kDropIndex, DDL_DML_REQUEST_GROUP},

        // search operations
        {BaseRequest::kSearchByID, DQL_REQUEST_GROUP},
        {BaseRequest::kSearch, DQL_REQUEST_GROUP},
        {BaseRequest::kSearchCombine, DQL_REQUEST_GROUP},
        {BaseRequest::kHybridSearch, DQL_REQUEST_GROUP},
    };

    auto iter = s_map_type_group.find(type);
    if (iter == s_map_type_group.end()) {
        LOG_SERVER_ERROR_ << "Unsupported request type: " << type;
        throw Exception(SERVER_NOT_IMPLEMENT, "request group undefined");
    }
    return iter->second;
}
}  // namespace

BaseRequest::BaseRequest(const std::shared_ptr<milvus::server::Context>& context, BaseRequest::RequestType type,
                         bool async)
    : context_(context), type_(type), async_(async), done_(false) {
    request_group_ = milvus::server::RequestGroup(type);
    if (nullptr != context_) {
        context_->SetRequestType(type_);
    }
}

BaseRequest::~BaseRequest() {
    WaitToFinish();
}

Status
BaseRequest::PreExecute() {
    status_ = OnPreExecute();
    if (!status_.ok()) {
        Done();
    }
    return status_;
}

Status
BaseRequest::Execute() {
    status_ = OnExecute();
    Done();
    return status_;
}

Status
BaseRequest::PostExecute() {
    status_ = OnPostExecute();
    return status_;
}

Status
BaseRequest::OnPreExecute() {
    return Status::OK();
}

Status
BaseRequest::OnPostExecute() {
    return Status::OK();
}

void
BaseRequest::Done() {
    done_ = true;
    finish_cond_.notify_all();
}

void
BaseRequest::set_status(const Status& status) {
    status_ = status;
    if (!status_.ok()) {
        LOG_SERVER_ERROR_ << status_.message();
    }
}

std::string
BaseRequest::CollectionNotExistMsg(const std::string& collection_name) {
    return "Collection " + collection_name +
           " does not exist. Use milvus.has_collection to verify whether the collection exists. "
           "You also can check whether the collection name exists.";
}

Status
BaseRequest::WaitToFinish() {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    finish_cond_.wait(lock, [this] { return done_; });
    return status_;
}

}  // namespace server
}  // namespace milvus
