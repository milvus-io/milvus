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

#include "server/delivery/request/BaseReq.h"

#include <map>

#include "server/context/Context.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace server {

static const char* DQL_REQ_GROUP = "dql";
static const char* DDL_DML_REQ_GROUP = "ddl_dml";
static const char* INFO_REQ_GROUP = "info";

namespace {
std::string
ReqGroup(BaseReq::ReqType type) {
    static std::map<BaseReq::ReqType, std::string> s_map_type_group = {
        /* general operations */
        {BaseReq::kCmd, INFO_REQ_GROUP},

        /* collection operations */
        {BaseReq::kCreateCollection, DDL_DML_REQ_GROUP},
        {BaseReq::kDropCollection, DDL_DML_REQ_GROUP},
        {BaseReq::kHasCollection, INFO_REQ_GROUP},
        {BaseReq::kListCollections, INFO_REQ_GROUP},
        {BaseReq::kGetCollectionInfo, INFO_REQ_GROUP},
        {BaseReq::kGetCollectionStats, INFO_REQ_GROUP},
        {BaseReq::kCountEntities, INFO_REQ_GROUP},

        /* partition operations */
        {BaseReq::kCreatePartition, DDL_DML_REQ_GROUP},
        {BaseReq::kDropPartition, DDL_DML_REQ_GROUP},
        {BaseReq::kHasPartition, INFO_REQ_GROUP},
        {BaseReq::kListPartitions, INFO_REQ_GROUP},

        /* index operations */
        {BaseReq::kCreateIndex, DDL_DML_REQ_GROUP},
        {BaseReq::kDropIndex, DDL_DML_REQ_GROUP},

        /* data operations */
        {BaseReq::kInsert, DDL_DML_REQ_GROUP},
        {BaseReq::kGetEntityByID, INFO_REQ_GROUP},
        {BaseReq::kDeleteEntityByID, DDL_DML_REQ_GROUP},
        {BaseReq::kSearch, DQL_REQ_GROUP},
        {BaseReq::kListIDInSegment, DQL_REQ_GROUP},

        /* other operations */
        {BaseReq::kLoadCollection, DQL_REQ_GROUP},
        {BaseReq::kFlush, DDL_DML_REQ_GROUP},
        {BaseReq::kCompact, DDL_DML_REQ_GROUP},
    };

    auto iter = s_map_type_group.find(type);
    if (iter == s_map_type_group.end()) {
        LOG_SERVER_ERROR_ << "Unsupported request type: " << type;
        throw Exception(SERVER_NOT_IMPLEMENT, "request group undefined");
    }
    return iter->second;
}
}  // namespace

BaseReq::BaseReq(const std::shared_ptr<milvus::server::Context>& context, BaseReq::ReqType type, bool async)
    : context_(context), type_(type), async_(async), done_(false) {
    req_group_ = milvus::server::ReqGroup(type);
    if (nullptr != context_) {
        context_->SetReqType(type_);
    }
}

BaseReq::~BaseReq() {
    WaitToFinish();
}

Status
BaseReq::PreExecute() {
    status_ = OnPreExecute();
    if (!status_.ok()) {
        Done();
    }
    return status_;
}

Status
BaseReq::Execute() {
    status_ = OnExecute();
    Done();
    return status_;
}

Status
BaseReq::PostExecute() {
    status_ = OnPostExecute();
    return status_;
}

Status
BaseReq::OnPreExecute() {
    return Status::OK();
}

Status
BaseReq::OnPostExecute() {
    return Status::OK();
}

void
BaseReq::Done() {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    done_ = true;
    finish_cond_.notify_all();
}

void
BaseReq::SetStatus(const Status& status) {
    status_ = status;
    if (!status_.ok()) {
        LOG_SERVER_ERROR_ << status_.message();
    }
}

std::string
BaseReq::CollectionNotExistMsg(const std::string& collection_name) {
    return "Collection " + collection_name +
           " does not exist. Use milvus.has_collection to verify whether the collection exists. "
           "You also can check whether the collection name exists.";
}

Status
BaseReq::WaitToFinish() {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    finish_cond_.wait(lock, [this] { return done_; });
    return status_;
}

}  // namespace server
}  // namespace milvus
