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

#include "server/delivery/request/Types.h"

#include "server/context/Context.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace server {

static const char* DQL_REQ_GROUP = "dql";
static const char* DDL_DML_REQ_GROUP = "ddl_dml";
static const char* INFO_REQ_GROUP = "info";

std::string
GetReqGroup(ReqType type) {
    static std::unordered_map<ReqType, std::string> s_map_type_group = {
        /* general operations */
        {ReqType::kCmd, INFO_REQ_GROUP},

        /* collection operations */
        {ReqType::kCreateCollection, DDL_DML_REQ_GROUP},
        {ReqType::kDropCollection, DDL_DML_REQ_GROUP},
        {ReqType::kHasCollection, INFO_REQ_GROUP},
        {ReqType::kListCollections, INFO_REQ_GROUP},
        {ReqType::kGetCollectionInfo, INFO_REQ_GROUP},
        {ReqType::kGetCollectionStats, INFO_REQ_GROUP},
        {ReqType::kCountEntities, INFO_REQ_GROUP},

        /* partition operations */
        {ReqType::kCreatePartition, DDL_DML_REQ_GROUP},
        {ReqType::kDropPartition, DDL_DML_REQ_GROUP},
        {ReqType::kHasPartition, INFO_REQ_GROUP},
        {ReqType::kListPartitions, INFO_REQ_GROUP},

        /* index operations */
        {ReqType::kCreateIndex, DDL_DML_REQ_GROUP},
        {ReqType::kDropIndex, DDL_DML_REQ_GROUP},

        /* data operations */
        {ReqType::kInsert, DDL_DML_REQ_GROUP},
        {ReqType::kGetEntityByID, INFO_REQ_GROUP},
        {ReqType::kDeleteEntityByID, DDL_DML_REQ_GROUP},
        {ReqType::kSearch, DQL_REQ_GROUP},
        {ReqType::kListIDInSegment, DQL_REQ_GROUP},

        /* other operations */
        {ReqType::kLoadCollection, DQL_REQ_GROUP},
        {ReqType::kFlush, DDL_DML_REQ_GROUP},
        {ReqType::kCompact, DDL_DML_REQ_GROUP},
    };

    auto iter = s_map_type_group.find(type);
    if (iter == s_map_type_group.end()) {
        LOG_SERVER_ERROR_ << "Unsupported request type: " << static_cast<int32_t>(type);
        throw Exception(SERVER_NOT_IMPLEMENT, "request group undefined");
    }
    return iter->second;
}

}  // namespace server
}  // namespace milvus
