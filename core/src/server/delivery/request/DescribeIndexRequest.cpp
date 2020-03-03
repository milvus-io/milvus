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

#include "server/delivery/request/DescribeIndexRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>

namespace milvus {
namespace server {

DescribeIndexRequest::DescribeIndexRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                           IndexParam& index_param)
    : BaseRequest(context, INFO_REQUEST_GROUP), table_name_(table_name), index_param_(index_param) {
}

BaseRequestPtr
DescribeIndexRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name,
                             IndexParam& index_param) {
    return std::shared_ptr<BaseRequest>(new DescribeIndexRequest(context, table_name, index_param));
}

Status
DescribeIndexRequest::OnExecute() {
    try {
        fiu_do_on("DescribeIndexRequest.OnExecute.throw_std_exception", throw std::exception());
        std::string hdr = "DescribeIndexRequest(table=" + table_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // only process root table, ignore partition table
        engine::meta::TableSchema table_schema;
        table_schema.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
            } else {
                return status;
            }
        } else {
            if (!table_schema.owner_table_.empty()) {
                return Status(SERVER_INVALID_TABLE_NAME, TableNotExistMsg(table_name_));
            }
        }

        // step 2: check table existence
        engine::TableIndex index;
        status = DBWrapper::DB()->DescribeIndex(table_name_, index);
        if (!status.ok()) {
            return status;
        }

        // for binary vector, IDMAP and IVFLAT will be treated as BIN_IDMAP and BIN_IVFLAT internally
        // return IDMAP and IVFLAT for outside caller
        if (index.engine_type_ == (int32_t)engine::EngineType::FAISS_BIN_IDMAP) {
            index.engine_type_ = (int32_t)engine::EngineType::FAISS_IDMAP;
        } else if (index.engine_type_ == (int32_t)engine::EngineType::FAISS_BIN_IVFFLAT) {
            index.engine_type_ = (int32_t)engine::EngineType::FAISS_IVFFLAT;
        }

        index_param_.table_name_ = table_name_;
        index_param_.index_type_ = index.engine_type_;
        index_param_.nlist_ = index.nlist_;
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
