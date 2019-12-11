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

#include "server/delivery/request/DeleteByDateRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

DeleteByDateRequest::DeleteByDateRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                         const Range& range)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP), table_name_(table_name), range_(range) {
}

BaseRequestPtr
DeleteByDateRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name,
                            const Range& range) {
    return std::shared_ptr<BaseRequest>(new DeleteByDateRequest(context, table_name, range));
}

Status
DeleteByDateRequest::OnExecute() {
    try {
        TimeRecorderAuto rc("DeleteByDateRequest");

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code(), DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
            } else {
                return status;
            }
        }

        rc.RecordSection("check validation");

        // step 3: check date range, and convert to db dates
        std::vector<DB_DATE> dates;
        ErrorCode error_code = SERVER_SUCCESS;
        std::string error_msg;

        std::vector<Range> ranges({range_});
        status = ConvertTimeRangeToDBDates(ranges, dates);
        if (!status.ok()) {
            return status;
        }

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_nq_" + this->delete_by_range_param_->table_name() + ".profiling";
        ProfilerStart(fname.c_str());
#endif
        status = DBWrapper::DB()->DropTable(table_name_, dates);
        if (!status.ok()) {
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
