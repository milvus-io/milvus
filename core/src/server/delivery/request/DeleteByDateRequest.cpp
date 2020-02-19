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

#include "server/delivery/request/DeleteByDateRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
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
        fiu_do_on("DeleteByDateRequest.OnExecute.db_not_found", status = Status(milvus::DB_NOT_FOUND, ""));
        fiu_do_on("DeleteByDateRequest.OnExecute.describe_table_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        fiu_do_on("DeleteByDateRequest.OnExecute.throw_std_exception", throw std::exception());

        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
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

        status = DBWrapper::DB()->DropTable(table_name_, dates);
        fiu_do_on("DeleteByDateRequest.OnExecute.drop_table_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
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
