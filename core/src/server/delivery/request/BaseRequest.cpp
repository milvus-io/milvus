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

#include "server/delivery/request/BaseRequest.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"

namespace milvus {
namespace server {

constexpr int64_t DAY_SECONDS = 24 * 60 * 60;

Status
ConvertTimeRangeToDBDates(const std::vector<std::pair<std::string, std::string>>& range_array,
                          std::vector<DB_DATE>& dates) {
    dates.clear();
    for (auto& range : range_array) {
        time_t tt_start, tt_end;
        tm tm_start, tm_end;
        if (!CommonUtil::TimeStrToTime(range.first, tt_start, tm_start)) {
            return Status(SERVER_INVALID_TIME_RANGE, "Invalid time range: " + range.first);
        }

        if (!CommonUtil::TimeStrToTime(range.second, tt_end, tm_end)) {
            return Status(SERVER_INVALID_TIME_RANGE, "Invalid time range: " + range.second);
        }

        int64_t days = (tt_end - tt_start) / DAY_SECONDS;
        if (days <= 0) {
            return Status(SERVER_INVALID_TIME_RANGE,
                          "Invalid time range: The start-date should be smaller than end-date!");
        }

        // range: [start_day, end_day)
        for (int64_t i = 0; i < days; i++) {
            time_t tt_day = tt_start + DAY_SECONDS * i;
            tm tm_day;
            CommonUtil::ConvertTime(tt_day, tm_day);

            int64_t date = tm_day.tm_year * 10000 + tm_day.tm_mon * 100 + tm_day.tm_mday;  // according to db logic
            dates.push_back(date);
        }
    }

    return Status::OK();
}

BaseRequest::BaseRequest(const std::shared_ptr<Context>& context, const std::string& request_group, bool async)
    : context_(context), request_group_(request_group), async_(async), done_(false) {
}

BaseRequest::~BaseRequest() {
    WaitToFinish();
}

Status
BaseRequest::Execute() {
    status_ = OnExecute();
    Done();
    return status_;
}

void
BaseRequest::Done() {
    done_ = true;
    finish_cond_.notify_all();
}

Status
BaseRequest::SetStatus(ErrorCode error_code, const std::string& error_msg) {
    status_ = Status(error_code, error_msg);
    SERVER_LOG_ERROR << error_msg;
    return status_;
}

std::string
BaseRequest::TableNotExistMsg(const std::string& table_name) {
    return "Table " + table_name +
           " does not exist. Use milvus.has_table to verify whether the table exists. "
           "You also can check whether the table name exists.";
}

Status
BaseRequest::WaitToFinish() {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    finish_cond_.wait(lock, [this] { return done_; });

    return status_;
}

}  // namespace server
}  // namespace milvus
