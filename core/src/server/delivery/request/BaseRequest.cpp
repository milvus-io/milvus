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
#include "utils/CommonUtil.h"
#include "utils/Log.h"

namespace milvus {
namespace server {

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
