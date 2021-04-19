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

namespace milvus {
namespace server {

BaseReq::BaseReq(const ContextPtr& context, ReqType type, bool async)
    : context_(context), type_(type), async_(async), done_(false) {
    req_group_ = GetReqGroup(type);
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
}

Status
BaseReq::WaitToFinish() {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    finish_cond_.wait(lock, [this] { return done_; });
    return status_;
}

}  // namespace server
}  // namespace milvus
