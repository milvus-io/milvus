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

#pragma once

#include "server/context/Context.h"
#include "server/delivery/request/Types.h"
#include "utils/Status.h"
#include "pulsar/message_client/ClientV2.h"

#include <condition_variable>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace server {

class BaseReq {
 protected:
    BaseReq(const ContextPtr& context, ReqType type, bool async = false);

    virtual ~BaseReq();

 public:
    const ContextPtr&
    context() const {
        return context_;
    }

    ReqType
    type() const {
        return type_;
    }

    std::string
    req_group() const {
        return req_group_;
    }

    const Status&
    status() const {
        return status_;
    }

    bool
    async() const {
        return async_;
    }

    Status
    PreExecute();

    Status
    Execute();

    Status
    PostExecute();

    void
    Done();

    Status
    WaitToFinish();

    void
    SetStatus(const Status& status);

    void
    SetTimestamp(uint64_t ts);

 protected:
    virtual Status
    OnPreExecute();

    virtual Status
    OnExecute() = 0;

    virtual Status
    OnPostExecute();

 protected:
    const ContextPtr context_;
    ReqType type_;
    std::string req_group_;
    bool async_;
    Status status_;
    uint64_t timestamp_;


 private:
    mutable std::mutex finish_mtx_;
    std::condition_variable finish_cond_;
    bool done_;
};

using BaseReqPtr = std::shared_ptr<BaseReq>;

}  // namespace server
}  // namespace milvus
