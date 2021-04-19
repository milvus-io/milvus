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

#include "server/delivery/strategy/SearchReqStrategy.h"
#include "config/ServerConfig.h"
#include "utils/CommonUtil.h"
#include "utils/Error.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <queue>
#include <string>

namespace milvus {
namespace server {

SearchReqStrategy::SearchReqStrategy() {
    ConfigMgr::GetInstance().Attach("engine.search_combine_nq", this);
}

SearchReqStrategy::~SearchReqStrategy() {
    ConfigMgr::GetInstance().Detach("engine.search_combine_nq", this);
}

Status
SearchReqStrategy::ReScheduleQueue(const BaseReqPtr& req, std::queue<BaseReqPtr>& queue) {
    if (req->type() != ReqType::kSearch) {
        std::string msg = "search strategy can only handle search request";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_UNSUPPORTED_ERROR, msg);
    }

    queue.push(req);

    return Status::OK();
}

void
SearchReqStrategy::ConfigUpdate(const std::string& name) {
    search_combine_nq_ = config.engine.search_combine_nq();
}

}  // namespace server
}  // namespace milvus
