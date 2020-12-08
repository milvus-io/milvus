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
#include "config/Config.h"
#include "server/delivery/request/SearchCombineRequest.h"
#include "server/delivery/request/SearchRequest.h"
#include "utils/CommonUtil.h"
#include "utils/Error.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <queue>
#include <string>

namespace milvus {
namespace server {

SearchReqStrategy::SearchReqStrategy() {
    SetIdentity("SearchReqStrategy");
    AddSearchCombineMaxNqListener();
}

Status
SearchReqStrategy::ReScheduleQueue(const BaseRequestPtr& request, std::queue<BaseRequestPtr>& queue) {
    if (request->GetRequestType() != BaseRequest::kSearch) {
        std::string msg = "search strategy can only handle search request";
        LOG_SERVER_ERROR_ << msg;

        // directly put request to queue if the request is not a search request
        queue.push(request);
        return Status::OK();
    }

    // if config set to 0, never combine, directly put request to queue
    if (search_combine_nq_ <= 0) {
        queue.push(request);
        return Status::OK();
    }

    //    TimeRecorderAuto rc("SearchReqStrategy::ReScheduleQueue");
    SearchRequestPtr new_search_req = std::static_pointer_cast<SearchRequest>(request);

    BaseRequestPtr last_req = queue.back();
    if (last_req->GetRequestType() == BaseRequest::kSearch) {
        SearchRequestPtr last_search_req = std::static_pointer_cast<SearchRequest>(last_req);
        if (SearchCombineRequest::CanCombine(last_search_req, new_search_req, search_combine_nq_)) {
            // combine requests, create a SearchCombineRequest to hold them
            SearchCombineRequestPtr combine_request = std::make_shared<SearchCombineRequest>(search_combine_nq_);
            combine_request->Combine(last_search_req);
            combine_request->Combine(new_search_req);
            queue.back() = combine_request;  // replace the last request to combine request
            LOG_SERVER_DEBUG_ << "Combine 2 search request";
        } else {
            // directly put request to queue since the two search requests have different parameters
            queue.push(request);
        }
    } else if (last_req->GetRequestType() == BaseRequest::kSearchCombine) {
        SearchCombineRequestPtr combine_req = std::static_pointer_cast<SearchCombineRequest>(last_req);
        if (combine_req->CanCombine(new_search_req)) {
            // combine requests, the last request is a SearchCombineRequest
            combine_req->Combine(new_search_req);
            LOG_SERVER_DEBUG_ << "Combine more search request";
        } else {
            // directly put request to queue since the two search requests have different parameters
            queue.push(request);
        }
    } else {
        // the search queue could contains PreloadRequest/GetEntityByID/ReloadSegment request
        // if the last request is not SearchRequest/SearchCombineRequest, we can't do combination
        // directly put request to queue
        queue.push(request);
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
