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

#include "server/delivery/request/BaseRequest.h"
#include "server/delivery/request/SearchRequest.h"

#include <memory>
#include <set>
#include <string>
#include <vector>

namespace milvus {
namespace server {

constexpr int64_t COMBINE_MAX_NQ = 64;

class SearchCombineRequest : public BaseRequest {
 public:
    explicit SearchCombineRequest(int64_t max_nq = COMBINE_MAX_NQ);

    Status
    Combine(const SearchRequestPtr& request);

    bool
    CanCombine(const SearchRequestPtr& request);

    static bool
    CanCombine(const SearchRequestPtr& left, const SearchRequestPtr& right, int64_t max_nq = COMBINE_MAX_NQ);

 protected:
    Status
    OnExecute() override;

 private:
    Status
    FreeRequests(const Status& status);

 private:
    std::string collection_name_;
    engine::VectorsData vectors_data_;
    int64_t min_topk_ = 0;
    int64_t search_topk_ = 0;
    int64_t max_topk_ = 0;
    milvus::json extra_params_;
    std::set<std::string> partition_list_;
    std::set<std::string> file_id_list_;

    std::vector<SearchRequestPtr> request_list_;

    int64_t combine_max_nq_ = COMBINE_MAX_NQ;
};

using SearchCombineRequestPtr = std::shared_ptr<SearchCombineRequest>;

}  // namespace server
}  // namespace milvus
