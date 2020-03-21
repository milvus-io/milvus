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

#include "server/delivery/request/SearchCombineRequest.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <set>
#include <memory>

namespace milvus {
namespace server {

namespace {

constexpr int64_t MAX_TOPK_GAP = 200;


void GetUniqueList(const std::vector<std::string>& list, std::set<std::string>& unique_list) {
    for (const std::string& item : list) {
        unique_list.insert(item);
    }
}

bool IsSameList(const std::set<std::string>& left, const std::set<std::string>& right) {
    if (left.size() != right.size()) {
        return false;
    }

    std::set<std::string>::const_iterator iter_left;
    std::set<std::string>::const_iterator iter_right;
    for (iter_left = left.begin(), iter_right = right.begin(); iter_left != left.end(); iter_left++, iter_right++) {
        if ((*iter_left) != (*iter_right)) {
            return false;
        }
    }

    return true;
}

} // namespace

SearchCombineRequest::SearchCombineRequest(const std::shared_ptr<Context>& context)
    : BaseRequest(context, BaseRequest::kSearchCombine) {
}

Status
SearchCombineRequest::Combine(const SearchRequestPtr& request) {
    if (request == nullptr) {
        return Status(SERVER_NULL_POINTER, "");
    }

    // the request must be tested by CanCombine before invoke this function
    // reset some parameters in necessary
    if (request_list_.empty()) {
        table_name_ = request->TableName();
        vectors_data_ = request->VectorsData();
        min_topk_ = request->TopK() - MAX_TOPK_GAP/2;
        search_topk_ = request->TopK();
        max_topk_ = request->TopK() + MAX_TOPK_GAP/2;
        extra_params_ = request->ExtraParams();

        GetUniqueList(request->PartitionList(), partition_list_);
        GetUniqueList(request->FileIDList(), file_id_list_);
    } else {
        search_topk_ = (search_topk_ > request->TopK()) ? search_topk_ : request->TopK();
    }

    request_list_.push_back(request);
    return Status::OK();
}

bool
SearchCombineRequest::CanCombine(const SearchRequestPtr& request) {
    if (table_name_ != request->TableName()) {
        return false;
    }

    if (extra_params_ != request->ExtraParams()) {
        return false;
    }

    // topk must within certain range
    if (request->TopK() < min_topk_ || request->TopK() > max_topk_) {
        return false;
    }

    // partition list must be equal for each one
    std::set<std::string> partition_list;
    GetUniqueList(request->PartitionList(), partition_list);
    if (!IsSameList(partition_list_, partition_list)) {
        return false;
    }

    // file id list must be equal for each one
    std::set<std::string> file_id_list;
    GetUniqueList(request->FileIDList(), file_id_list);
    if (!IsSameList(file_id_list_, file_id_list)) {
        return false;
    }

    return true;
}

bool
SearchCombineRequest::CanCombine(const SearchRequestPtr& left, const SearchRequestPtr& right) {
    if (left->TableName() != right->TableName()) {
        return false;
    }

    if (left->ExtraParams() != right->ExtraParams()) {
        return false;
    }

    // topk must within certain range
    if (abs(left->TopK() - right->TopK() > MAX_TOPK_GAP)) {
        return false;
    }

    // partition list must be equal for each one
    std::set<std::string> left_partition_list, right_partition_list;
    GetUniqueList(left->PartitionList(), left_partition_list);
    GetUniqueList(right->PartitionList(), right_partition_list);
    if (!IsSameList(left_partition_list, right_partition_list)) {
        return false;
    }

    // file id list must be equal for each one
    std::set<std::string> left_file_id_list, right_file_id_list;
    GetUniqueList(left->FileIDList(), left_file_id_list);
    GetUniqueList(right->FileIDList(), right_file_id_list);
    if (!IsSameList(left_file_id_list, right_file_id_list)) {
        return false;
    }

    return true;
}

Status
SearchCombineRequest::OnExecute() {


    return Status::OK();
}

}  // namespace server
}  // namespace milvus
