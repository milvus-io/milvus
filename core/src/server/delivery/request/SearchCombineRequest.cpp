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
#include "server/context/Context.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <set>

namespace milvus {
namespace server {

namespace {

constexpr int64_t MAX_TOPK_GAP = 200;
constexpr uint64_t MAX_NQ = 200;

void
GetUniqueList(const std::vector<std::string>& list, std::set<std::string>& unique_list) {
    for (const std::string& item : list) {
        unique_list.insert(item);
    }
}

bool
IsSameList(const std::set<std::string>& left, const std::set<std::string>& right) {
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

void
FreeRequest(SearchRequestPtr& request, const Status& status) {
    request->set_status(status);
    request->Done();
}

class TracingContextList {
 public:
    TracingContextList() = default;

    ~TracingContextList() {
        Finish();
    }

    void
    CreateChild(std::vector<SearchRequestPtr>& requests, const std::string& operation_name) {
        Finish();
        for (auto& request : requests) {
            auto parent_context = request->Context();
            if (parent_context) {
                auto child_context = request->Context()->Child(operation_name);
                context_list_.emplace_back(child_context);
            }
        }
    }

    void
    Finish() {
        for (auto& context : context_list_) {
            context->GetTraceContext()->GetSpan()->Finish();
        }
        context_list_.clear();
    }

 private:
    std::vector<milvus::server::ContextPtr> context_list_;
};

}  // namespace

SearchCombineRequest::SearchCombineRequest() : BaseRequest(nullptr, BaseRequest::kSearchCombine) {
}

Status
SearchCombineRequest::Combine(const SearchRequestPtr& request) {
    if (request == nullptr) {
        return Status(SERVER_NULL_POINTER, "");
    }

    // the request must be tested by CanCombine before invoke this function
    // reset some parameters in necessary
    if (request_list_.empty()) {
        // validate first request input
        auto status = ValidationUtil::ValidateCollectionName(request->CollectionName());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateSearchTopk(request->TopK());
        if (!status.ok()) {
            return status;
        }

        // assign base parameters
        collection_name_ = request->CollectionName();
        min_topk_ = request->TopK() - MAX_TOPK_GAP / 2;
        if (min_topk_ < 0) {
            min_topk_ = 0;
        }
        max_topk_ = request->TopK() + MAX_TOPK_GAP / 2;
        if (max_topk_ > QUERY_MAX_TOPK) {
            max_topk_ = QUERY_MAX_TOPK;
        }
        extra_params_ = request->ExtraParams();

        GetUniqueList(request->PartitionList(), partition_list_);
        GetUniqueList(request->FileIDList(), file_id_list_);
    }

    request_list_.push_back(request);
    return Status::OK();
}

bool
SearchCombineRequest::CanCombine(const SearchRequestPtr& request) {
    if (collection_name_ != request->CollectionName()) {
        return false;
    }

    if (extra_params_ != request->ExtraParams()) {
        return false;
    }

    // topk must within certain range
    if (request->TopK() < min_topk_ || request->TopK() > max_topk_) {
        return false;
    }

    // sum of nq must less-equal than MAX_NQ
    if (vectors_data_.vector_count_ > MAX_NQ || request->VectorsData().vector_count_ > MAX_NQ) {
        return false;
    }
    uint64_t total_nq = vectors_data_.vector_count_ + request->VectorsData().vector_count_;
    if (total_nq > MAX_NQ) {
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
    if (left->CollectionName() != right->CollectionName()) {
        return false;
    }

    if (left->ExtraParams() != right->ExtraParams()) {
        return false;
    }

    // topk must within certain range
    if (abs(left->TopK() - right->TopK() > MAX_TOPK_GAP)) {
        return false;
    }

    // sum of nq must less-equal than MAX_NQ
    if (left->VectorsData().vector_count_ > MAX_NQ || right->VectorsData().vector_count_ > MAX_NQ) {
        return false;
    }
    uint64_t total_nq = left->VectorsData().vector_count_ + right->VectorsData().vector_count_;
    if (total_nq > MAX_NQ) {
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
SearchCombineRequest::FreeRequests(const Status& status) {
    for (auto request : request_list_) {
        FreeRequest(request, status);
    }

    return Status::OK();
}

Status
SearchCombineRequest::OnExecute() {
    try {
        size_t combined_request = request_list_.size();
        LOG_SERVER_DEBUG_ << "SearchCombineRequest execute, request count=" << combined_request
                          << ", extra_params=" << extra_params_.dump();
        std::string hdr = "SearchCombineRequest(collection=" + collection_name_ + ")";

        TimeRecorderAuto rc(hdr);

        // step 1: check collection existence
        // only process root collection, ignore partition collection
        engine::meta::CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_name_;
        auto status = DBWrapper::DB()->DescribeCollection(collection_schema);

        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                status = Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
                FreeRequests(status);
                return status;
            } else {
                FreeRequests(status);
                return status;
            }
        } else {
            if (!collection_schema.owner_collection_.empty()) {
                status = Status(SERVER_INVALID_COLLECTION_NAME, CollectionNotExistMsg(collection_name_));
                FreeRequests(status);
                return status;
            }
        }

        // step 2: check input
        size_t run_request = 0;
        std::vector<SearchRequestPtr>::iterator iter = request_list_.begin();
        for (; iter != request_list_.end();) {
            SearchRequestPtr& request = *iter;
            status = ValidationUtil::ValidateSearchTopk(request->TopK());
            if (!status.ok()) {
                // check failed, erase request and let it return error status
                FreeRequest(request, status);
                iter = request_list_.erase(iter);
                continue;
            }

            status = ValidationUtil::ValidateSearchParams(extra_params_, collection_schema, request->TopK());
            if (!status.ok()) {
                // check failed, erase request and let it return error status
                FreeRequest(request, status);
                iter = request_list_.erase(iter);
                continue;
            }

            status = ValidationUtil::ValidateVectorData(request->VectorsData(), collection_schema);
            if (!status.ok()) {
                // check failed, erase request and let it return error status
                FreeRequest(request, status);
                iter = request_list_.erase(iter);
                continue;
            }

            status = ValidationUtil::ValidatePartitionTags(request->PartitionList());
            if (!status.ok()) {
                // check failed, erase request and let it return error status
                FreeRequest(request, status);
                iter = request_list_.erase(iter);
                continue;
            }

            // reset topk
            search_topk_ = request->TopK() > search_topk_ ? request->TopK() : search_topk_;

            // next one
            run_request++;
            iter++;
        }

        // all requests are skipped
        if (request_list_.empty()) {
            LOG_SERVER_DEBUG_ << "all combined requests were skipped";
            return Status::OK();
        }

        LOG_SERVER_DEBUG_ << (combined_request - run_request) << " requests were skipped";
        LOG_SERVER_DEBUG_ << "reset topk to " << search_topk_;
        rc.RecordSection("check validation");

        // step 3: construct vectors_data
        SearchRequestPtr& first_request = *request_list_.begin();
        uint64_t total_count = 0;
        for (auto& request : request_list_) {
            total_count += request->VectorsData().vector_count_;
        }
        vectors_data_.vector_count_ = total_count;

        uint16_t dimension = collection_schema.dimension_;
        bool is_float = true;
        if (!first_request->VectorsData().float_data_.empty()) {
            vectors_data_.float_data_.resize(total_count * dimension);
        } else {
            vectors_data_.binary_data_.resize(total_count * dimension / 8);
            is_float = false;
        }

        int64_t offset = 0;
        for (auto& request : request_list_) {
            const engine::VectorsData& src = request->VectorsData();
            if (is_float) {
                size_t element_cnt = src.vector_count_ * dimension;
                memcpy(vectors_data_.float_data_.data() + offset, src.float_data_.data(), element_cnt * sizeof(float));
                offset += element_cnt;
            } else {
                size_t element_cnt = src.vector_count_ * dimension / 8;
                memcpy(vectors_data_.binary_data_.data() + offset, src.binary_data_.data(), element_cnt);
                offset += element_cnt;
            }
        }

        LOG_SERVER_DEBUG_ << total_count << " query vectors combined";
        rc.RecordSection("combined query vectors");

        // step 4: search vectors
        const std::vector<std::string>& partition_list = first_request->PartitionList();
        const std::vector<std::string>& file_id_list = first_request->FileIDList();

        engine::ResultIds result_ids;
        engine::ResultDistances result_distances;
        {
            TracingContextList context_list;
            context_list.CreateChild(request_list_, "Combine Query");

            if (file_id_list_.empty()) {
                status = DBWrapper::DB()->Query(nullptr, collection_name_, partition_list, (size_t)search_topk_,
                                                extra_params_, vectors_data_, result_ids, result_distances);
            } else {
                status = DBWrapper::DB()->QueryByFileID(nullptr, file_id_list, (size_t)search_topk_, extra_params_,
                                                        vectors_data_, result_ids, result_distances);
            }
        }

        rc.RecordSection("search vectors from engine");

        if (!status.ok()) {
            // let all request return
            FreeRequests(status);
            return status;
        }
        if (result_ids.empty()) {
            status = Status(DB_ERROR, "no result returned for combined request");
            // let all request return
            FreeRequests(status);
            return status;
        }

        // avoid memcpy crash, check id count = target vector count * topk
        if (result_ids.size() != total_count * search_topk_) {
            status = Status(DB_ERROR, "Result count doesn't match target vectors count");
            // let all request return
            FreeRequests(status);
            return status;
        }

        // avoid memcpy crash, check distance count = id count
        if (result_distances.size() != result_ids.size()) {
            status = Status(DB_ERROR, "Result distance and id count doesn't match");
            // let all request return
            FreeRequests(status);
            return status;
        }

        // step 5: construct result array
        offset = 0;
        for (auto& request : request_list_) {
            uint64_t count = request->VectorsData().vector_count_;
            int64_t topk = request->TopK();
            uint64_t element_cnt = count * topk;
            TopKQueryResult& result = request->QueryResult();
            result.row_num_ = count;
            result.id_list_.resize(element_cnt);
            result.distance_list_.resize(element_cnt);
            memcpy(result.id_list_.data(), result_ids.data() + offset, element_cnt * sizeof(int64_t));
            memcpy(result.distance_list_.data(), result_distances.data() + offset, element_cnt * sizeof(float));
            offset += (count * search_topk_);

            // let request return
            FreeRequest(request, Status::OK());
        }

        rc.RecordSection("construct result and send");
    } catch (std::exception& ex) {
        Status status = Status(SERVER_UNEXPECTED_ERROR, ex.what());
        FreeRequests(status);
        return status;
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
