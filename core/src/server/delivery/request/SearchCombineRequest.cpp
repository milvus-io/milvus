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
        auto status = ValidationUtil::ValidateTableName(request->TableName());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateSearchTopk(request->TopK());
        if (!status.ok()) {
            return status;
        }

        // assign base parameters
        table_name_ = request->TableName();
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
    try {
        size_t combined_request = request_list_.size();
        std::string hdr = "SearchCombineRequest(table=" + table_name_ +
                          ", combined requests=" + std::to_string(combined_request) +
                          ", k=" + std::to_string(search_topk_) + ", extra_params=" + extra_params_.dump() + ")";

        TimeRecorder rc(hdr);

        // step 1: check table name
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check search topk
        if (search_topk_ > QUERY_MAX_TOPK) {
            search_topk_ = QUERY_MAX_TOPK;
        }
        status = ValidationUtil::ValidateSearchTopk(search_topk_);
        if (!status.ok()) {
            return status;
        }

        // step 3: check table existence
        // only process root table, ignore partition table
        engine::meta::TableSchema table_schema;
        table_schema.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
            } else {
                return status;
            }
        } else {
            if (!table_schema.owner_table_.empty()) {
                return Status(SERVER_INVALID_TABLE_NAME, TableNotExistMsg(table_name_));
            }
        }

        // step 4: check input
        size_t run_request = 0;
        std::vector<SearchRequestPtr>::iterator iter = request_list_.begin();
        for (; iter != request_list_.end();) {
            SearchRequestPtr& request = *iter;
            status = ValidationUtil::ValidateSearchParams(extra_params_, table_schema, request->TopK());
            if (!status.ok()) {
                // check failed, erase request and let it return error status
                request->set_status(status.code(), status.message());
                request->Done();
                iter = request_list_.erase(iter);
                continue;
            }

            status = ValidationUtil::ValidateVectorData(request->VectorsData(), table_schema);
            if (!status.ok()) {
                // check failed, erase request and let it return error status
                request->set_status(status.code(), status.message());
                request->Done();
                iter = request_list_.erase(iter);
                continue;
            }

            status = ValidationUtil::ValidatePartitionTags(request->PartitionList());
            if (!status.ok()) {
                // check failed, erase request and let it return error status
                request->set_status(status.code(), status.message());
                request->Done();
                iter = request_list_.erase(iter);
                continue;
            }

            run_request++;
            iter++;
        }

        // all requests are skipped
        if (request_list_.empty()) {
            SERVER_LOG_DEBUG << "all combined requests were skipped";
            return Status::OK();
        }

        SERVER_LOG_DEBUG << std::to_string(combined_request - run_request) << " requests were skipped";
        rc.RecordSection("check validation");

        // step 5: construct vectors_data and set search_topk
        SearchRequestPtr& first_request = *request_list_.begin();
        uint64_t total_count = 0;
        for (auto& request : request_list_) {
            total_count += request->VectorsData().vector_count_;
        }

        uint16_t dimension = table_schema.dimension_;
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
            size_t data_size = 0;
            if (is_float) {
                data_size = src.vector_count_ * dimension;
                memcpy(vectors_data_.float_data_.data() + offset, src.float_data_.data(), data_size);
            } else {
                data_size = src.vector_count_ * dimension / 8;
                memcpy(vectors_data_.binary_data_.data() + offset, src.binary_data_.data(), data_size);
            }
            offset += data_size;
        }

        // step 6: search vectors
        const std::vector<std::string>& partition_list = first_request->PartitionList();
        const std::vector<std::string>& file_id_list = first_request->FileIDList();
        auto context = first_request->Context();

        engine::ResultIds result_ids;
        engine::ResultDistances result_distances;

        if (file_id_list_.empty()) {
            status = DBWrapper::DB()->Query(context, table_name_, partition_list, (size_t)search_topk_, extra_params_,
                                            vectors_data_, result_ids, result_distances);
        } else {
            status = DBWrapper::DB()->QueryByFileID(context, file_id_list, (size_t)search_topk_, extra_params_,
                                                    vectors_data_, result_ids, result_distances);
        }

        rc.RecordSection("search combined vectors from engine");

        if (!status.ok()) {
            // let all request return
            for (auto request : request_list_) {
                request->set_status(status.code(), status.message());
                request->Done();
            }

            return status;
        }
        if (result_ids.empty()) {
            // let all request return
            for (auto request : request_list_) {
                request->set_status(status.code(), status.message());
                request->Done();
            }

            return Status::OK();  // empty table
        }

        // step 6: construct result array
        offset = 0;
        status = Status::OK();
        for (auto& request : request_list_) {
            uint64_t count = request->VectorsData().vector_count_;
            int64_t topk = request->TopK();
            TopKQueryResult& result = request->QueryResult();
            memcpy(result.id_list_.data(), result_ids.data() + offset, count * topk * sizeof(int64_t));
            memcpy(result.distance_list_.data(), result_distances.data() + offset, count * topk * sizeof(float));
            offset += count;

            // let request return
            request->set_status(status.code(), status.message());
            request->Done();
        }

        rc.RecordSection("construct result and send");
        rc.ElapseFromBegin("totally cost");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
