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

#include "scheduler/job/SSSearchJob.h"

#include "utils/Log.h"

namespace milvus {
namespace scheduler {

SSSearchJob::SSSearchJob(const server::ContextPtr& context, int64_t topk, const milvus::json& extra_params,
                         engine::VectorsData& vectors)
    : Job(JobType::SS_SEARCH), context_(context), topk_(topk), extra_params_(extra_params), vectors_(vectors) {
}

SSSearchJob::SSSearchJob(const server::ContextPtr& context, milvus::query::GeneralQueryPtr general_query,
                         query::QueryPtr query_ptr,
                         std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type,
                         engine::VectorsData& vectors)
    : Job(JobType::SS_SEARCH),
      context_(context),
      general_query_(general_query),
      query_ptr_(query_ptr),
      attr_type_(attr_type),
      vectors_(vectors) {
}

void
SSSearchJob::AddSegmentVisitor(const engine::SegmentVisitorPtr& visitor) {
    if (visitor != nullptr) {
        segment_visitor_map_[visitor->GetSegment()->GetID()] = visitor;
    }
}

void
SSSearchJob::WaitResult() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return segment_visitor_map_.empty(); });
    //    LOG_SERVER_DEBUG_ << LogOut("[%s][%ld] SearchJob %ld: query_time %f, map_uids_time %f, reduce_time %f",
    //    "search", 0,
    //                                id(), this->time_stat().query_time, this->time_stat().map_uids_time,
    //                                this->time_stat().reduce_time);
    LOG_SERVER_DEBUG_ << LogOut("[%s][%ld] SearchJob %ld all done", "search", 0, id());
}

void
SSSearchJob::SearchDone(const engine::snapshot::ID_TYPE seg_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    segment_visitor_map_.erase(seg_id);
    if (segment_visitor_map_.empty()) {
        cv_.notify_all();
    }
    LOG_SERVER_DEBUG_ << LogOut("[%s][%ld] SearchJob %ld finish segment: %ld", "search", 0, id(), seg_id);
}

ResultIds&
SSSearchJob::GetResultIds() {
    return result_ids_;
}

ResultDistances&
SSSearchJob::GetResultDistances() {
    return result_distances_;
}

Status&
SSSearchJob::GetStatus() {
    return status_;
}

json
SSSearchJob::Dump() const {
    json ret{
        {"topk", topk_},
        {"nq", vectors_.vector_count_},
        {"extra_params", extra_params_.dump()},
    };
    auto base = Job::Dump();
    ret.insert(base.begin(), base.end());
    return ret;
}

const std::shared_ptr<server::Context>&
SSSearchJob::GetContext() const {
    return context_;
}

}  // namespace scheduler
}  // namespace milvus
