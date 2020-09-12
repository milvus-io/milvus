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

#include "scheduler/job/SearchJob.h"
#include "scheduler/task/SearchTask.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

<<<<<<< HEAD
SearchJob::SearchJob(const server::ContextPtr& context, const engine::snapshot::ScopedSnapshotT& snapshot,
                     engine::DBOptions options, const query::QueryPtr& query_ptr,
                     const engine::snapshot::IDS_TYPE& segment_ids)
    : Job(JobType::SEARCH),
      context_(context),
      snapshot_(snapshot),
      options_(options),
      query_ptr_(query_ptr),
      segment_ids_(segment_ids) {
=======
SearchJob::SearchJob(const std::shared_ptr<server::Context>& context, uint64_t topk, const milvus::json& extra_params,
                     const engine::VectorsData& vectors)
    : Job(JobType::SEARCH), context_(context), topk_(topk), extra_params_(extra_params), vectors_(vectors) {
}

SearchJob::SearchJob(const std::shared_ptr<server::Context>& context, milvus::query::GeneralQueryPtr general_query,
                     std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type,
                     const engine::VectorsData& vectors)
    : Job(JobType::SEARCH), context_(context), vectors_(vectors), general_query_(general_query), attr_type_(attr_type) {
}

bool
SearchJob::AddIndexFile(const SegmentSchemaPtr& index_file) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (index_file == nullptr || index_files_.find(index_file->id_) != index_files_.end()) {
        return false;
    }

    LOG_SERVER_DEBUG_ << LogOut("[%s][%ld] SearchJob %ld add index file: %ld", "search", 0, id(), index_file->id_);

    index_files_[index_file->id_] = index_file;
    return true;
}

void
SearchJob::WaitResult() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return index_files_.empty(); });
    LOG_SERVER_DEBUG_ << LogOut("[%s][%ld] SearchJob %ld all done", "search", 0, id());
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
}

void
SearchJob::OnCreateTasks(JobTasks& tasks) {
    for (auto& id : segment_ids_) {
        auto task = std::make_shared<SearchTask>(context_, snapshot_, options_, query_ptr_, id, nullptr);
        task->job_ = this;
        tasks.emplace_back(task);
    }
}

json
SearchJob::Dump() const {
    json ret{
        {"number_of_search_segment", segment_ids_.size()},
    };
    auto base = Job::Dump();
    ret.insert(base.begin(), base.end());
    return ret;
}

}  // namespace scheduler
}  // namespace milvus
