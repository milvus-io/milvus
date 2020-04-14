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
#if 0
#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/Utils.h"
#include "db/engine/EngineFactory.h"
#include "search/Task.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace search {

Task::Task(const std::shared_ptr<server::Context>& context, SegmentSchemaPtr& file,
           milvus::query::GeneralQueryPtr general_query, std::unordered_map<std::string, engine::DataType>& attr_type,
           context::HybridSearchContextPtr hybrid_search_context)
    : context_(context),
      file_(file),
      general_query_(general_query),
      attr_type_(attr_type),
      hybrid_search_context_(hybrid_search_context) {
    if (file_) {
        // distance -- value 0 means two vectors equal, ascending reduce, L2/HAMMING/JACCARD/TONIMOTO ...
        // similarity -- infinity value means two vectors equal, descending reduce, IP
        if (file_->metric_type_ == static_cast<int>(engine::MetricType::IP) &&
            file_->engine_type_ != static_cast<int>(engine::EngineType::FAISS_PQ)) {
            ascending_reduce = false;
        }

        engine::EngineType engine_type;
        if (file->file_type_ == engine::meta::SegmentSchema::FILE_TYPE::RAW ||
            file->file_type_ == engine::meta::SegmentSchema::FILE_TYPE::TO_INDEX ||
            file->file_type_ == engine::meta::SegmentSchema::FILE_TYPE::BACKUP) {
            engine_type = engine::utils::IsBinaryMetricType(file->metric_type_) ? engine::EngineType::FAISS_BIN_IDMAP
                                                                                : engine::EngineType::FAISS_IDMAP;
        } else {
            engine_type = (engine::EngineType)file->engine_type_;
        }

        milvus::json json_params;
        if (!file_->index_params_.empty()) {
            json_params = milvus::json::parse(file_->index_params_);
        }

        index_engine_ = engine::EngineFactory::Build(file_->dimension_, file_->location_, engine_type,
                                                     (engine::MetricType)file_->metric_type_, json_params);
    }
}

void
Task::Load() {
    auto load_ctx = context_->Follower("XSearchTask::Load " + std::to_string(file_->id_));

    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        stat = index_engine_->Load();
        type_str = "IDSK2CPU";
    } catch (std::exception& ex) {
        // typical error: out of disk space or permition denied
        error_msg = "Failed to load index file: " + std::string(ex.what());
        stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
    }

    if (!stat.ok()) {
        return;
    }
}

void
Task::Execute() {
    auto execute_ctx = context_->Follower("XSearchTask::Execute " + std::to_string(index_id_));

    if (index_engine_ == nullptr) {
        return;
    }

    TimeRecorder rc("DoSearch file id:" + std::to_string(index_id_));

    std::vector<int64_t> output_ids;
    std::vector<float> output_distance;

    // step 1: allocate memory

    try {
        // step 2: search
        Status s;
        if (general_query_ != nullptr) {
            faiss::ConcurrentBitsetPtr bitset;
            uint64_t nq, topk;
            s = index_engine_->ExecBinaryQuery(general_query_, bitset, attr_type_, nq, topk, output_distance,
                                               output_ids);

            if (!s.ok()) {
                return;
            }

            auto spec_k = file_->row_count_ < topk ? file_->row_count_ : topk;
            if (spec_k == 0) {
                ENGINE_LOG_WARNING << "Searching in an empty file. file location = " << file_->location_;
            }

            {
                if (result_ids_.size() > spec_k) {
                    if (result_ids_.front() == -1) {
                        result_ids_.resize(spec_k * nq);
                        result_distances_.resize(spec_k * nq);
                    }
                }
                Task::MergeTopkToResultSet(output_ids, output_distance, spec_k, nq, topk, ascending_reduce, result_ids_,
                                           result_distances_);
            }
            index_engine_ = nullptr;
            execute_ctx->GetTraceContext()->GetSpan()->Finish();
            return;
        }

        if (!s.ok()) {
            return;
        }
    } catch (std::exception& ex) {
        ENGINE_LOG_ERROR << "SearchTask encounter exception: " << ex.what();
        //            search_job->IndexSearchDone(index_id_);//mark as done avoid dead lock, even search failed
    }

    rc.ElapseFromBegin("totally cost");

    // release index in resource
    index_engine_ = nullptr;

    execute_ctx->GetTraceContext()->GetSpan()->Finish();
}

void
Task::MergeTopkToResultSet(const milvus::search::ResultIds& src_ids,
                           const milvus::search::ResultDistances& src_distances, size_t src_k, size_t nq, size_t topk,
                           bool ascending, milvus::search::ResultIds& tar_ids,
                           milvus::search::ResultDistances& tar_distances) {
    if (src_ids.empty()) {
        return;
    }

    size_t tar_k = tar_ids.size() / nq;
    size_t buf_k = std::min(topk, src_k + tar_k);

    ResultIds buf_ids(nq * buf_k, -1);
    ResultDistances buf_distances(nq * buf_k, 0.0);

    for (uint64_t i = 0; i < nq; i++) {
        size_t buf_k_j = 0, src_k_j = 0, tar_k_j = 0;
        size_t buf_idx, src_idx, tar_idx;

        size_t buf_k_multi_i = buf_k * i;
        size_t src_k_multi_i = topk * i;
        size_t tar_k_multi_i = tar_k * i;

        while (buf_k_j < buf_k && src_k_j < src_k && tar_k_j < tar_k) {
            src_idx = src_k_multi_i + src_k_j;
            tar_idx = tar_k_multi_i + tar_k_j;
            buf_idx = buf_k_multi_i + buf_k_j;

            if ((tar_ids[tar_idx] == -1) ||  // initialized value
                (ascending && src_distances[src_idx] < tar_distances[tar_idx]) ||
                (!ascending && src_distances[src_idx] > tar_distances[tar_idx])) {
                buf_ids[buf_idx] = src_ids[src_idx];
                buf_distances[buf_idx] = src_distances[src_idx];
                src_k_j++;
            } else {
                buf_ids[buf_idx] = tar_ids[tar_idx];
                buf_distances[buf_idx] = tar_distances[tar_idx];
                tar_k_j++;
            }
            buf_k_j++;
        }

        if (buf_k_j < buf_k) {
            if (src_k_j < src_k) {
                while (buf_k_j < buf_k && src_k_j < src_k) {
                    buf_idx = buf_k_multi_i + buf_k_j;
                    src_idx = src_k_multi_i + src_k_j;
                    buf_ids[buf_idx] = src_ids[src_idx];
                    buf_distances[buf_idx] = src_distances[src_idx];
                    src_k_j++;
                    buf_k_j++;
                }
            } else {
                while (buf_k_j < buf_k && tar_k_j < tar_k) {
                    buf_idx = buf_k_multi_i + buf_k_j;
                    tar_idx = tar_k_multi_i + tar_k_j;
                    buf_ids[buf_idx] = tar_ids[tar_idx];
                    buf_distances[buf_idx] = tar_distances[tar_idx];
                    tar_k_j++;
                    buf_k_j++;
                }
            }
        }
    }
    tar_ids.swap(buf_ids);
    tar_distances.swap(buf_distances);
}

}  // namespace search
}  // namespace milvus

#endif
