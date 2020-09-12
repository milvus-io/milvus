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

#include "scheduler/task/SearchTask.h"

#include <fiu/fiu-local.h>

#include <src/index/thirdparty/faiss/IndexFlat.h>
#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "db/Utils.h"
#include "db/engine/ExecutionEngineImpl.h"
#include "scheduler/SchedInst.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace scheduler {

SearchTask::SearchTask(const server::ContextPtr& context, engine::snapshot::ScopedSnapshotT snapshot,
                       const engine::DBOptions& options, const query::QueryPtr& query_ptr,
                       engine::snapshot::ID_TYPE segment_id, TaskLabelPtr label)
    : Task(TaskType::SearchTask, std::move(label)),
      context_(context),
      snapshot_(snapshot),
      options_(options),
      query_ptr_(query_ptr),
      segment_id_(segment_id) {
    CreateExecEngine();
}

<<<<<<< HEAD
void
SearchTask::CreateExecEngine() {
    if (execution_engine_ == nullptr && query_ptr_ != nullptr) {
        execution_engine_ = engine::EngineFactory::Build(snapshot_, options_.meta_.path_, segment_id_);
=======
XSearchTask::XSearchTask(const std::shared_ptr<server::Context>& context, SegmentSchemaPtr file, TaskLabelPtr label)
    : Task(TaskType::SearchTask, std::move(label)), context_(context), file_(file) {
    if (file_) {
        // distance -- value 0 means two vectors equal, ascending reduce, L2/HAMMING/JACCARD/TONIMOTO ...
        // similarity -- value 1 means two vectors equal, descending reduce, IP
        if (file_->metric_type_ == static_cast<int>(MetricType::IP)) {
            ascending_reduce = false;
        }

        EngineType engine_type;
        if (file->file_type_ == SegmentSchema::FILE_TYPE::RAW ||
            file->file_type_ == SegmentSchema::FILE_TYPE::TO_INDEX ||
            file->file_type_ == SegmentSchema::FILE_TYPE::BACKUP) {
            engine_type = engine::utils::IsBinaryMetricType(file->metric_type_) ? EngineType::FAISS_BIN_IDMAP
                                                                                : EngineType::FAISS_IDMAP;
        } else {
            engine_type = (EngineType)file->engine_type_;
        }

        milvus::json json_params;
        if (!file_->index_params_.empty()) {
            json_params = milvus::json::parse(file_->index_params_);
        }
        //        if (auto job = job_.lock()) {
        //            auto search_job = std::static_pointer_cast<scheduler::SearchJob>(job);
        //            query::GeneralQueryPtr general_query = search_job->general_query();
        //            if (general_query != nullptr) {
        //                std::unordered_map<std::string, engine::DataType> types;
        //                auto attr_type = search_job->attr_type();
        //                auto type_it = attr_type.begin();
        //                for (; type_it != attr_type.end(); type_it++) {
        //                    types.insert(std::make_pair(type_it->first, (engine::DataType)(type_it->second)));
        //                }
        //                index_engine_ =
        //                    EngineFactory::Build(file_->dimension_, file_->location_, engine_type,
        //                                         (MetricType)file_->metric_type_, types, json_params);
        //            }
        //        }
        index_engine_ = EngineFactory::Build(file_->dimension_, file_->location_, engine_type,
                                             (MetricType)file_->metric_type_, json_params);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    }
}

Status
SearchTask::OnLoad(LoadType type, uint8_t device_id) {
    TimeRecorder rc("SearchTask::OnLoad " + std::to_string(segment_id_));
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        if (type == LoadType::DISK2CPU) {
            engine::ExecutionEngineContext context;
            context.query_ptr_ = query_ptr_;
            stat = execution_engine_->Load(context);
            type_str = "DISK2CPU";
        } else if (type == LoadType::CPU2GPU) {
            stat = execution_engine_->CopyToGpu(device_id);
            type_str = "CPU2GPU" + std::to_string(device_id);
        } else if (type == LoadType::GPU2CPU) {
            // stat = engine_->CopyToCpu();
            type_str = "GPU2CPU";
        } else {
            error_msg = "Wrong load type";
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }
    } catch (std::exception& ex) {
        // typical error: out of disk space or permition denied
        error_msg = "Failed to load index file: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << LogOut("Search task encounter exception: %s", error_msg.c_str());
        stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
    }

    if (!stat.ok()) {
        Status s;
        if (stat.ToString().find("out of memory") != std::string::npos) {
            error_msg = "out of memory: " + type_str + " : " + stat.message();
            s = Status(SERVER_OUT_OF_MEMORY, error_msg);
        } else {
            error_msg = "Failed to load index file: " + type_str + " : " + stat.message();
            s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }

        job_->status() = s;
        return Status::OK();
    }

    std::string info = "Search task load segment id: " + std::to_string(segment_id_) + " " + type_str + " totally cost";
    rc.ElapseFromBegin(info);

    return Status::OK();
}

<<<<<<< HEAD
Status
SearchTask::OnExecute() {
    milvus::server::ContextFollower tracer(context_, "XSearchTask::Execute " + std::to_string(segment_id_));
    TimeRecorder rc(LogOut("[%s][%ld] DoSearch file id:%ld", "search", 0, segment_id_));

    if (execution_engine_ == nullptr) {
        return Status(DB_ERROR, "execution engine is null");
    }

    //    auto search_job = std::static_pointer_cast<scheduler::SearchJob>(std::shared_ptr<scheduler::Job>(job_));
    auto search_job = static_cast<scheduler::SearchJob*>(job_);
    try {
        /* step 2: search */
        engine::ExecutionEngineContext context;
        context.query_ptr_ = query_ptr_;
        context.query_result_ = std::make_shared<engine::QueryResult>();
        STATUS_CHECK(execution_engine_->Search(context));

        rc.RecordSection("search done");

        /* step 3: pick up topk result */
        // TODO(yukun): Remove hardcode here
        auto vector_param = context.query_ptr_->vectors.begin()->second;
        auto topk = vector_param->topk;
        auto segment_ptr = snapshot_->GetSegmentCommitBySegmentId(segment_id_);
        auto spec_k = segment_ptr->GetRowCount() < topk ? segment_ptr->GetRowCount() : topk;
        int64_t nq = vector_param->nq;
        if (spec_k == 0) {
            LOG_ENGINE_WARNING_ << LogOut("[%s][%ld] Searching in an empty segment. segment id = %d", "search", 0,
                                          segment_ptr->GetID());
        } else {
            //            std::unique_lock<std::mutex> lock(search_job->mutex());
            if (!search_job->query_result()) {
                search_job->query_result() = std::make_shared<engine::QueryResult>();
                search_job->query_result()->row_num_ = nq;
            }
            if (vector_param->metric_type == "IP") {
                ascending_reduce_ = false;
            }
            SearchTask::MergeTopkToResultSet(context.query_result_->result_ids_,
                                             context.query_result_->result_distances_, spec_k, nq, topk,
                                             ascending_reduce_, search_job->query_result());
=======
void
XSearchTask::Execute() {
    milvus::server::ContextFollower tracer(context_, "XSearchTask::Execute " + std::to_string(index_id_));

    //    LOG_ENGINE_DEBUG_ << "Searching in file id:" << index_id_ << " with "
    //                     << search_contexts_.size() << " tasks";

    //    TimeRecorder rc("DoSearch file id:" + std::to_string(index_id_));
    TimeRecorder rc(LogOut("[%s][%ld] DoSearch file id:%ld", "search", 0, index_id_));

    server::CollectDurationMetrics metrics(index_type_);

    std::vector<int64_t> output_ids;
    std::vector<float> output_distance;

    if (auto job = job_.lock()) {
        auto search_job = std::static_pointer_cast<scheduler::SearchJob>(job);

        if (index_engine_ == nullptr) {
            search_job->SearchDone(index_id_);
            return;
        }

        // step 1: allocate memory
        query::GeneralQueryPtr general_query = search_job->general_query();

        uint64_t nq = search_job->nq();
        uint64_t topk = search_job->topk();

        const milvus::json& extra_params = search_job->extra_params();
        const engine::VectorsData& vectors = search_job->vectors();

        output_ids.resize(topk * nq);
        output_distance.resize(topk * nq);
        std::string hdr =
            "job " + std::to_string(search_job->id()) + " nq " + std::to_string(nq) + " topk " + std::to_string(topk);

        try {
            fiu_do_on("XSearchTask.Execute.throw_std_exception", throw std::exception());
            // step 2: search
            bool hybrid = false;
            if (index_engine_->IndexEngineType() == engine::EngineType::FAISS_IVFSQ8H &&
                ResMgrInst::GetInstance()->GetResource(path().Last())->type() == ResourceType::CPU) {
                hybrid = true;
            }
            Status s;
            if (general_query != nullptr) {
                std::unordered_map<std::string, engine::DataType> types;
                auto attr_type = search_job->attr_type();
                auto type_it = attr_type.begin();
                for (; type_it != attr_type.end(); type_it++) {
                    types.insert(std::make_pair(type_it->first, (engine::DataType)(type_it->second)));
                }
                faiss::ConcurrentBitsetPtr bitset;
                s = index_engine_->ExecBinaryQuery(general_query, bitset, types, nq, topk, output_distance, output_ids);

                if (!s.ok()) {
                    search_job->GetStatus() = s;
                    search_job->SearchDone(index_id_);
                    return;
                }

                auto spec_k = file_->row_count_ < topk ? file_->row_count_ : topk;
                if (spec_k == 0) {
                    LOG_ENGINE_WARNING_ << "Searching in an empty file. file location = " << file_->location_;
                } else {
                    std::unique_lock<std::mutex> lock(search_job->mutex());
                    search_job->vector_count() = nq;
                    XSearchTask::MergeTopkToResultSet(output_ids, output_distance, spec_k, nq, topk, ascending_reduce,
                                                      search_job->GetResultIds(), search_job->GetResultDistances());

                    if (search_job->GetResultIds().empty()) {
                        LOG_ENGINE_ERROR_ << "Result reduce error: result id array is empty";
                    }
                }
                search_job->SearchDone(index_id_);
                index_engine_ = nullptr;
                return;
            }
            if (!vectors.float_data_.empty()) {
                s = index_engine_->Search(nq, vectors.float_data_.data(), topk, extra_params, output_distance.data(),
                                          output_ids.data(), hybrid);
            } else if (!vectors.binary_data_.empty()) {
                s = index_engine_->Search(nq, vectors.binary_data_.data(), topk, extra_params, output_distance.data(),
                                          output_ids.data(), hybrid);
            }

            fiu_do_on("XSearchTask.Execute.search_fail", s = Status(SERVER_UNEXPECTED_ERROR, ""));

            if (!s.ok()) {
                search_job->GetStatus() = s;
                search_job->SearchDone(index_id_);
                return;
            }

            // double span = rc.RecordSection(hdr + ", do search");
            // search_job->AccumSearchCost(span);

            // step 3: pick up topk result
            auto spec_k = file_->row_count_ < topk ? file_->row_count_ : topk;
            if (spec_k == 0) {
                LOG_ENGINE_WARNING_ << LogOut("[%s][%ld] Searching in an empty file. file location = %s", "search", 0,
                                              file_->location_.c_str());
            } else {
                std::unique_lock<std::mutex> lock(search_job->mutex());
                XSearchTask::MergeTopkToResultSet(output_ids, output_distance, spec_k, nq, topk, ascending_reduce,
                                                  search_job->GetResultIds(), search_job->GetResultDistances());
                LOG_ENGINE_DEBUG_ << "Merged result: "
                                  << "nq = " << nq << ", topk = " << topk << ", len of ids = " << output_ids.size()
                                  << ", len of distance = " << output_distance.size();

                if (search_job->GetResultIds().empty()) {
                    LOG_ENGINE_ERROR_ << "Result reduce error: result id array is empty!";
                }
            }

            // span = rc.RecordSection(hdr + ", reduce topk");
            // search_job->AccumReduceCost(span);
        } catch (std::exception& ex) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] SearchTask encounter exception: %s", "search", 0, ex.what());
            search_job->GetStatus() = Status(SERVER_UNEXPECTED_ERROR, ex.what());
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        }

        rc.RecordSection("reduce topk done");
    } catch (std::exception& ex) {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] SearchTask encounter exception: %s", "search", 0, ex.what());
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    rc.ElapseFromBegin("totally cost");
    return Status::OK();
}

void
SearchTask::MergeTopkToResultSet(const engine::ResultIds& src_ids, const engine::ResultDistances& src_distances,
                                 size_t src_k, size_t nq, size_t topk, bool ascending, engine::QueryResultPtr& result) {
    if (src_ids.empty()) {
        LOG_ENGINE_DEBUG_ << LogOut("[%s][%d] Search result is empty.", "search", 0);
        return;
    }

    size_t tar_k = result->result_ids_.size() / nq;
    size_t buf_k = std::min(topk, src_k + tar_k);

    engine::ResultIds buf_ids(nq * buf_k, -1);
    engine::ResultDistances buf_distances(nq * buf_k, 0.0);
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

            if ((result->result_ids_[tar_idx] == -1) ||  // initialized value
                (ascending && src_distances[src_idx] < result->result_distances_[tar_idx]) ||
                (!ascending && src_distances[src_idx] > result->result_distances_[tar_idx])) {
                buf_ids[buf_idx] = src_ids[src_idx];
                buf_distances[buf_idx] = src_distances[src_idx];
                src_k_j++;
            } else {
                buf_ids[buf_idx] = result->result_ids_[tar_idx];
                buf_distances[buf_idx] = result->result_distances_[tar_idx];
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
                    buf_ids[buf_idx] = result->result_ids_[tar_idx];
                    buf_distances[buf_idx] = result->result_distances_[tar_idx];
                    tar_k_j++;
                    buf_k_j++;
                }
            }
        }
    }
    result->result_ids_.swap(buf_ids);
    result->result_distances_.swap(buf_distances);
}

int64_t
SearchTask::nq() {
    if (query_ptr_) {
        auto vector_query = query_ptr_->vectors.begin();
        if (vector_query != query_ptr_->vectors.end()) {
            if (vector_query->second) {
                auto vector_param = vector_query->second;
                auto field_visitor = snapshot_->GetField(vector_query->second->field_name);
                if (field_visitor) {
                    if (field_visitor->GetParams().contains(engine::PARAM_DIMENSION)) {
                        int64_t dim = field_visitor->GetParams()[engine::PARAM_DIMENSION];
                        if (!vector_param->query_vector.float_data.empty()) {
                            return vector_param->query_vector.float_data.size() / dim;
                        } else if (!vector_param->query_vector.binary_data.empty()) {
                            return vector_param->query_vector.binary_data.size() * 8 / dim;
                        }
                    }
                }
            }
        }
    }
    return 0;
}

milvus::json
SearchTask::ExtraParam() {
    milvus::json param;
    if (query_ptr_) {
        auto vector_query = query_ptr_->vectors.begin();
        if (vector_query != query_ptr_->vectors.end()) {
            if (vector_query->second) {
                return vector_query->second->extra_params;
            }
        }
    }
    return param;
}

std::string
SearchTask::IndexType() {
    if (!index_type_.empty()) {
        return index_type_;
    }
    auto seg_visitor = engine::SegmentVisitor::Build(snapshot_, segment_id_);
    index_type_ = "FLAT";

    if (seg_visitor) {
        for (const auto& name : query_ptr_->index_fields) {
            auto field_visitor = seg_visitor->GetFieldVisitor(name);
            auto type = field_visitor->GetField()->GetFtype();
            if (!field_visitor) {
                continue;
            }
            if (type == engine::DataType::VECTOR_FLOAT || type == engine::DataType::VECTOR_BINARY) {
                auto fe_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
                if (fe_visitor) {
                    auto element = fe_visitor->GetElement();
                    index_type_ = element->GetTypeName();
                }
                return index_type_;
            }
        }
    }
    return index_type_;
}

}  // namespace scheduler
}  // namespace milvus
