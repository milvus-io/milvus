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

#include <fiu-local.h>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "db/Utils.h"
#include "db/engine/EngineFactory.h"
#include "metrics/Metrics.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/SSSearchJob.h"
#include "scheduler/task/SSSearchTask.h"
#include "segment/SegmentReader.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace scheduler {

// void
// CollectFileMetrics(int file_type, size_t file_size) {
//    server::MetricsBase& inst = server::Metrics::GetInstance();
//    switch (file_type) {
//        case SegmentSchema::RAW:
//        case SegmentSchema::TO_INDEX: {
//            inst.RawFileSizeHistogramObserve(file_size);
//            inst.RawFileSizeTotalIncrement(file_size);
//            inst.RawFileSizeGaugeSet(file_size);
//            break;
//        }
//        default: {
//            inst.IndexFileSizeHistogramObserve(file_size);
//            inst.IndexFileSizeTotalIncrement(file_size);
//            inst.IndexFileSizeGaugeSet(file_size);
//            break;
//        }
//    }
//}

XSSSearchTask::XSSSearchTask(const server::ContextPtr& context, const engine::SegmentVisitorPtr& visitor,
                             TaskLabelPtr label)
    : Task(TaskType::SearchTask, std::move(label)), context_(context), visitor_(visitor) {
    //    if (file_) {
    //        // distance -- value 0 means two vectors equal, ascending reduce, L2/HAMMING/JACCARD/TONIMOTO ...
    //        // similarity -- infinity value means two vectors equal, descending reduce, IP
    //        if (file_->metric_type_ == static_cast<int>(MetricType::IP) &&
    //            file_->engine_type_ != static_cast<int>(EngineType::FAISS_PQ)) {
    //            ascending_reduce = false;
    //        }
    //
    //        EngineType engine_type;
    //        if (file->file_type_ == SegmentSchema::FILE_TYPE::RAW ||
    //            file->file_type_ == SegmentSchema::FILE_TYPE::TO_INDEX ||
    //            file->file_type_ == SegmentSchema::FILE_TYPE::BACKUP) {
    //            engine_type = engine::utils::IsBinaryMetricType(file->metric_type_) ? EngineType::FAISS_BIN_IDMAP
    //                                                                                : EngineType::FAISS_IDMAP;
    //        } else {
    //            engine_type = (EngineType)file->engine_type_;
    //        }
    //
    //        milvus::json json_params;
    //        if (!file_->index_params_.empty()) {
    //            json_params = milvus::json::parse(file_->index_params_);
    //        }
    //        index_engine_ = EngineFactory::Build(file_->dimension_, file_->location_, engine_type,
    //                                             (MetricType)file_->metric_type_, json_params);
    //    }
}

void
XSSSearchTask::Load(LoadType type, uint8_t device_id) {
    //    milvus::server::ContextFollower tracer(context_, "XSearchTask::Load " + std::to_string(file_->id_));

    TimeRecorder rc(LogOut("[%s][%ld]", "search", 0));
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        fiu_do_on("XSearchTask.Load.throw_std_exception", throw std::exception());
        if (type == LoadType::DISK2CPU) {
            //            stat = index_engine_->Load();
            //            stat = index_engine_->LoadAttr();
            type_str = "DISK2CPU";
        } else if (type == LoadType::CPU2GPU) {
            //            bool hybrid = false;
            //            if (index_engine_->IndexEngineType() == engine::EngineType::FAISS_IVFSQ8H) {
            //                hybrid = true;
            //            }
            //            stat = index_engine_->CopyToGpu(device_id, hybrid);
            type_str = "CPU2GPU" + std::to_string(device_id);
        } else if (type == LoadType::GPU2CPU) {
            //            stat = index_engine_->CopyToCpu();
            type_str = "GPU2CPU";
        } else {
            error_msg = "Wrong load type";
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }
    } catch (std::exception& ex) {
        // typical error: out of disk space or permition denied
        error_msg = "Failed to load index file: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Encounter execption: %s", "search", 0, error_msg.c_str());
        stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
    }
    fiu_do_on("XSearchTask.Load.out_of_memory", stat = Status(SERVER_UNEXPECTED_ERROR, "out of memory"));

    if (!stat.ok()) {
        Status s;
        if (stat.ToString().find("out of memory") != std::string::npos) {
            error_msg = "out of memory: " + type_str + " : " + stat.message();
            s = Status(SERVER_OUT_OF_MEMORY, error_msg);
        } else {
            error_msg = "Failed to load index file: " + type_str + " : " + stat.message();
            s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }

        if (auto job = job_.lock()) {
            auto search_job = std::static_pointer_cast<scheduler::SSSearchJob>(job);
            search_job->SearchDone(visitor_->GetSegment()->GetID());
            search_job->GetStatus() = s;
        }

        return;
    }

    //    size_t file_size = index_engine_->Size();

    //    std::string info = "Search task load file id:" + std::to_string(file_->id_) + " " + type_str +
    //                       " file type:" + std::to_string(file_->file_type_) + " size:" + std::to_string(file_size) +
    //                       " bytes from location: " + file_->location_ + " totally cost";
    //    rc.ElapseFromBegin(info);
    //
    //    CollectFileMetrics(file_->file_type_, file_size);
    //
    //    // step 2: return search task for later execution
    //    index_id_ = file_->id_;
    //    index_type_ = file_->file_type_;
    //    search_contexts_.swap(search_contexts_);
}

void
XSSSearchTask::Execute() {
    auto seg_id = visitor_->GetSegment()->GetID();
    milvus::server::ContextFollower tracer(context_, "XSearchTask::Execute " + std::to_string(seg_id));
    TimeRecorder rc(LogOut("[%s][%ld] DoSearch file id:%ld", "search", 0, seg_id));

    server::CollectDurationMetrics metrics(index_type_);

    std::vector<int64_t> output_ids;
    std::vector<float> output_distance;
    double span;

    if (auto job = job_.lock()) {
        auto search_job = std::static_pointer_cast<scheduler::SSSearchJob>(job);

        if (index_engine_ == nullptr) {
            search_job->SearchDone(seg_id);
            return;
        }

        /* step 1: allocate memory */
        query::GeneralQueryPtr general_query = search_job->general_query();

        uint64_t nq = search_job->nq();
        uint64_t topk = search_job->topk();

        fiu_do_on("XSearchTask.Execute.throw_std_exception", throw std::exception());

        //        try {
        //            /* step 2: search */
        //            bool hybrid = false;
        //            if (index_engine_->IndexEngineType() == engine::EngineType::FAISS_IVFSQ8H &&
        //                ResMgrInst::GetInstance()->GetResource(path().Last())->type() == ResourceType::CPU) {
        //                hybrid = true;
        //            }
        //            Status s;
        //            if (general_query != nullptr) {
        //                std::unordered_map<std::string, DataType> types;
        //                auto attr_type = search_job->attr_type();
        //                auto type_it = attr_type.begin();
        //                for (; type_it != attr_type.end(); type_it++) {
        //                    types.insert(std::make_pair(type_it->first, (DataType)(type_it->second)));
        //                }
        //
        //                auto query_ptr = search_job->query_ptr();
        //
        //                s = index_engine_->HybridSearch(search_job, types, output_distance, output_ids, hybrid);
        //                auto vector_query = query_ptr->vectors.begin()->second;
        //                topk = vector_query->topk;
        //                nq = vector_query->query_vector.float_data.size() / file_->dimension_;
        //                search_job->vector_count() = nq;
        //            } else {
        //                s = index_engine_->Search(output_ids, output_distance, search_job, hybrid);
        //            }
        //
        //            fiu_do_on("XSearchTask.Execute.search_fail", s = Status(SERVER_UNEXPECTED_ERROR, ""));
        //            if (!s.ok()) {
        //                search_job->GetStatus() = s;
        //                search_job->SearchDone(index_id_);
        //                return;
        //            }
        //
        //            span = rc.RecordSection("search done");
        //
        //            /* step 3: pick up topk result */
        //            auto spec_k = file_->row_count_ < topk ? file_->row_count_ : topk;
        //            if (spec_k == 0) {
        //                LOG_ENGINE_WARNING_ << LogOut("[%s][%ld] Searching in an empty file. file location = %s",
        //                "search", 0,
        //                                              file_->location_.c_str());
        //            } else {
        //                std::unique_lock<std::mutex> lock(search_job->mutex());
        //                XSearchTask::MergeTopkToResultSet(output_ids, output_distance, spec_k, nq, topk,
        //                ascending_reduce,
        //                                                  search_job->GetResultIds(),
        //                                                  search_job->GetResultDistances());
        //            }
        //
        //            span = rc.RecordSection("reduce topk done");
        //            search_job->time_stat().reduce_time += span / 1000;
        //        } catch (std::exception& ex) {
        //            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] SearchTask encounter exception: %s", "search", 0,
        //            ex.what()); search_job->GetStatus() = Status(SERVER_UNEXPECTED_ERROR, ex.what());
        //        }

        /* step 4: notify to send result to client */
        search_job->SearchDone(seg_id);
    }

    rc.ElapseFromBegin("totally cost");

    // release index in resource
    index_engine_ = nullptr;
}

void
XSSSearchTask::MergeTopkToResultSet(const scheduler::ResultIds& src_ids,
                                    const scheduler::ResultDistances& src_distances, size_t src_k, size_t nq,
                                    size_t topk, bool ascending, scheduler::ResultIds& tar_ids,
                                    scheduler::ResultDistances& tar_distances) {
    if (src_ids.empty()) {
        LOG_ENGINE_DEBUG_ << LogOut("[%s][%d] Search result is empty.", "search", 0);
        return;
    }

    size_t tar_k = tar_ids.size() / nq;
    size_t buf_k = std::min(topk, src_k + tar_k);

    scheduler::ResultIds buf_ids(nq * buf_k, -1);
    scheduler::ResultDistances buf_distances(nq * buf_k, 0.0);
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

// const std::string&
// XSSSearchTask::GetLocation() const {
//    return file_->location_;
//}

// size_t
// XSSSearchTask::GetIndexId() const {
//    return file_->id_;
//}

}  // namespace scheduler
}  // namespace milvus
