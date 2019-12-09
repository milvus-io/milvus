// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "scheduler/task/SearchTask.h"

#include <src/scheduler/SchedInst.h>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "db/engine/EngineFactory.h"
#include "metrics/Metrics.h"
#include "scheduler/job/SearchJob.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace scheduler {

static constexpr size_t PARALLEL_REDUCE_THRESHOLD = 10000;
static constexpr size_t PARALLEL_REDUCE_BATCH = 1000;

// TODO(wxyu): remove unused code
// bool
// NeedParallelReduce(uint64_t nq, uint64_t topk) {
//    server::ServerConfig &config = server::ServerConfig::GetInstance();
//    server::ConfigNode &db_config = config.GetConfig(server::CONFIG_DB);
//    bool need_parallel = db_config.GetBoolValue(server::CONFIG_DB_PARALLEL_REDUCE, false);
//    if (!need_parallel) {
//        return false;
//    }
//
//    return nq * topk >= PARALLEL_REDUCE_THRESHOLD;
//}
//
// void
// ParallelReduce(std::function<void(size_t, size_t)> &reduce_function, size_t max_index) {
//    size_t reduce_batch = PARALLEL_REDUCE_BATCH;
//
//    auto thread_count = std::thread::hardware_concurrency() - 1; //not all core do this work
//    if (thread_count > 0) {
//        reduce_batch = max_index / thread_count + 1;
//    }
//    ENGINE_LOG_DEBUG << "use " << thread_count <<
//                     " thread parallelly do reduce, each thread process " << reduce_batch << " vectors";
//
//    std::vector<std::shared_ptr<std::thread> > thread_array;
//    size_t from_index = 0;
//    while (from_index < max_index) {
//        size_t to_index = from_index + reduce_batch;
//        if (to_index > max_index) {
//            to_index = max_index;
//        }
//
//        auto reduce_thread = std::make_shared<std::thread>(reduce_function, from_index, to_index);
//        thread_array.push_back(reduce_thread);
//
//        from_index = to_index;
//    }
//
//    for (auto &thread_ptr : thread_array) {
//        thread_ptr->join();
//    }
//}

void
CollectFileMetrics(int file_type, size_t file_size) {
    server::MetricsBase& inst = server::Metrics::GetInstance();
    switch (file_type) {
        case TableFileSchema::RAW:
        case TableFileSchema::TO_INDEX: {
            inst.RawFileSizeHistogramObserve(file_size);
            inst.RawFileSizeTotalIncrement(file_size);
            inst.RawFileSizeGaugeSet(file_size);
            break;
        }
        default: {
            inst.IndexFileSizeHistogramObserve(file_size);
            inst.IndexFileSizeTotalIncrement(file_size);
            inst.IndexFileSizeGaugeSet(file_size);
            break;
        }
    }
}

XSearchTask::XSearchTask(const std::shared_ptr<server::Context>& context, TableFileSchemaPtr file, TaskLabelPtr label)
    : Task(TaskType::SearchTask, std::move(label)), context_(context), file_(file) {
    if (file_) {
        if (file_->metric_type_ != static_cast<int>(MetricType::L2)) {
            metric_l2 = false;
        }
        index_engine_ = EngineFactory::Build(file_->dimension_, file_->location_, (EngineType)file_->engine_type_,
                                             (MetricType)file_->metric_type_, file_->nlist_);
    }
}

void
XSearchTask::Load(LoadType type, uint8_t device_id) {
    auto load_ctx = context_->Follower("XSearchTask::Load " + std::to_string(file_->id_));

    TimeRecorder rc("");
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        if (type == LoadType::DISK2CPU) {
            stat = index_engine_->Load();
            type_str = "DISK2CPU";
        } else if (type == LoadType::CPU2GPU) {
            bool hybrid = false;
            if (index_engine_->IndexEngineType() == engine::EngineType::FAISS_IVFSQ8H) {
                hybrid = true;
            }
            stat = index_engine_->CopyToGpu(device_id, hybrid);
            type_str = "CPU2GPU:" + std::to_string(device_id);
        } else if (type == LoadType::GPU2CPU) {
            stat = index_engine_->CopyToCpu();
            type_str = "GPU2CPU";
        } else {
            error_msg = "Wrong load type";
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }
    } catch (std::exception& ex) {
        // typical error: out of disk space or permition denied
        error_msg = "Failed to load index file: " + std::string(ex.what());
        stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
    }

    if (!stat.ok()) {
        Status s;
        if (stat.ToString().find("out of memory") != std::string::npos) {
            error_msg = "out of memory: " + type_str;
            s = Status(SERVER_OUT_OF_MEMORY, error_msg);
        } else {
            error_msg = "Failed to load index file: " + type_str;
            s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }

        if (auto job = job_.lock()) {
            auto search_job = std::static_pointer_cast<scheduler::SearchJob>(job);
            search_job->SearchDone(file_->id_);
            search_job->GetStatus() = s;
        }

        return;
    }

    size_t file_size = index_engine_->PhysicalSize();

    std::string info = "Search task load file id:" + std::to_string(file_->id_) + " " + type_str +
                       " file type:" + std::to_string(file_->file_type_) + " size:" + std::to_string(file_size) +
                       " bytes from location: " + file_->location_ + " totally cost";
    double span = rc.ElapseFromBegin(info);
    //    for (auto &context : search_contexts_) {
    //        context->AccumLoadCost(span);
    //    }

    CollectFileMetrics(file_->file_type_, file_size);

    // step 2: return search task for later execution
    index_id_ = file_->id_;
    index_type_ = file_->file_type_;
    //    search_contexts_.swap(search_contexts_);

    load_ctx->GetTraceContext()->GetSpan()->Finish();
}

void
XSearchTask::Execute() {
    auto execute_ctx = context_->Follower("XSearchTask::Execute " + std::to_string(index_id_));

    if (index_engine_ == nullptr) {
        return;
    }

    //    ENGINE_LOG_DEBUG << "Searching in file id:" << index_id_ << " with "
    //                     << search_contexts_.size() << " tasks";

    TimeRecorder rc("DoSearch file id:" + std::to_string(index_id_));

    server::CollectDurationMetrics metrics(index_type_);

    std::vector<int64_t> output_ids;
    std::vector<float> output_distance;

    if (auto job = job_.lock()) {
        auto search_job = std::static_pointer_cast<scheduler::SearchJob>(job);
        // step 1: allocate memory
        uint64_t nq = search_job->nq();
        uint64_t topk = search_job->topk();
        uint64_t nprobe = search_job->nprobe();
        const float* vectors = search_job->vectors();

        output_ids.resize(topk * nq);
        output_distance.resize(topk * nq);
        std::string hdr =
            "job " + std::to_string(search_job->id()) + " nq " + std::to_string(nq) + " topk " + std::to_string(topk);

        try {
            // step 2: search
            bool hybrid = false;
            if (index_engine_->IndexEngineType() == engine::EngineType::FAISS_IVFSQ8H &&
                ResMgrInst::GetInstance()->GetResource(path().Last())->type() == ResourceType::CPU) {
                hybrid = true;
            }
            Status s =
                index_engine_->Search(nq, vectors, topk, nprobe, output_distance.data(), output_ids.data(), hybrid);
            if (!s.ok()) {
                search_job->GetStatus() = s;
                search_job->SearchDone(index_id_);
                return;
            }

            double span = rc.RecordSection(hdr + ", do search");
            //            search_job->AccumSearchCost(span);

            // step 3: pick up topk result
            auto spec_k = index_engine_->Count() < topk ? index_engine_->Count() : topk;
            {
                std::unique_lock<std::mutex> lock(search_job->mutex());
                XSearchTask::MergeTopkToResultSet(output_ids, output_distance, spec_k, nq, topk, metric_l2,
                                                  search_job->GetResultIds(), search_job->GetResultDistances());
            }

            span = rc.RecordSection(hdr + ", reduce topk");
            //            search_job->AccumReduceCost(span);
        } catch (std::exception& ex) {
            ENGINE_LOG_ERROR << "SearchTask encounter exception: " << ex.what();
            //            search_job->IndexSearchDone(index_id_);//mark as done avoid dead lock, even search failed
        }

        // step 4: notify to send result to client
        search_job->SearchDone(index_id_);
    }

    rc.ElapseFromBegin("totally cost");

    // release index in resource
    index_engine_ = nullptr;

    execute_ctx->GetTraceContext()->GetSpan()->Finish();
}

void
XSearchTask::MergeTopkToResultSet(const scheduler::ResultIds& src_ids, const scheduler::ResultDistances& src_distances,
                                  size_t src_k, size_t nq, size_t topk, bool ascending, scheduler::ResultIds& tar_ids,
                                  scheduler::ResultDistances& tar_distances) {
    if (src_ids.empty()) {
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

            if ((ascending && src_distances[src_idx] < tar_distances[tar_idx]) ||
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

// void
// XSearchTask::MergeTopkArray(std::vector<int64_t>& tar_ids, std::vector<float>& tar_distance, uint64_t& tar_input_k,
//                            const std::vector<int64_t>& src_ids, const std::vector<float>& src_distance,
//                            uint64_t src_input_k, uint64_t nq, uint64_t topk, bool ascending) {
//    if (src_ids.empty() || src_distance.empty()) {
//        return;
//    }
//
//    uint64_t output_k = std::min(topk, tar_input_k + src_input_k);
//    std::vector<int64_t> id_buf(nq * output_k, -1);
//    std::vector<float> dist_buf(nq * output_k, 0.0);
//
//    uint64_t buf_k, src_k, tar_k;
//    uint64_t src_idx, tar_idx, buf_idx;
//    uint64_t src_input_k_multi_i, tar_input_k_multi_i, buf_k_multi_i;
//
//    for (uint64_t i = 0; i < nq; i++) {
//        src_input_k_multi_i = src_input_k * i;
//        tar_input_k_multi_i = tar_input_k * i;
//        buf_k_multi_i = output_k * i;
//        buf_k = src_k = tar_k = 0;
//        while (buf_k < output_k && src_k < src_input_k && tar_k < tar_input_k) {
//            src_idx = src_input_k_multi_i + src_k;
//            tar_idx = tar_input_k_multi_i + tar_k;
//            buf_idx = buf_k_multi_i + buf_k;
//            if ((ascending && src_distance[src_idx] < tar_distance[tar_idx]) ||
//                (!ascending && src_distance[src_idx] > tar_distance[tar_idx])) {
//                id_buf[buf_idx] = src_ids[src_idx];
//                dist_buf[buf_idx] = src_distance[src_idx];
//                src_k++;
//            } else {
//                id_buf[buf_idx] = tar_ids[tar_idx];
//                dist_buf[buf_idx] = tar_distance[tar_idx];
//                tar_k++;
//            }
//            buf_k++;
//        }
//
//        if (buf_k < output_k) {
//            if (src_k < src_input_k) {
//                while (buf_k < output_k && src_k < src_input_k) {
//                    src_idx = src_input_k_multi_i + src_k;
//                    buf_idx = buf_k_multi_i + buf_k;
//                    id_buf[buf_idx] = src_ids[src_idx];
//                    dist_buf[buf_idx] = src_distance[src_idx];
//                    src_k++;
//                    buf_k++;
//                }
//            } else {
//                while (buf_k < output_k && tar_k < tar_input_k) {
//                    tar_idx = tar_input_k_multi_i + tar_k;
//                    buf_idx = buf_k_multi_i + buf_k;
//                    id_buf[buf_idx] = tar_ids[tar_idx];
//                    dist_buf[buf_idx] = tar_distance[tar_idx];
//                    tar_k++;
//                    buf_k++;
//                }
//            }
//        }
//    }
//
//    tar_ids.swap(id_buf);
//    tar_distance.swap(dist_buf);
//    tar_input_k = output_k;
//}

}  // namespace scheduler
}  // namespace milvus
