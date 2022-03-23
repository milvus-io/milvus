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
#include "index/knowhere/knowhere/index/vector_index/helpers/IndexParameter.h"
#include "metrics/Metrics.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/SearchJob.h"
#include "scheduler/tasklabel/SpecResLabel.h"
#include "segment/SegmentReader.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

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
//    LOG_ENGINE_DEBUG_ << "use " << thread_count <<
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
        case SegmentSchema::RAW:
        case SegmentSchema::TO_INDEX: {
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

XSearchTask::XSearchTask(const std::shared_ptr<server::Context>& context, SegmentSchemaPtr file, TaskLabelPtr label)
    : Task(TaskType::SearchTask, std::move(label)), context_(context), file_(file) {
    if (file_) {
        // distance -- value 0 means two vectors equal, ascending reduce, L2/HAMMING/JACCARD/TONIMOTO ...
        // similarity -- value 1 means two vectors equal, descending reduce, IP
        if (file_->metric_type_ == static_cast<int>(MetricType::IP)) {
            ascending_reduce = false;
        }

        // The file may be a raw file, or an index file.
        // Deduce the file's engine_type by this rule:
        //     If the file is a raw file, its file_id is equal to segment id, engine_type is IDMAP
        //     else if the file is an index file, its file_id is different to segment id, engine_type is index_type
        EngineType engine_type = (EngineType)file->engine_type_;
        if (file->segment_id_ == file->file_id_) {
            engine_type = engine::utils::IsBinaryMetricType(file->metric_type_) ? EngineType::FAISS_BIN_IDMAP
                                                                                : EngineType::FAISS_IDMAP;
        }

        milvus::json json_params;
        if (!file_->index_params_.empty()) {
            json_params = milvus::json::parse(file_->index_params_);
        }

        index_engine_ = EngineFactory::Build(file_->dimension_, file_->location_, engine_type,
                                             (MetricType)file_->metric_type_, json_params, file_->updated_time_);
    }
}

void
XSearchTask::Load(LoadType type, uint8_t device_id) {
    LOG_ENGINE_DEBUG_ << "XSearchTask::Load() start";
    milvus::server::ContextFollower tracer(context_, "XSearchTask::Load " + std::to_string(file_->id_));

    TimeRecorder rc(LogOut("[%s][%ld]", "search", 0));
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        fiu_do_on("XSearchTask.Load.throw_std_exception", throw std::exception());
        if (type == LoadType::DISK2CPU) {
            stat = index_engine_->Load(true);
            type_str = "DISK2CPU";
        } else if (type == LoadType::CPU2GPU) {
            bool hybrid = false;
            if (index_engine_->IndexEngineType() == engine::EngineType::FAISS_IVFSQ8H) {
                hybrid = true;
            }
            stat = index_engine_->CopyToGpu(device_id, hybrid);
            type_str = "CPU2GPU" + std::to_string(device_id);
        } else if (type == LoadType::GPU2CPU) {
            stat = index_engine_->CopyToCpu();
            type_str = "GPU2CPU";
        } else if (type == LoadType::CPU2FPGA) {
            stat = index_engine_->CopyToFpga();
            type_str = "CPU2FPGA";
        } else if (type == LoadType::CPU2APU) {
            stat = index_engine_->CopyToApu(file_->row_count_);
            type_str = "CPU2APU";
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
        LOG_ENGINE_DEBUG_ << "XSearchTask::Load() failed status:" << stat.ToString();
        Status s;
        if (stat.ToString().find("out of memory") != std::string::npos) {
            error_msg = "out of memory: " + type_str + " : " + stat.message();
            s = Status(SERVER_OUT_OF_MEMORY, error_msg);
        } else {
            error_msg = "Failed to load index file: " + type_str + " : " + stat.message();
            s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }

        if (auto job = job_.lock()) {
            LOG_ENGINE_DEBUG_ << "XSearchTask::Load() failed status begin set";
            auto search_job = std::static_pointer_cast<scheduler::SearchJob>(job);
            search_job->SetStatus(s);
            search_job->SearchDone(file_->id_);
            LOG_ENGINE_DEBUG_ << "XSearchTask::Load() failed status finish set";
        }

        return;
    }

    size_t file_size = index_engine_->Size();

    std::string info = "Search task load file id:" + std::to_string(file_->id_) + " " + type_str +
                       " file type:" + std::to_string(file_->file_type_) + " size:" + std::to_string(file_size) +
                       " bytes from location: " + file_->location_ + " totally cost";
    rc.ElapseFromBegin(info);

    CollectFileMetrics(file_->file_type_, file_size);

    // step 2: return search task for later execution
    index_id_ = file_->id_;
    index_type_ = file_->file_type_;
    //    search_contexts_.swap(search_contexts_);
}

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

        auto engine_type = index_engine_->IndexEngineType();
        if (engine_type == EngineType::FAISS_IDMAP || engine_type == EngineType::FAISS_BIN_IDMAP) {
            // allow to assign a metric type in IDMAP and BIN_IDMAP
            if (extra_params.contains(knowhere::Metric::TYPE)) {
                auto metric_type = extra_params[knowhere::Metric::TYPE].get<int64_t>();
                LOG_ENGINE_DEBUG_ << "User's metric type " << metric_type;

                auto Illegal_Metric_Type = [&]() {
                    std::string msg = "Illegal metric type" + metric_type;
                    search_job->SetStatus(Status(SERVER_INVALID_ARGUMENT, msg));
                    search_job->SearchDone(index_id_);
                };

                if (engine_type == EngineType::FAISS_IDMAP) {
                    if (metric_type == static_cast<int64_t>(MetricType::IP)) {
                        ascending_reduce = false;
                    } else if (metric_type == static_cast<int64_t>(MetricType::L2)) {
                        // do nothing
                    } else {
                        Illegal_Metric_Type();
                        return;
                    }
                } else {
                    // FAISS_BIN_IDMAP
                    if (metric_type == static_cast<int64_t>(MetricType::HAMMING) ||
                        metric_type == static_cast<int64_t>(MetricType::JACCARD) ||
                        metric_type == static_cast<int64_t>(MetricType::TANIMOTO) ||
                        metric_type == static_cast<int64_t>(MetricType::SUBSTRUCTURE) ||
                        metric_type == static_cast<int64_t>(MetricType::SUPERSTRUCTURE)) {
                        // do nothing
                    } else {
                        Illegal_Metric_Type();
                        return;
                    }
                }
            }
        }

        output_ids.resize(topk * nq);
        output_distance.resize(topk * nq);
        std::string hdr =
            "job " + std::to_string(search_job->id()) + " nq " + std::to_string(nq) + " topk " + std::to_string(topk);

        try {
            fiu_do_on("XSearchTask.Execute.throw_std_exception", throw std::exception());
            // step 2: search
            bool hybrid = std::dynamic_pointer_cast<SpecResLabel>(label_)->IsHybrid();
            Status s;
#if 0
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
                    search_job->SetStatus(s);
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
                return;
            }
#endif
            if (!vectors.float_data_.empty()) {
                s = index_engine_->Search(nq, vectors.float_data_.data(), topk, extra_params, output_distance.data(),
                                          output_ids.data(), hybrid);
            } else if (!vectors.binary_data_.empty()) {
                s = index_engine_->Search(nq, vectors.binary_data_.data(), topk, extra_params, output_distance.data(),
                                          output_ids.data(), hybrid);
            }

            fiu_do_on("XSearchTask.Execute.search_fail", s = Status(SERVER_UNEXPECTED_ERROR, ""));

            if (!s.ok()) {
                search_job->SetStatus(s);
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
            search_job->SetStatus(Status(SERVER_UNEXPECTED_ERROR, ex.what()));
        }

        // step 4: notify to send result to client
        search_job->SearchDone(index_id_);
    }

    rc.ElapseFromBegin("totally cost");
}

void
XSearchTask::MergeTopkToResultSet(const scheduler::ResultIds& src_ids, const scheduler::ResultDistances& src_distances,
                                  size_t src_k, size_t nq, size_t topk, bool ascending, scheduler::ResultIds& tar_ids,
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

const std::string&
XSearchTask::GetLocation() const {
    return file_->location_;
}

size_t
XSearchTask::GetIndexId() const {
    return file_->id_;
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
