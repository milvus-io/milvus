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

#include "SearchTask.h"
#include "metrics/Metrics.h"
#include "db/engine/EngineFactory.h"
#include "utils/TimeRecorder.h"
#include "utils/Log.h"

#include <thread>
#include "scheduler/job/SearchJob.h"


namespace zilliz {
namespace milvus {
namespace engine {

static constexpr size_t PARALLEL_REDUCE_THRESHOLD = 10000;
static constexpr size_t PARALLEL_REDUCE_BATCH = 1000;

std::mutex XSearchTask::merge_mutex_;

//bool
//NeedParallelReduce(uint64_t nq, uint64_t topk) {
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
//void
//ParallelReduce(std::function<void(size_t, size_t)> &reduce_function, size_t max_index) {
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
    switch (file_type) {
        case meta::TableFileSchema::RAW:
        case meta::TableFileSchema::TO_INDEX: {
            server::Metrics::GetInstance().RawFileSizeHistogramObserve(file_size);
            server::Metrics::GetInstance().RawFileSizeTotalIncrement(file_size);
            server::Metrics::GetInstance().RawFileSizeGaugeSet(file_size);
            break;
        }
        default: {
            server::Metrics::GetInstance().IndexFileSizeHistogramObserve(file_size);
            server::Metrics::GetInstance().IndexFileSizeTotalIncrement(file_size);
            server::Metrics::GetInstance().IndexFileSizeGaugeSet(file_size);
            break;
        }
    }
}

XSearchTask::XSearchTask(meta::TableFileSchemaPtr file)
    : Task(TaskType::SearchTask), file_(file) {
    if (file_) {
        index_engine_ = EngineFactory::Build(file_->dimension_,
                                             file_->location_,
                                             (EngineType) file_->engine_type_,
                                             (MetricType) file_->metric_type_,
                                             file_->nlist_);
    }

}

void
XSearchTask::Load(LoadType type, uint8_t device_id) {
    TimeRecorder rc("");
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        if (type == LoadType::DISK2CPU) {
            stat = index_engine_->Load();
            type_str = "DISK2CPU";
        } else if (type == LoadType::CPU2GPU) {
            stat = index_engine_->CopyToGpu(device_id);
            type_str = "CPU2GPU";
        } else if (type == LoadType::GPU2CPU) {
            stat = index_engine_->CopyToCpu();
            type_str = "GPU2CPU";
        } else {
            error_msg = "Wrong load type";
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }
    } catch (std::exception &ex) {
        //typical error: out of disk space or permition denied
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

        if (auto job = job_.lock()){
            auto search_job = std::static_pointer_cast<scheduler::SearchJob>(job);
            search_job->SearchDone(file_->id_);
            search_job->GetStatus() = s;
        }

        return;
    }

    size_t file_size = index_engine_->PhysicalSize();

    std::string info = "Load file id:" + std::to_string(file_->id_) + " file type:" + std::to_string(file_->file_type_)
        + " size:" + std::to_string(file_size) + " bytes from location: " + file_->location_ + " totally cost";
    double span = rc.ElapseFromBegin(info);
//    for (auto &context : search_contexts_) {
//        context->AccumLoadCost(span);
//    }

    CollectFileMetrics(file_->file_type_, file_size);

    //step 2: return search task for later execution
    index_id_ = file_->id_;
    index_type_ = file_->file_type_;
//    search_contexts_.swap(search_contexts_);
}

void
XSearchTask::Execute() {
    if (index_engine_ == nullptr) {
        return;
    }

//    ENGINE_LOG_DEBUG << "Searching in file id:" << index_id_ << " with "
//                     << search_contexts_.size() << " tasks";

    TimeRecorder rc("DoSearch file id:" + std::to_string(index_id_));

    server::CollectDurationMetrics metrics(index_type_);

    std::vector<long> output_ids;
    std::vector<float> output_distance;

    if (auto job = job_.lock()) {
        auto search_job = std::static_pointer_cast<scheduler::SearchJob>(job);
        //step 1: allocate memory
        uint64_t nq = search_job->nq();
        uint64_t topk = search_job->topk();
        uint64_t nprobe = search_job->nprobe();
        const float* vectors = search_job->vectors();

        output_ids.resize(topk * nq);
        output_distance.resize(topk * nq);
        std::string hdr = "job " + std::to_string(search_job->id()) +
            " nq " + std::to_string(nq) +
            " topk " + std::to_string(topk);

        try {
            //step 2: search
            index_engine_->Search(nq, vectors, topk, nprobe, output_distance.data(), output_ids.data());

            double span = rc.RecordSection(hdr + ", do search");
//            search_job->AccumSearchCost(span);


            //step 3: cluster result
            scheduler::ResultSet result_set;
            auto spec_k = index_engine_->Count() < topk ? index_engine_->Count() : topk;
            XSearchTask::ClusterResult(output_ids, output_distance, nq, spec_k, result_set);

            span = rc.RecordSection(hdr + ", cluster result");
//            search_job->AccumReduceCost(span);

            // step 4: pick up topk result
            XSearchTask::TopkResult(result_set, topk, metric_l2, search_job->GetResult());

            span = rc.RecordSection(hdr + ", reduce topk");
//            search_job->AccumReduceCost(span);
        } catch (std::exception &ex) {
            ENGINE_LOG_ERROR << "SearchTask encounter exception: " << ex.what();
//            search_job->IndexSearchDone(index_id_);//mark as done avoid dead lock, even search failed
        }

        //step 5: notify to send result to client
        search_job->SearchDone(index_id_);
    }

    rc.ElapseFromBegin("totally cost");

    // release index in resource
    index_engine_ = nullptr;
}

Status XSearchTask::ClusterResult(const std::vector<long> &output_ids,
                                  const std::vector<float> &output_distance,
                                  uint64_t nq,
                                  uint64_t topk,
                                  scheduler::ResultSet &result_set) {
    if (output_ids.size() < nq * topk || output_distance.size() < nq * topk) {
        std::string msg = "Invalid id array size: " + std::to_string(output_ids.size()) +
            " distance array size: " + std::to_string(output_distance.size());
        ENGINE_LOG_ERROR << msg;
        return Status(DB_ERROR, msg);
    }

    result_set.clear();
    result_set.resize(nq);

    std::function<void(size_t, size_t)> reduce_worker = [&](size_t from_index, size_t to_index) {
        for (auto i = from_index; i < to_index; i++) {
            scheduler::Id2DistanceMap id_distance;
            id_distance.reserve(topk);
            for (auto k = 0; k < topk; k++) {
                uint64_t index = i * topk + k;
                if (output_ids[index] < 0) {
                    continue;
                }
                id_distance.push_back(std::make_pair(output_ids[index], output_distance[index]));
            }
            result_set[i] = id_distance;
        }
    };

//    if (NeedParallelReduce(nq, topk)) {
//        ParallelReduce(reduce_worker, nq);
//    } else {
    reduce_worker(0, nq);
//    }

    return Status::OK();
}

Status XSearchTask::MergeResult(scheduler::Id2DistanceMap &distance_src,
                                scheduler::Id2DistanceMap &distance_target,
                                uint64_t topk,
                                bool ascending) {
    //Note: the score_src and score_target are already arranged by score in ascending order
    if (distance_src.empty()) {
        ENGINE_LOG_WARNING << "Empty distance source array";
        return Status::OK();
    }

    std::unique_lock<std::mutex> lock(merge_mutex_);
    if (distance_target.empty()) {
        distance_target.swap(distance_src);
        return Status::OK();
    }

    size_t src_count = distance_src.size();
    size_t target_count = distance_target.size();
    scheduler::Id2DistanceMap distance_merged;
    distance_merged.reserve(topk);
    size_t src_index = 0, target_index = 0;
    while (true) {
        //all score_src items are merged, if score_merged.size() still less than topk
        //move items from score_target to score_merged until score_merged.size() equal topk
        if (src_index >= src_count) {
            for (size_t i = target_index; i < target_count && distance_merged.size() < topk; ++i) {
                distance_merged.push_back(distance_target[i]);
            }
            break;
        }

        //all score_target items are merged, if score_merged.size() still less than topk
        //move items from score_src to score_merged until score_merged.size() equal topk
        if (target_index >= target_count) {
            for (size_t i = src_index; i < src_count && distance_merged.size() < topk; ++i) {
                distance_merged.push_back(distance_src[i]);
            }
            break;
        }

        //compare score,
        // if ascending = true, put smallest score to score_merged one by one
        // else, put largest score to score_merged one by one
        auto &src_pair = distance_src[src_index];
        auto &target_pair = distance_target[target_index];
        if (ascending) {
            if (src_pair.second > target_pair.second) {
                distance_merged.push_back(target_pair);
                target_index++;
            } else {
                distance_merged.push_back(src_pair);
                src_index++;
            }
        } else {
            if (src_pair.second < target_pair.second) {
                distance_merged.push_back(target_pair);
                target_index++;
            } else {
                distance_merged.push_back(src_pair);
                src_index++;
            }
        }

        //score_merged.size() already equal topk
        if (distance_merged.size() >= topk) {
            break;
        }
    }

    distance_target.swap(distance_merged);

    return Status::OK();
}

Status XSearchTask::TopkResult(scheduler::ResultSet &result_src,
                               uint64_t topk,
                               bool ascending,
                               scheduler::ResultSet &result_target) {
    if (result_target.empty()) {
        result_target.swap(result_src);
        return Status::OK();
    }

    if (result_src.size() != result_target.size()) {
        std::string msg = "Invalid result set size";
        ENGINE_LOG_ERROR << msg;
        return Status(DB_ERROR, msg);
    }

    std::function<void(size_t, size_t)> ReduceWorker = [&](size_t from_index, size_t to_index) {
        for (size_t i = from_index; i < to_index; i++) {
            scheduler::Id2DistanceMap &score_src = result_src[i];
            scheduler::Id2DistanceMap &score_target = result_target[i];
            XSearchTask::MergeResult(score_src, score_target, topk, ascending);
        }
    };

//    if (NeedParallelReduce(result_src.size(), topk)) {
//        ParallelReduce(ReduceWorker, result_src.size());
//    } else {
    ReduceWorker(0, result_src.size());
//    }

    return Status::OK();
}


}
}
}
