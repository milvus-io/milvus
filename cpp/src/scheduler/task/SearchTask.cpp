/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "src/metrics/Metrics.h"
#include "src/utils/TimeRecorder.h"
#include "src/db/engine/EngineFactory.h"
#include "src/db/Log.h"
#include "SearchTask.h"

#include <thread>


namespace zilliz {
namespace milvus {
namespace engine {

static constexpr size_t PARALLEL_REDUCE_THRESHOLD = 10000;
static constexpr size_t PARALLEL_REDUCE_BATCH = 1000;

bool
NeedParallelReduce(uint64_t nq, uint64_t topk) {
    server::ServerConfig &config = server::ServerConfig::GetInstance();
    server::ConfigNode &db_config = config.GetConfig(server::CONFIG_DB);
    bool need_parallel = db_config.GetBoolValue(server::CONFIG_DB_PARALLEL_REDUCE, false);
    if (!need_parallel) {
        return false;
    }

    return nq * topk >= PARALLEL_REDUCE_THRESHOLD;
}

void
ParallelReduce(std::function<void(size_t, size_t)> &reduce_function, size_t max_index) {
    size_t reduce_batch = PARALLEL_REDUCE_BATCH;

    auto thread_count = std::thread::hardware_concurrency() - 1; //not all core do this work
    if (thread_count > 0) {
        reduce_batch = max_index / thread_count + 1;
    }
    ENGINE_LOG_DEBUG << "use " << thread_count <<
                     " thread parallelly do reduce, each thread process " << reduce_batch << " vectors";

    std::vector<std::shared_ptr<std::thread> > thread_array;
    size_t from_index = 0;
    while (from_index < max_index) {
        size_t to_index = from_index + reduce_batch;
        if (to_index > max_index) {
            to_index = max_index;
        }

        auto reduce_thread = std::make_shared<std::thread>(reduce_function, from_index, to_index);
        thread_array.push_back(reduce_thread);

        from_index = to_index;
    }

    for (auto &thread_ptr : thread_array) {
        thread_ptr->join();
    }
}

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

void
CollectDurationMetrics(int index_type, double total_time) {
    switch (index_type) {
        case meta::TableFileSchema::RAW: {
            server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
            break;
        }
        case meta::TableFileSchema::TO_INDEX: {
            server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
            break;
        }
        default: {
            server::Metrics::GetInstance().SearchIndexDataDurationSecondsHistogramObserve(total_time);
            break;
        }
    }
}

XSearchTask::XSearchTask(TableFileSchemaPtr file) : file_(file) {
    index_engine_ = EngineFactory::Build(file_->dimension_,
                                         file_->location_,
                                         (EngineType) file_->engine_type_);
}

void
XSearchTask::Load(LoadType type, uint8_t device_id) {
    server::TimeRecorder rc("");

    try {
        if (type == LoadType::DISK2CPU) {
            index_engine_->Load();
        } else if (type == LoadType::CPU2GPU) {
            index_engine_->CopyToGpu(device_id);
        } else if (type == LoadType::GPU2CPU) {
            index_engine_->CopyToCpu();
        } else {
            // TODO: exception
        }
    } catch (std::exception &ex) {
        //typical error: out of disk space or permition denied
        std::string msg = "Failed to load index file: " + std::string(ex.what());
        ENGINE_LOG_ERROR << msg;

        for (auto &context : search_contexts_) {
            context->IndexSearchDone(file_->id_);//mark as done avoid dead lock, even failed
        }

        return;
    }

    size_t file_size = index_engine_->PhysicalSize();

    std::string info = "Load file id:" + std::to_string(file_->id_) + " file type:" + std::to_string(file_->file_type_)
        + " size:" + std::to_string(file_size) + " bytes from location: " + file_->location_ + " totally cost";
    double span = rc.ElapseFromBegin(info);
    for (auto &context : search_contexts_) {
        context->AccumLoadCost(span);
    }

    CollectFileMetrics(file_->file_type_, file_size);

    //step 2: return search task for later execution
    index_id_ = file_->id_;
    index_type_ = file_->file_type_;
    search_contexts_.swap(search_contexts_);
}

void
XSearchTask::Execute() {
    if (index_engine_ == nullptr) {
        return;
    }

    ENGINE_LOG_DEBUG << "Searching in file id:" << index_id_ << " with "
                     << search_contexts_.size() << " tasks";

    server::TimeRecorder rc("DoSearch file id:" + std::to_string(index_id_));

    auto start_time = METRICS_NOW_TIME;

    std::vector<long> output_ids;
    std::vector<float> output_distence;
    for (auto &context : search_contexts_) {
        //step 1: allocate memory
        auto inner_k = context->topk();
        auto nprobe = context->nprobe();
        output_ids.resize(inner_k * context->nq());
        output_distence.resize(inner_k * context->nq());

        try {
            //step 2: search
            index_engine_->Search(context->nq(), context->vectors(), inner_k, nprobe, output_distence.data(),
                                  output_ids.data());

            double span = rc.RecordSection("do search for context:" + context->Identity());
            context->AccumSearchCost(span);

            //step 3: cluster result
            SearchContext::ResultSet result_set;
            auto spec_k = index_engine_->Count() < context->topk() ? index_engine_->Count() : context->topk();
            XSearchTask::ClusterResult(output_ids, output_distence, context->nq(), spec_k, result_set);

            span = rc.RecordSection("cluster result for context:" + context->Identity());
            context->AccumReduceCost(span);

            //step 4: pick up topk result
            XSearchTask::TopkResult(result_set, inner_k, metric_l2, context->GetResult());

            span = rc.RecordSection("reduce topk for context:" + context->Identity());
            context->AccumReduceCost(span);

        } catch (std::exception &ex) {
            ENGINE_LOG_ERROR << "SearchTask encounter exception: " << ex.what();
            context->IndexSearchDone(index_id_);//mark as done avoid dead lock, even search failed
            continue;
        }

        //step 5: notify to send result to client
        context->IndexSearchDone(index_id_);
    }

    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    CollectDurationMetrics(index_type_, total_time);

    rc.ElapseFromBegin("totally cost");

    // release index in resource
    index_engine_ = nullptr;
}

TaskPtr
XSearchTask::Clone() {
    auto ret = std::make_shared<XSearchTask>(file_);
    ret->index_id_ = index_id_;
    ret->index_engine_ = index_engine_->Clone();
    ret->search_contexts_ = search_contexts_;
    ret->metric_l2 = metric_l2;
    return ret;
}

Status XSearchTask::ClusterResult(const std::vector<long> &output_ids,
                                  const std::vector<float> &output_distence,
                                  uint64_t nq,
                                  uint64_t topk,
                                  SearchContext::ResultSet &result_set) {
    if (output_ids.size() < nq * topk || output_distence.size() < nq * topk) {
        std::string msg = "Invalid id array size: " + std::to_string(output_ids.size()) +
            " distance array size: " + std::to_string(output_distence.size());
        ENGINE_LOG_ERROR << msg;
        return Status::Error(msg);
    }

    result_set.clear();
    result_set.resize(nq);

    std::function<void(size_t, size_t)> reduce_worker = [&](size_t from_index, size_t to_index) {
        for (auto i = from_index; i < to_index; i++) {
            SearchContext::Id2DistanceMap id_distance;
            id_distance.reserve(topk);
            for (auto k = 0; k < topk; k++) {
                uint64_t index = i * topk + k;
                if (output_ids[index] < 0) {
                    continue;
                }
                id_distance.push_back(std::make_pair(output_ids[index], output_distence[index]));
            }
            result_set[i] = id_distance;
        }
    };

    if (NeedParallelReduce(nq, topk)) {
        ParallelReduce(reduce_worker, nq);
    } else {
        reduce_worker(0, nq);
    }

    return Status::OK();
}

Status XSearchTask::MergeResult(SearchContext::Id2DistanceMap &distance_src,
                                SearchContext::Id2DistanceMap &distance_target,
                                uint64_t topk,
                                bool ascending) {
    //Note: the score_src and score_target are already arranged by score in ascending order
    if (distance_src.empty()) {
        ENGINE_LOG_WARNING << "Empty distance source array";
        return Status::OK();
    }

    if (distance_target.empty()) {
        distance_target.swap(distance_src);
        return Status::OK();
    }

    size_t src_count = distance_src.size();
    size_t target_count = distance_target.size();
    SearchContext::Id2DistanceMap distance_merged;
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

Status XSearchTask::TopkResult(SearchContext::ResultSet &result_src,
                               uint64_t topk,
                               bool ascending,
                               SearchContext::ResultSet &result_target) {
    if (result_target.empty()) {
        result_target.swap(result_src);
        return Status::OK();
    }

    if (result_src.size() != result_target.size()) {
        std::string msg = "Invalid result set size";
        ENGINE_LOG_ERROR << msg;
        return Status::Error(msg);
    }

    std::function<void(size_t, size_t)> ReduceWorker = [&](size_t from_index, size_t to_index) {
        for (size_t i = from_index; i < to_index; i++) {
            SearchContext::Id2DistanceMap &score_src = result_src[i];
            SearchContext::Id2DistanceMap &score_target = result_target[i];
            XSearchTask::MergeResult(score_src, score_target, topk, ascending);
        }
    };

    if (NeedParallelReduce(result_src.size(), topk)) {
        ParallelReduce(ReduceWorker, result_src.size());
    } else {
        ReduceWorker(0, result_src.size());
    }

    return Status::OK();
}


}
}
}
