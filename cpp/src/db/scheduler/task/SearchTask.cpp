/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "SearchTask.h"
#include "metrics/Metrics.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace zilliz {
namespace milvus {
namespace engine {

namespace {
void CollectDurationMetrics(int index_type, double total_time) {
    switch(index_type) {
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

std::string GetMetricType() {
    server::ServerConfig &config = server::ServerConfig::GetInstance();
    server::ConfigNode engine_config = config.GetConfig(server::CONFIG_ENGINE);
    return engine_config.GetValue(server::CONFIG_METRICTYPE, "L2");
}

}

SearchTask::SearchTask()
: IScheduleTask(ScheduleTaskType::kSearch) {
    std::string metric_type = GetMetricType();
    if(metric_type != "L2") {
        metric_l2 = false;
    }
}

std::shared_ptr<IScheduleTask> SearchTask::Execute() {
    if(index_engine_ == nullptr) {
        return nullptr;
    }

    SERVER_LOG_INFO << "Searching in index(" << index_id_<< ") with "
                    << search_contexts_.size() << " tasks";

    server::TimeRecorder rc("DoSearch index(" + std::to_string(index_id_) + ")");

    auto start_time = METRICS_NOW_TIME;

    std::vector<long> output_ids;
    std::vector<float> output_distence;
    for(auto& context : search_contexts_) {
        //step 1: allocate memory
        auto inner_k = context->topk();
        output_ids.resize(inner_k*context->nq());
        output_distence.resize(inner_k*context->nq());

        try {
            //step 2: search
            index_engine_->Search(context->nq(), context->vectors(), inner_k, output_distence.data(),
                                  output_ids.data());

            rc.Record("do search");

            //step 3: cluster result
            SearchContext::ResultSet result_set;
            auto spec_k = index_engine_->Count() < context->topk() ? index_engine_->Count() : context->topk();
            SearchTask::ClusterResult(output_ids, output_distence, context->nq(), spec_k, result_set);
            rc.Record("cluster result");

            //step 4: pick up topk result
            SearchTask::TopkResult(result_set, inner_k, metric_l2, context->GetResult());
            rc.Record("reduce topk");

        } catch (std::exception& ex) {
            SERVER_LOG_ERROR << "SearchTask encounter exception: " << ex.what();
            context->IndexSearchDone(index_id_);//mark as done avoid dead lock, even search failed
            continue;
        }

        //step 5: notify to send result to client
        context->IndexSearchDone(index_id_);
    }

    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    CollectDurationMetrics(index_type_, total_time);

    rc.Elapse("totally cost");

    return nullptr;
}

Status SearchTask::ClusterResult(const std::vector<long> &output_ids,
                                 const std::vector<float> &output_distence,
                                 uint64_t nq,
                                 uint64_t topk,
                                 SearchContext::ResultSet &result_set) {
    if(output_ids.size() != nq*topk || output_distence.size() != nq*topk) {
        std::string msg = "Invalid id array size: " + std::to_string(output_ids.size()) +
                " distance array size: " + std::to_string(output_distence.size());
        SERVER_LOG_ERROR << msg;
        return Status::Error(msg);
    }

    result_set.clear();
    result_set.reserve(nq);
    for (auto i = 0; i < nq; i++) {
        SearchContext::Id2DistanceMap id_distance;
        id_distance.reserve(topk);
        for (auto k = 0; k < topk; k++) {
            uint64_t index = i * topk + k;
            if(output_ids[index] < 0) {
                continue;
            }
            id_distance.push_back(std::make_pair(output_ids[index], output_distence[index]));
        }
        result_set.emplace_back(id_distance);
    }

    return Status::OK();
}

Status SearchTask::MergeResult(SearchContext::Id2DistanceMap &distance_src,
                               SearchContext::Id2DistanceMap &distance_target,
                               uint64_t topk,
                               bool ascending) {
    //Note: the score_src and score_target are already arranged by score in ascending order
    if(distance_src.empty()) {
        SERVER_LOG_WARNING << "Empty distance source array";
        return Status::OK();
    }

    if(distance_target.empty()) {
        distance_target.swap(distance_src);
        return Status::OK();
    }

    size_t src_count = distance_src.size();
    size_t target_count = distance_target.size();
    SearchContext::Id2DistanceMap distance_merged;
    distance_merged.reserve(topk);
    size_t src_index = 0, target_index = 0;
    while(true) {
        //all score_src items are merged, if score_merged.size() still less than topk
        //move items from score_target to score_merged until score_merged.size() equal topk
        if(src_index >= src_count) {
            for(size_t i = target_index; i < target_count && distance_merged.size() < topk; ++i) {
                distance_merged.push_back(distance_target[i]);
            }
            break;
        }

        //all score_target items are merged, if score_merged.size() still less than topk
        //move items from score_src to score_merged until score_merged.size() equal topk
        if(target_index >= target_count) {
            for(size_t i = src_index; i < src_count && distance_merged.size() < topk; ++i) {
                distance_merged.push_back(distance_src[i]);
            }
            break;
        }

        //compare score,
        // if ascending = true, put smallest score to score_merged one by one
        // else, put largest score to score_merged one by one
        auto& src_pair = distance_src[src_index];
        auto& target_pair = distance_target[target_index];
        if(ascending){
            if(src_pair.second > target_pair.second) {
                distance_merged.push_back(target_pair);
                target_index++;
            } else {
                distance_merged.push_back(src_pair);
                src_index++;
            }
        } else {
            if(src_pair.second < target_pair.second) {
                distance_merged.push_back(target_pair);
                target_index++;
            } else {
                distance_merged.push_back(src_pair);
                src_index++;
            }
        }

        //score_merged.size() already equal topk
        if(distance_merged.size() >= topk) {
            break;
        }
    }

    distance_target.swap(distance_merged);

    return Status::OK();
}

Status SearchTask::TopkResult(SearchContext::ResultSet &result_src,
                              uint64_t topk,
                              bool ascending,
                              SearchContext::ResultSet &result_target) {
    if (result_target.empty()) {
        result_target.swap(result_src);
        return Status::OK();
    }

    if (result_src.size() != result_target.size()) {
        std::string msg = "Invalid result set size";
        SERVER_LOG_ERROR << msg;
        return Status::Error(msg);
    }

    for (size_t i = 0; i < result_src.size(); i++) {
        SearchContext::Id2DistanceMap &score_src = result_src[i];
        SearchContext::Id2DistanceMap &score_target = result_target[i];
        SearchTask::MergeResult(score_src, score_target, topk, ascending);
    }

    return Status::OK();
}

}
}
}
