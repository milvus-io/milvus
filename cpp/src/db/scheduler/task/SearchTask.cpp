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
void ClusterResult(const std::vector<long> &output_ids,
                   const std::vector<float> &output_distence,
                   uint64_t nq,
                   uint64_t topk,
                   SearchContext::ResultSet &result_set) {
    result_set.clear();
    result_set.reserve(nq);
    for (auto i = 0; i < nq; i++) {
        SearchContext::Id2ScoreMap id_score;
        id_score.reserve(topk);
        for (auto k = 0; k < topk; k++) {
            uint64_t index = i * topk + k;
            if(output_ids[index] < 0) {
                continue;
            }
            id_score.push_back(std::make_pair(output_ids[index], output_distence[index]));
        }
        result_set.emplace_back(id_score);
    }
}

void MergeResult(SearchContext::Id2ScoreMap &score_src,
                 SearchContext::Id2ScoreMap &score_target,
                 uint64_t topk) {
    //Note: the score_src and score_target are already arranged by score in ascending order
    if(score_src.empty()) {
        return;
    }

    if(score_target.empty()) {
        score_target.swap(score_src);
        return;
    }

    size_t src_count = score_src.size();
    size_t target_count = score_target.size();
    SearchContext::Id2ScoreMap score_merged;
    score_merged.reserve(topk);
    size_t src_index = 0, target_index = 0;
    while(true) {
        //all score_src items are merged, if score_merged.size() still less than topk
        //move items from score_target to score_merged until score_merged.size() equal topk
        if(src_index >= src_count) {
            for(size_t i = target_index; i < target_count && score_merged.size() < topk; ++i) {
                score_merged.push_back(score_target[i]);
            }
            break;
        }

        //all score_target items are merged, if score_merged.size() still less than topk
        //move items from score_src to score_merged until score_merged.size() equal topk
        if(target_index >= target_count) {
            for(size_t i = src_index; i < src_count && score_merged.size() < topk; ++i) {
                score_merged.push_back(score_src[i]);
            }
            break;
        }

        //compare score, put smallest score to score_merged one by one
        auto& src_pair = score_src[src_index];
        auto& target_pair = score_target[target_index];
        if(src_pair.second > target_pair.second) {
            score_merged.push_back(target_pair);
            target_index++;
        } else {
            score_merged.push_back(src_pair);
            src_index++;
        }

        //score_merged.size() already equal topk
        if(score_merged.size() >= topk) {
            break;
        }
    }

    score_target.swap(score_merged);
}

void TopkResult(SearchContext::ResultSet &result_src,
                uint64_t topk,
                SearchContext::ResultSet &result_target) {
    if (result_target.empty()) {
        result_target.swap(result_src);
        return;
    }

    if (result_src.size() != result_target.size()) {
        SERVER_LOG_ERROR << "Invalid result set";
        return;
    }

    for (size_t i = 0; i < result_src.size(); i++) {
        SearchContext::Id2ScoreMap &score_src = result_src[i];
        SearchContext::Id2ScoreMap &score_target = result_target[i];
        MergeResult(score_src, score_target, topk);
    }
}

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

}

SearchTask::SearchTask()
: IScheduleTask(ScheduleTaskType::kSearch) {

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
        auto inner_k = index_engine_->Count() < context->topk() ? index_engine_->Count() : context->topk();
        output_ids.resize(inner_k*context->nq());
        output_distence.resize(inner_k*context->nq());

        try {
            //step 2: search
            index_engine_->Search(context->nq(), context->vectors(), inner_k, output_distence.data(),
                                  output_ids.data());

            rc.Record("do search");

            //step 3: cluster result
            SearchContext::ResultSet result_set;
            ClusterResult(output_ids, output_distence, context->nq(), inner_k, result_set);
            rc.Record("cluster result");

            //step 4: pick up topk result
            TopkResult(result_set, inner_k, context->GetResult());
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

}
}
}
