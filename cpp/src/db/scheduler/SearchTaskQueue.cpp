/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "SearchTaskQueue.h"
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
    for (auto i = 0; i < nq; i++) {
        SearchContext::Id2ScoreMap id_score;
        for (auto k = 0; k < topk; k++) {
            uint64_t index = i * topk + k;
            id_score.push_back(std::make_pair(output_ids[index], output_distence[index]));
        }
        result_set.emplace_back(id_score);
    }
}

void MergeResult(SearchContext::Id2ScoreMap &score_src,
        SearchContext::Id2ScoreMap &score_target,
        uint64_t topk) {
    for (auto& pair_src : score_src) {
        for (auto iter = score_target.begin(); iter != score_target.end(); ++iter) {
            if(pair_src.second > iter->second) {
                score_target.insert(iter, pair_src);
            }
        }
    }

    //remove unused items
    while (score_target.size() > topk) {
        score_target.pop_back();
    }
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

void CalcScore(uint64_t vector_count,
        const float *vectors_data,
        uint64_t dimension,
        const SearchContext::ResultSet &result_src,
        SearchContext::ResultSet &result_target) {
    result_target.clear();
    if(result_src.empty()){
        return;
    }

    int vec_index = 0;
    for(auto& result : result_src) {
        const float * vec_data = vectors_data + vec_index*dimension;
        double vec_len = 0;
        for(uint64_t i = 0; i < dimension; i++) {
            vec_len += vec_data[i]*vec_data[i];
        }
        vec_index++;

        SearchContext::Id2ScoreMap score_array;
        for(auto& pair : result) {
            score_array.push_back(std::make_pair(pair.first, (1 - pair.second/vec_len)*100.0));
        }
        result_target.emplace_back(score_array);
    }
}

}

bool SearchTask::DoSearch() {
    if(index_engine_ == nullptr) {
        return false;
    }

    server::TimeRecorder rc("DoSearch index(" + std::to_string(index_id_) + ")");

    std::vector<long> output_ids;
    std::vector<float> output_distence;
    for(auto& context : search_contexts_) {
        //step 1: allocate memory
        auto inner_k = index_engine_->Count() < context->topk() ? index_engine_->Count() : context->topk();
        output_ids.resize(inner_k*context->nq());
        output_distence.resize(inner_k*context->nq());

        //step 2: search
        try {
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

            //step 5: calculate score between 0 ~ 100
            CalcScore(context->nq(), context->vectors(), index_engine_->Dimension(), context->GetResult(), result_set);
            context->GetResult().swap(result_set);
            rc.Record("calculate score");

        } catch (std::exception& ex) {
            SERVER_LOG_ERROR << "SearchTask encounter exception: " << ex.what();
            context->IndexSearchDone(index_id_);//mark as done avoid dead lock, even search failed
            continue;
        }

        //step 6: notify to send result to client
        context->IndexSearchDone(index_id_);
    }

    rc.Elapse("totally cost");

    return true;
}

}
}
}
