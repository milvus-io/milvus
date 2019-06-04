/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "SearchTaskQueue.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace zilliz {
namespace vecwise {
namespace engine {

namespace {
void ClusterResult(const std::vector<long> &output_ids,
                   const std::vector<float> &output_distence,
                   uint64_t nq,
                   uint64_t topk,
                   SearchContext::ResultSet &result_set) {
    result_set.clear();
    for (auto i = 0; i < nq; i++) {
        SearchContext::Score2IdMap score2id;
        for (auto k = 0; k < topk; k++) {
            uint64_t index = i * nq + k;
            score2id.insert(std::make_pair(output_distence[index], output_ids[index]));
        }
        result_set.emplace_back(score2id);
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
        SearchContext::Score2IdMap &score2id_src = result_src[i];
        SearchContext::Score2IdMap &score2id_target = result_target[i];
        for (auto iter = score2id_src.begin(); iter != score2id_src.end(); ++iter) {
            score2id_target.insert(std::make_pair(iter->first, iter->second));
        }

        //remove unused items
        while (score2id_target.size() > topk) {
            score2id_target.erase(score2id_target.rbegin()->first);
        }
    }
}
}

SearchTaskQueue&
SearchTaskQueue::GetInstance() {
    static SearchTaskQueue s_instance;
    return s_instance;
}

template<typename trait>
bool SearchTask<trait>::DoSearch() {
    if(index_engine_ == nullptr) {
        return false;
    }

    server::TimeRecorder rc("DoSearch");

    std::vector<long> output_ids;
    std::vector<float> output_distence;
    for(auto& context : search_contexts_) {
        auto inner_k = index_engine_->Count() < context->Topk() ? index_engine_->Count() : context->Topk();
        output_ids.resize(inner_k*context->Nq());
        output_distence.resize(inner_k*context->Nq());

        try {
            index_engine_->Search(context->Nq(), context->Vectors(), inner_k, output_distence.data(),
                                  output_ids.data());
        } catch (std::exception& ex) {
            SERVER_LOG_ERROR << "SearchTask encounter exception: " << ex.what();
        }

        rc.Record("do search");

        SearchContext::ResultSet result_set;
        ClusterResult(output_ids, output_distence, context->Nq(), inner_k, result_set);
        rc.Record("cluster result");
        TopkResult(result_set, inner_k, context->GetResult());
        rc.Record("reduce topk");
        context->IndexSearchDone(index_id_);
    }

    rc.Elapse("totally cost");

    return true;
}

}
}
}
