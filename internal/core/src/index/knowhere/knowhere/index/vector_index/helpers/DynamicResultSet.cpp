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

#include <cstring>
#include <iostream>
#include <string>
#include <utility>

#include "faiss/impl/AuxIndexStructures.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/helpers/DynamicResultSet.h"

namespace milvus {
namespace knowhere {

/***********************************************************************
 * DynamicResultSet
 ***********************************************************************/

void
DynamicResultSet::AlloctionImpl() {
    if (count <= 0) {
        KNOWHERE_THROW_MSG("DynamicResultSet::do_alloction failed because of count <= 0");
    }
    labels = std::shared_ptr<idx_t[]>(new idx_t[count], std::default_delete<idx_t[]>());
    distances = std::shared_ptr<float[]>(new float[count], std::default_delete<float[]>());
    //    labels = std::make_shared<idx_t []>(new idx_t[count], std::default_delete<idx_t[]>());
    //    distances = std::make_shared<float []>(new float[count], std::default_delete<float[]>());
}

void
DynamicResultSet::SortImpl(ResultSetPostProcessType postProcessType) {
    if (postProcessType == ResultSetPostProcessType::SortAsc) {
        quick_sort<true>(0, count);
    } else if (postProcessType == ResultSetPostProcessType::SortDesc) {
        quick_sort<false>(0, count);
    } else {
        KNOWHERE_THROW_MSG("invalid sort type!");
    }
}

template <bool asc>
void
DynamicResultSet::quick_sort(size_t lp, size_t rp) {
    auto len = rp - lp;
    if (len <= 1) {
        return;
    }
    auto pvot = lp + (len >> 1);
    size_t low = lp;
    size_t high = rp - 1;
    auto pids = labels.get();
    auto pdis = distances.get();
    std::swap(pdis[pvot], pdis[high]);
    std::swap(pids[pvot], pids[high]);
    if (asc) {
        while (low < high) {
            while (low < high && pdis[low] <= pdis[high]) {
                low++;
            }
            if (low == high) {
                break;
            }
            std::swap(pdis[low], pdis[high]);
            std::swap(pids[low], pids[high]);
            high--;
            while (low < high && pdis[high] >= pdis[low]) {
                high--;
            }
            if (low == high) {
                break;
            }
            std::swap(pdis[low], pdis[high]);
            std::swap(pids[low], pids[high]);
            low++;
        }
    } else {
        while (low < high) {
            while (low < high && pdis[low] >= pdis[high]) {
                low++;
            }
            if (low == high) {
                break;
            }
            std::swap(pdis[low], pdis[high]);
            std::swap(pids[low], pids[high]);
            high--;
            while (low < high && pdis[high] <= pdis[low]) {
                high--;
            }
            if (low == high) {
                break;
            }
            std::swap(pdis[low], pdis[high]);
            std::swap(pids[low], pids[high]);
            low++;
        }
    }
    quick_sort<asc>(lp, low);
    quick_sort<asc>(low, rp);
}

DynamicResultSet
DynamicResultCollector::Merge(size_t limit, ResultSetPostProcessType postProcessType) {
    if (limit <= 0) {
        KNOWHERE_THROW_MSG("limit must > 0!");
    }
    DynamicResultSet ret;
    auto seg_num = seg_results.size();
    std::vector<size_t> boundaries(seg_num + 1, 0);
#pragma omp parallel for
    for (auto i = 0; i < seg_num; ++i) {
        for (auto& pseg : seg_results[i]) {
            boundaries[i] += (pseg->buffer_size * pseg->buffers.size() - pseg->buffer_size + pseg->wp);
        }
    }
    for (size_t i = 0, ofs = 0; i <= seg_num; ++i) {
        auto bn = boundaries[i];
        boundaries[i] = ofs;
        ofs += bn;
        //        boundaries[i] += boundaries[i - 1];
    }
    ret.count = boundaries[seg_num] <= limit ? boundaries[seg_num] : limit;
    ret.AlloctionImpl();

    // abandon redundancy answers randomly
    // abandon strategy: keep the top limit sequentially
    int32_t pos = 1;
    for (int i = 1; i < boundaries.size(); ++i) {
        if (boundaries[i] >= ret.count) {
            pos = i;
            break;
        }
    }
    pos--;  // last segment id
    // full copy
#pragma omp parallel for
    for (auto i = 0; i < pos; ++i) {
        for (auto& pseg : seg_results[i]) {
            auto len = pseg->buffers.size() * pseg->buffer_size - pseg->buffer_size + pseg->wp;
            pseg->copy_range(0, len, ret.labels.get() + boundaries[i], ret.distances.get() + boundaries[i]);
            boundaries[i] += len;
        }
    }
    // partial copy
    auto last_len = ret.count - boundaries[pos];
    for (auto& pseg : seg_results[pos]) {
        auto len = pseg->buffers.size() * pseg->buffer_size - pseg->buffer_size + pseg->wp;
        auto ncopy = last_len > len ? len : last_len;
        pseg->copy_range(0, ncopy, ret.labels.get() + boundaries[pos], ret.distances.get() + boundaries[pos]);
        boundaries[pos] += ncopy;
        last_len -= ncopy;
        if (last_len <= 0) {
            break;
        }
    }

    if (postProcessType != ResultSetPostProcessType::None) {
        ret.SortImpl(postProcessType);
    }
    return ret;
}

void
DynamicResultCollector::Append(milvus::knowhere::DynamicResultSegment&& seg_result) {
    seg_results.emplace_back(std::move(seg_result));
}

void
ExchangeDataset(DynamicResultSegment& milvus_dataset, std::vector<faiss::RangeSearchPartialResult*>& faiss_dataset) {
    for (auto& prspr : faiss_dataset) {
        auto mrspr = std::make_shared<DynamicResultFragment>(prspr->res->buffer_size);
        mrspr->wp = prspr->wp;
        mrspr->buffers.resize(prspr->buffers.size());
        for (auto i = 0; i < prspr->buffers.size(); ++i) {
            mrspr->buffers[i].ids = prspr->buffers[i].ids;
            mrspr->buffers[i].dis = prspr->buffers[i].dis;
            prspr->buffers[i].ids = nullptr;
            prspr->buffers[i].dis = nullptr;
        }
        delete prspr->res;
        delete prspr;
        milvus_dataset.push_back(mrspr);
    }
}

}  // namespace knowhere
}  // namespace milvus
