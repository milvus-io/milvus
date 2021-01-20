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

#include <string>
#include <cstring>
#include <iostream>
#include <src/index/thirdparty/faiss/impl/AuxIndexStructures.h>
#include "DynamicResultSet.h"

namespace milvus {
namespace knowhere {

/***********************************************************************
 * DynamicResultSet
 ***********************************************************************/

/***********************************************************************
 * BufferPool
 ***********************************************************************/


BufferPool::BufferPool (size_t buffer_size):
    buffer_size (buffer_size)
{
    wp = buffer_size;
}

BufferPool::~BufferPool ()
{
    for (int i = 0; i < buffers.size(); i++) {
        delete [] buffers[i].ids;
        delete [] buffers[i].dis;
    }
}

void BufferPool::add (idx_t id, float dis) {
    if (wp == buffer_size) { // need new buffer
        append();
    }
    Buffer & buf = buffers.back();
    buf.ids [wp] = id;
    buf.dis [wp] = dis;
    wp++;
}


void BufferPool::append ()
{
    Buffer buf = {new idx_t [buffer_size], new float [buffer_size]};
    buffers.push_back (buf);
    wp = 0;
}

/// copy elemnts ofs:ofs+n-1 seen as linear data in the buffers to
/// tables dest_ids, dest_dis
void BufferPool::copy_range (size_t ofs, size_t n,
                             idx_t * dest_ids, float *dest_dis)
{
    size_t bno = ofs / buffer_size;
    ofs -= bno * buffer_size;
    while (n > 0) {
        size_t ncopy = ofs + n < buffer_size ? n : buffer_size - ofs;
        Buffer buf = buffers [bno];
        memcpy (dest_ids, buf.ids + ofs, ncopy * sizeof(*dest_ids));
        memcpy (dest_dis, buf.dis + ofs, ncopy * sizeof(*dest_dis));
        dest_ids += ncopy;
        dest_dis += ncopy;
        ofs = 0;
        bno ++;
        n -= ncopy;
    }
}

DynamicResultSet DynamicResultCollector::Merge(size_t limit, bool need_sort) {
    auto seg_num = seg_results.size();
    std::vector<size_t> boundaries(seg_num + 1, 0);
#pragma omp parallel for
    for (auto i = 0; i < seg_num; ++ i) {
        for (auto &pseg : seg_results[i]) {
            boundaries[i] += (pseg->buffer_size * pseg->buffers.size() - pseg->buffer_size + pseg->wp);
        }
    }
    for (auto i = 1; i < boundaries.size(); ++ i)
        boundaries[i] += boundaries[i - 1];
    if (boundaries[seg_num] <= limit) {
        // do merge
        if (need_sort) {
            // do sort
        }
    } else {
        if (need_sort) {}
    }
#pragma omp parallel for
    for (auto i = 0; i < ns; ++ i) {
        auto seg = seg_res[i];
        for (auto &prspr : seg) {
            prspr->copy_result(base_boundaries[i], res);
        }
    }
}

void DynamicResultCollector::Append(milvus::knowhere::DynamicResultSegment&& seg_result) {
    seg_results.push_back(std::move(seg_result));
}

void ExchangeDataset(DynamicResultSegment &milvus_dataset,
                     std::vector<faiss::RangeSearchPartialResult*> &faiss_dataset) {
    for (auto &prspr: faiss_dataset) {
        auto mrspr = new DynamicResultFragment(prspr->res->buffer_size);
        mrspr->wp = prspr->wp;
        mrspr->buffers.resize(prspr->buffers.size());
        for (auto i = 0; i < prspr->buffers.size(); ++ i) {
            mrspr->buffers[i].ids = prspr->buffers[i].ids;
            mrspr->buffers[i].dis = prspr->buffers[i].dis;
            prspr->buffers[i].ids = nullptr;
            prspr->buffers[i].dis = nullptr;
        }
        delete prspr->res;
        milvus_dataset.push_back(mrspr);
    }
}

void MapUids(DynamicResultSegment &milvus_dataset, std::shared_ptr<std::vector<IDType>> uids) {
    if (uids) {
        for (auto &mrspr : milvus_dataset) {
            for (auto j = 0; j < mrspr->buffers.size() - 1; ++ j) {
                auto buf = mrspr->buffers[j];
                for (auto i = 0; i < mrspr->buffer_size; ++ i) {
                    if (buf.ids[i] >= 0)
                        buf.ids[i] = uids->at(buf.ids[i]);
                }
            }
            auto buf = mrspr->buffers[mrspr->buffers.size() - 1];
            for (auto i = 0; i < mrspr->wp; ++ i) {
                if (buf.ids[i] >= 0)
                    buf.ids[i] = uids->at(buf.ids[i]);
            }
        }
    }
}

}  // namespace knowhere
}  // namespace milvus
