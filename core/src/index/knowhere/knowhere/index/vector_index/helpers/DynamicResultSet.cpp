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
 * RangeSearchResult
 ***********************************************************************/

RangeSearchSet::RangeSearchSet () {
    labels = nullptr;
    distances = nullptr;
    buffer_size = 1024 * 256;
}

RangeSearchSet::~RangeSearchSet () {
    delete [] labels;
    delete [] distances;
}

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


/***********************************************************************
 * RangeSearchPartialResult
 ***********************************************************************/

void RangeQueryResult::add (float dis, idx_t id) {
    qnr++;
    pdr->add (id, dis);
}



RangeSearchPartialResult::RangeSearchPartialResult (size_t buffer_size):
    BufferPool(buffer_size)
{}


/// begin a new result
RangeQueryResult &
RangeSearchPartialResult::get_result (size_t qnr)
{
    query.pdr = this;
    query.qnr = qnr;
    return query;
}

/// called by range_search after do_allocation
void RangeSearchPartialResult::copy_result (size_t ofs, RangeSearchSet *res)
{
    copy_range (0, query.qnr,
                res->labels + ofs,
                res->distances + ofs);
}

DynamicResultSet::DynamicResultSet(size_t seg_nums, milvus::knowhere::RangeSearchSet* res_in): ns(seg_nums), res(res_in) {}

void DynamicResultSet::finalize ()
{
    set_lims ();

    allocation ();

    merge ();
}


/// called by range_search before do_allocation
void DynamicResultSet::set_lims ()
{
    base_boundaries.resize(ns + 1, 0);
    for (auto i = 0; i < ns; ++ i) {
        auto res_slice = seg_res[i];
        for (auto &prspr : res_slice) {
            base_boundaries[i] += prspr->query.qnr;
        }
        base_boundaries[i + 1] += base_boundaries[i];
    }
}

void DynamicResultSet::allocation() {
    res->labels = new idx_t[base_boundaries[ns]];
    res->distances = new float[base_boundaries[ns]];
}

void DynamicResultSet::merge() {
#pragma omp parallel for
    for (auto i = 0; i < ns; ++ i) {
        auto seg = seg_res[i];
        for (auto &prspr : seg) {
            prspr->copy_result(base_boundaries[i], res);
        }
    }
}

void ExchangeDataset(std::vector<RangeSearchPartialResult*> &milvus_dataset,
                     std::vector<faiss::RangeSearchPartialResult*> &faiss_dataset) {
    for (auto &prspr: faiss_dataset) {
        auto mrspr = new RangeSearchPartialResult(prspr->res->buffer_size);
        mrspr->wp = prspr->wp;
        auto qres = mrspr->get_result(prspr->queries[0].nres);
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

void MapUids(std::vector<RangeSearchPartialResult*> &milvus_dataset, std::shared_ptr<std::vector<IDType>> uids) {
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
