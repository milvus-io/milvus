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

#pragma once

#include <string>
#include <vector>
#include <memory>
#include "knowhere/common/Typedef.h"
#include "faiss/impl/AuxIndexStructures.h"

namespace milvus {
namespace knowhere {

using idx_t = int64_t;
struct RangeSearchSet {
    idx_t *labels; /// result for query i is labels[lims[i]:lims[i + 1]]
    float *distances; /// corresponding distances, not sorted
    size_t buffer_size; /// size of the result buffer's size, when reaches this size, auto start a new buffer

    explicit RangeSearchSet();
    virtual ~RangeSearchSet();
};

struct BufferPool {
    size_t buffer_size; /// max size of a single buffer, in bytes
    struct Buffer {
        idx_t *ids;
        float *dis;
    };
    std::vector<Buffer> buffers; /// buffer pool
    size_t wp; /// writer pointer of the last buffer

    explicit BufferPool(size_t buffer_size);
    ~BufferPool();

    /// create a new buffer and append it to buffer pool
    void append();

    /// add one result
    void add(idx_t id, float dis);

    /// copy elements [ofs: ofs + n - 1] seen as liner data in the buffers to
    /// target dest_ids, dest_dis
    void copy_range(size_t ofs, size_t n, idx_t *dest_ids, float *dest_dis);
};

struct RangeSearchPartialResult;

struct RangeQueryResult {
    size_t qnr; /// Query's Number of Results: number of results for this query
    RangeSearchPartialResult *pdr; /// Pointer to DynamicResultset

    void add(float dis, idx_t id);
};

struct RangeSearchPartialResult: BufferPool {
    RangeQueryResult query; /// partial query result

    /// eventually the results will be stored in res_in
    explicit RangeSearchPartialResult(size_t buffer_size);

    /// begin a new result set
    RangeQueryResult& get_result(size_t qnr);

    void copy_result (size_t ofs, RangeSearchSet *res);

    /// merge a set of PartialResult's into one RangeSearchResult
    /// on ouptut the partialresults are empty!
//    static void merge (std::vector <RangeSearchPartialResult *> &
//                       partial_results, bool do_delete=true);

};

struct DynamicResultSet {
    size_t ns; /// number of segments
    RangeSearchSet *res; /// final result container
    std::vector<size_t> base_boundaries; /// prefix of the size of each query's result set
    std::vector<std::vector<RangeSearchPartialResult*>> seg_res; /// unmerged results of every segments

    DynamicResultSet(size_t seg_nums, RangeSearchSet *res_in);
    ~DynamicResultSet();

    /*****************************************
    * functions used at the end of the search to merge the result
    * lists */
    void finalize();

    /// called by range_search before allocate space for res
    void set_lims();

    /// called by range_search after do_allocation
    void copy_result();

    /// called by range_search after set_lims, allocate memory space 4 final result space
    void allocation();

    void merge();
};

void ExchangeDataset(std::vector<RangeSearchPartialResult*> &milvus_dataset,
                     std::vector<faiss::RangeSearchPartialResult*> &faiss_dataset);

void MapUids(std::vector<RangeSearchPartialResult*> &milvus_dataset, std::shared_ptr<std::vector<IDType>> uids);

}  // namespace knowhere
}  // namespace milvus
