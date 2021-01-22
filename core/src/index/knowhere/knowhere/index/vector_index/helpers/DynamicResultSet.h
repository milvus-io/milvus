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

#include <memory>
#include <string>
#include <vector>
#include "faiss/impl/AuxIndexStructures.h"
#include "knowhere/common/Typedef.h"

namespace milvus {
namespace knowhere {

enum class ResultSetPostProcessType { None = 0, SortDesc, SortAsc };

using idx_t = int64_t;
struct DynamicResultSet {
    std::shared_ptr<idx_t[]> labels;     /// result for query i is labels[lims[i]:lims[i + 1]]
    std::shared_ptr<float[]> distances;  /// corresponding distances, not sorted
    size_t count;  /// size of the result buffer's size, when reaches this size, auto start a new buffer
    void
    do_alloction();
    void
    do_sort(ResultSetPostProcessType postProcessType = ResultSetPostProcessType::SortAsc);

 private:
    template <bool asc>
    void
    quick_sort(size_t lp, size_t rp);
};

struct BufferPool {
    size_t buffer_size;  /// max size of a single buffer, in bytes
    struct Buffer {
        idx_t* ids;
        float* dis;
    };
    std::vector<Buffer> buffers;  /// buffer pool
    size_t wp;                    /// writer pointer of the last buffer

    explicit BufferPool(size_t buffer_size);
    ~BufferPool();

    /// create a new buffer and append it to buffer pool
    void
    append();

    /// add one result
    void
    add(idx_t id, float dis);

    /// copy elements [ofs: ofs + n - 1] seen as liner data in the buffers to
    /// target dest_ids, dest_dis
    void
    copy_range(size_t ofs, size_t n, idx_t* dest_ids, float* dest_dis);
};

typedef BufferPool DynamicResultFragment;
typedef DynamicResultFragment* DynamicResultFragmentPtr;
typedef std::vector<DynamicResultFragmentPtr> DynamicResultSegment;

struct DynamicResultCollector {
 public:
    DynamicResultSet
    Merge(size_t limit = 10000, ResultSetPostProcessType postProcessType = ResultSetPostProcessType::None);
    void
    Append(DynamicResultSegment&& seg_result);

 private:
    std::vector<DynamicResultSegment> seg_results;  /// unmerged results of every segments

    /// called by range_search before allocate space for res
    void
    set_lims();

    /// called by range_search after do_allocation
    void
    copy_result();

    /// called by range_search after set_lims, allocate memory space 4 final result space
    void
    allocation();
};

void
ExchangeDataset(DynamicResultSegment& milvus_dataset, std::vector<faiss::RangeSearchPartialResult*>& faiss_dataset);

void
MapUids(DynamicResultSegment& milvus_dataset, std::shared_ptr<std::vector<IDType>> uids);

}  // namespace knowhere
}  // namespace milvus
