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

/*
 * Class: Dynamic result set (merged results)
 */
struct DynamicResultSet {
    std::shared_ptr<idx_t[]> labels;     /// result for query i is labels[lims[i]:lims[i + 1]]
    std::shared_ptr<float[]> distances;  /// corresponding distances, not sorted
    size_t count;  /// size of the result buffer's size, when reaches this size, auto start a new buffer

    void
    AlloctionImpl();

    void
    SortImpl(ResultSetPostProcessType postProcessType = ResultSetPostProcessType::SortAsc);

 private:
    template <bool asc>
    void
    quick_sort(size_t lp, size_t rp);
};

// BufferPool (inner classes)
typedef faiss::BufferList DynamicResultFragment;
typedef std::shared_ptr<DynamicResultFragment> DynamicResultFragmentPtr;
typedef std::vector<DynamicResultFragmentPtr> DynamicResultSegment;

/*
 * Class: Dynamic result collector
 * Example:
    DynamicResultCollector collector;
    for (auto &seg: segments) {
        auto seg_rst = seg.QueryByDistance(xxx);
        collector.append(seg_rst);
    }
    auto rst = collector.merge();
 */
struct DynamicResultCollector {
 public:
    /*
     * Merge the results of segments
     * Notes: Now, we apply limit before sort.
     *        It can be updated if you don't expect the policy.
     */
    DynamicResultSet
    Merge(size_t limit = 10000, ResultSetPostProcessType postProcessType = ResultSetPostProcessType::None);

    /*
     * Collect the results of segments
     */
    void
    Append(DynamicResultSegment&& seg_result);

 private:
    std::vector<DynamicResultSegment> seg_results;  /// unmerged results of every segments
};

void
ExchangeDataset(DynamicResultSegment& milvus_dataset, std::vector<faiss::RangeSearchPartialResult*>& faiss_dataset);

}  // namespace knowhere
}  // namespace milvus
