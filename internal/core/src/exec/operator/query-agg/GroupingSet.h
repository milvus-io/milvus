// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include "common/Types.h"
#include "exec/VectorHasher.h"
#include "AggregateInfo.h"
#include "exec/HashTable.h"
#include "plan/PlanNode.h"
#include "RowContainer.h"

namespace milvus {
namespace exec {

class GroupingSet {
 public:
    GroupingSet(const RowTypePtr& input_type,
                std::vector<std::unique_ptr<VectorHasher>>&& hashers,
                std::vector<AggregateInfo>&& aggregates,
                bool ignoreNullKeys,
                int64_t group_limit)
        : hashers_(std::move(hashers)),
          aggregates_(std::move(aggregates)),
          ignoreNullKeys_(ignoreNullKeys),
          group_limit_(group_limit) {
        isGlobal_ = hashers_.empty();
    }

    ~GroupingSet();

    void
    addInput(const RowVectorPtr& input);

    void
    initializeGlobalAggregation();

    void
    addGlobalAggregationInput(const RowVectorPtr& input);

    void
    addInputForActiveRows(const RowVectorPtr& input);

    void
    createHashTable();

    std::vector<Accumulator>
    accumulators();

    // Checks if input will fit in the existing memory and increases reservation
    // if not. If reservation cannot be increased, spills enough to make 'input'
    // fit.
    void
    ensureInputFits(const RowVectorPtr& input);

    bool
    getOutput(RowVectorPtr& result);

    void
    extractGroups(folly::Range<char**> groups, const RowVectorPtr& result);

    void
    populateTempVectors(int32_t aggregateIndex, const RowVectorPtr& input);

    bool
    getGlobalAggregationOutput(RowVectorPtr& result);

    int32_t
    outputRowCount() const;

 private:
    bool isGlobal_;
    const bool ignoreNullKeys_;
    const int64_t group_limit_;

    std::vector<std::unique_ptr<VectorHasher>> hashers_;
    std::vector<AggregateInfo> aggregates_;

    // Place for the arguments of the aggregate being updated.
    std::vector<VectorPtr> tempVectors_;
    std::unique_ptr<BaseHashTable> hash_table_;
    std::unique_ptr<HashLookup> lookup_;
    TargetBitmap active_rows_;

    uint64_t numInputRows_ = 0;

    // Boolean indicating whether accumulators for a global aggregation (i.e.
    // aggregation with no grouping keys) have been initialized.
    bool globalAggregationInitialized_{false};
};

}  // namespace exec
}  // namespace milvus
