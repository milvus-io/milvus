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
                bool nullableKeys,
                bool isRawInput):
                hashers_(std::move(hashers)),
                isGlobal_(hashers_.empty()),
                isRawInput_(isRawInput),
                aggregates_(std::move(aggregates)),
                nullableKeys_(nullableKeys),
                isAdaptive_(true){}

    ~GroupingSet();

    void addInput(const RowVectorPtr& input, bool mayPushDown);

    void initializeGlobalAggregation();

    void addGlobalAggregationInput(const RowVectorPtr& input, bool mayPushDown);

    void addInputForActiveRows(const RowVectorPtr& input, bool mayPushdown);

    void createHashTable();

    std::vector<Accumulator> accumulators(bool excludeToIntermediate);

    // Checks if input will fit in the existing memory and increases reservation
    // if not. If reservation cannot be increased, spills enough to make 'input'
    // fit.
    void ensureInputFits(const RowVectorPtr& input);

    bool getOutput(int32_t maxOutputRows,
                   int32_t maxOutputBytes,
                   RowContainerIterator& iterator,
                   RowVectorPtr& result);

    bool hasOutput() {
        return noMoreInput_;
    }

    void extractGroups(folly::Range<char**> groups, const RowVectorPtr& result);

    void populateTempVectors(int32_t aggregateIndex, const RowVectorPtr& input);

    bool getGlobalAggregationOutput(RowContainerIterator& iterator, RowVectorPtr& result);

private:
    const bool isGlobal_;
    const bool isRawInput_;
    const bool nullableKeys_;
    
    std::vector<std::unique_ptr<VectorHasher>> hashers_;
    std::vector<AggregateInfo> aggregates_;

    // Place for the arguments of the aggregate being updated.
    std::vector<VectorPtr> tempVectors_;
    std::unique_ptr<BaseHashTable> hash_table_;
    std::unique_ptr<HashLookup> lookup_;
    TargetBitmap active_rows_;

    uint64_t numInputRows_ = 0;


    const bool isAdaptive_;

    bool noMoreInput_{false};

    // Boolean indicating whether accumulators for a global aggregation (i.e.
    // aggregation with no grouping keys) have been initialized.
    bool globalAggregationInitialized_{false};
};

}
}
