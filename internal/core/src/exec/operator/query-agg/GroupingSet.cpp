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

#include "GroupingSet.h"
#include "common/Utils.h"
#include "SumAggregateBase.h"

namespace milvus {
namespace exec {
GroupingSet::~GroupingSet() {
    if (isGlobal_ && lookup_) {
        AssertInfo(lookup_->hits_.size() == 1,
                   "GlobalAggregation should have exactly one output line");
        // globalAggregationBuffer_ is automatically cleaned up by unique_ptr
        // No need to manually delete[] or set to nullptr
        lookup_->hits_[0] = nullptr;
    }
}

void
GroupingSet::addInput(const RowVectorPtr& input) {
    auto numRows = input->size();
    numInputRows_ += numRows;
    if (isGlobal_) {
        addGlobalAggregationInput(input);
        return;
    }
    addInputForActiveRows(input);
}

void
GroupingSet::initializeGlobalAggregation() {
    if (globalAggregationInitialized_) {
        return;
    }
    lookup_ = std::make_unique<HashLookup>(hashers_);
    lookup_->reset(1);

    // Row layout is:
    //  - alternating null flag - one bit per flag, one pair per
    //                                             aggregation,
    //  - uint32_t row size,
    //  - fixed-width accumulators - one per aggregate
    //
    // Here we always make space for a row size since we only have one row and no
    // RowContainer.  The whole row is allocated to guarantee that alignment
    // requirements of all aggregate functions are satisfied.

    // Allocate space for the null and initialized flags.
    size_t numAggregates = aggregates_.size();
    int32_t rowSizeOffset = milvus::bits::nBytes(numAggregates);
    int32_t offset = rowSizeOffset + sizeof(int32_t);
    int32_t accumulatorFlagsOffset = 0;
    int32_t alignment = 1;

    for (auto& aggregate : aggregates_) {
        auto& function = aggregate.function_;
        Accumulator accumulator(function.get());
        // Accumulator offset must be aligned by their alignment size.
        offset = milvus::bits::roundUp(offset, accumulator.alignment());
        function->setOffsets(offset,
                             RowContainer::nullByte(accumulatorFlagsOffset),
                             RowContainer::nullMask(accumulatorFlagsOffset),
                             rowSizeOffset);
        offset += accumulator.fixedWidthSize();
        accumulatorFlagsOffset += 1;
        alignment =
            RowContainer::combineAlignments(accumulator.alignment(), alignment);
    }
    AssertInfo(__builtin_popcount(alignment) == 1,
               "alignment of aggregations must be power of two");
    offset = milvus::Align(offset, alignment);
    // Use unique_ptr for exception-safe memory management
    // If initializeNewGroups throws, the buffer will be automatically cleaned up
    globalAggregationBuffer_ = std::make_unique<char[]>(offset);
    lookup_->hits_[0] = globalAggregationBuffer_.get();
    const auto singleGroup = std::vector<vector_size_t>{0};
    for (auto& aggregate : aggregates_) {
        aggregate.function_->initializeNewGroups(lookup_->hits_.data(),
                                                 singleGroup);
    }
    globalAggregationInitialized_ = true;
}

void
GroupingSet::addGlobalAggregationInput(const milvus::RowVectorPtr& input) {
    initializeGlobalAggregation();
    auto* group = lookup_->hits_[0];
    for (auto i = 0; i < aggregates_.size(); i++) {
        auto& function = aggregates_[i].function_;
        populateTempVectors(i, input);
        function->addSingleGroupRawInput(group, tempVectors_);
    }
    tempVectors_.clear();
}

bool
GroupingSet::getGlobalAggregationOutput(milvus::RowVectorPtr& result) {
    initializeGlobalAggregation();  // when input from upstream operator is empty, we need to initialize the accumulators for global aggregation
    AssertInfo(lookup_->hits_.size() == 1,
               "GlobalAggregation should have exactly one output line");
    auto groups = lookup_->hits_.data();
    for (auto i = 0; i < aggregates_.size(); i++) {
        auto& function = aggregates_[i].function_;
        auto resultVector = result->child(aggregates_[i].output_);
        function->extractValues(groups, 1, &resultVector);
    }
    return true;
}

bool
GroupingSet::getOutput(milvus::RowVectorPtr& result) {
    if (isGlobal_) {
        return getGlobalAggregationOutput(result);
    }
    AssertInfo(hash_table_ != nullptr,
               "hash_table_ should not be nullptr for non-global aggregation");
    const auto& all_rows = hash_table_->rows()->allRows();
    if (!all_rows.empty()) {
        extractGroups(result);
        return true;
    }
    return false;
}

std::vector<Accumulator>
GroupingSet::accumulators() {
    std::vector<Accumulator> accumulators;
    accumulators.reserve(aggregates_.size());
    for (auto& aggregate : aggregates_) {
        accumulators.emplace_back(Accumulator{aggregate.function_.get()});
    }
    return accumulators;
}

void
GroupingSet::ensureInputFits(const RowVectorPtr& input) {
    //TODO memory check
}

void
GroupingSet::extractGroups(const milvus::RowVectorPtr& result) {
    RowContainer* rows = hash_table_->rows();
    const auto& groups = rows->allRows();
    result->resize(groups.size());
    auto totalKeys = rows->KeyTypes().size();
    auto groups_range =
        folly::Range<char**>(const_cast<char**>(groups.data()), groups.size());
    for (auto i = 0; i < totalKeys; i++) {
        auto keyVector = result->child(i);
        rows->extractColumn(
            groups_range.data(), groups_range.size(), i, keyVector);
    }
    for (auto i = 0; i < aggregates_.size(); i++) {
        auto& function = aggregates_[i].function_;
        auto aggregateVector = result->child(totalKeys + i);
        function->extractValues(
            groups_range.data(), groups_range.size(), &aggregateVector);
    }
}

void
GroupingSet::addInputForActiveRows(const RowVectorPtr& input) {
    AssertInfo(
        !isGlobal_,
        "Global aggregations should not reach add input for active rows");
    if (!hash_table_) {
        createHashTable();
    }
    ensureInputFits(input);
    hash_table_->prepareForGroupProbe(*lookup_, input);
    hash_table_->groupProbe(*lookup_);
    auto& hits = lookup_->hits_;
    auto* groups = hits.data();
    auto numGroups = hits.size();
    const auto& newGroups = lookup_->newGroups_;
    for (auto i = 0; i < aggregates_.size(); i++) {
        auto& function = aggregates_[i].function_;
        if (!newGroups.empty()) {
            function->initializeNewGroups(groups, newGroups);
        }
        populateTempVectors(i, input);
        function->addRawInput(groups, numGroups, tempVectors_);
    }
    tempVectors_.clear();
}

void
GroupingSet::populateTempVectors(int32_t aggregateIndex,
                                 const milvus::RowVectorPtr& input) {
    const auto& channel_idxes = aggregates_[aggregateIndex].input_column_idxes_;
    if (channel_idxes.empty() && input->childrens().size() == 1) {
        tempVectors_.resize(1);
        tempVectors_[0] = input->child(0);
    } else {
        tempVectors_.resize(channel_idxes.size());
        for (auto i = 0; i < channel_idxes.size(); i++) {
            tempVectors_[i] = input->child(channel_idxes[i]);
        }
    }
}

int32_t
GroupingSet::outputRowCount() const {
    return lookup_->newGroups_.size();
}

void
initializeAggregates(const std::vector<AggregateInfo>& aggregates,
                     RowContainer& rows) {
    const auto numKeys = rows.KeyTypes().size();
    int i = 0;
    for (auto& aggregate : aggregates) {
        auto& function = aggregate.function_;
        const auto& rowColumn = rows.columnAt(numKeys + i);
        function->setOffsets(rowColumn.offset(),
                             rowColumn.nullByte(),
                             rowColumn.nullMask(),
                             rows.rowSizeOffset());
        i++;
    }
}

void
GroupingSet::createHashTable() {
    hash_table_ =
        std::make_unique<HashTable>(std::move(hashers_), accumulators());
    auto& rows = *(hash_table_->rows());
    initializeAggregates(aggregates_, rows);
    lookup_ = std::make_unique<HashLookup>(hash_table_->hashers());
}

}  // namespace exec
}  // namespace milvus