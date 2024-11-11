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

namespace milvus{
namespace exec{
GroupingSet::~GroupingSet(){}

void GroupingSet::addInput(const RowVectorPtr& input, bool mayPushDown) {
    if (isGlobal_) {
        addGlobalAggregationInput(input, mayPushDown);
        return;
    }
    auto numRows = input->size();
    numInputRows_ += numRows;

    active_rows_.resize(numRows);
    active_rows_.set();
    addInputForActiveRows(input, mayPushDown);
}

void GroupingSet::initializeGlobalAggregation() {
    if (globalAggregationInitialized_) {
        return;
    }
    lookup_ = std::make_unique<HashLookup>(hashers_);
    lookup_->reset(1);

    // Row layout is:
    //  - alternating null flag, intialized flag - one bit per flag, one pair per
    //                                             aggregation,
    //  - uint32_t row size,
    //  - fixed-width accumulators - one per aggregate
    //
    // Here we always make space for a row size since we only have one row and no
    // RowContainer.  The whole row is allocated to guarantee that alignment
    // requirements of all aggregate functions are satisfied.

    // Allocate space for the null and initialized flags.
    size_t numAggregates = aggregates_.size();
    int32_t rowSizeOffset = milvus::bits::nBytes(numAggregates * RowContainer::kNumAccumulatorFlags);
    int32_t offset = rowSizeOffset + sizeof(int32_t);
    int32_t accumulatorFlagsOffset = 0;
    int32_t alignment = 1;

    for(auto& aggregate: aggregates_) {
        auto& function = aggregate.function_;
        Accumulator accumulator(function.get(), function->resultType());
        // Accumulator offset must be aligned by their alignment size.
        offset = milvus::bits::roundUp(offset, accumulator.alignment());
        function->setOffsets(offset,
                             RowContainer::nullByte(accumulatorFlagsOffset),
                             RowContainer::nullMask(accumulatorFlagsOffset),
                             RowContainer::initializedByte(accumulatorFlagsOffset),
                             RowContainer::initializedMask(accumulatorFlagsOffset),
                             rowSizeOffset);
        offset += accumulator.fixedWidthSize();
        accumulatorFlagsOffset += RowContainer::kNumAccumulatorFlags;
        alignment = RowContainer::combineAlignments(accumulator.alignment(), alignment);
    }
    lookup_->hits_[0] = new char[offset + alignment];//TODO memory allocation control
    const auto singleGroup = std::vector<vector_size_t>{0};
    for(auto& aggregate: aggregates_) {
        aggregate.function_->initializeNewGroups(lookup_->hits_.data(), singleGroup);
    }

    globalAggregationInitialized_ = true;
}

void GroupingSet::addGlobalAggregationInput(const milvus::RowVectorPtr& input, bool mayPushDown) {
    initializeGlobalAggregation();
    auto numRows = input->size();
    active_rows_.resize(numRows);
    active_rows_.set();
    auto* group = lookup_->hits_[0];
    for(auto i = 0; i < aggregates_.size(); i++) {
        auto& function = aggregates_[i].function_;
        populateTempVectors(i, input);
        function->addSingleGroupRawInput(group, active_rows_, tempVectors_, false);
    }
    tempVectors_.clear();
}

bool GroupingSet::getGlobalAggregationOutput(milvus::exec::RowContainerIterator &iterator,
                                             milvus::RowVectorPtr &result) {
    if (iterator.allocationIndex != 0) {
        return false;
    }
    initializeGlobalAggregation();
    auto groups = lookup_->hits_.data();
    for(auto i = 0; i < aggregates_.size(); i++) {
        auto& function = aggregates_[i].function_;
        auto resultVector = result->child(aggregates_[i].output_);
        function->extractValues(groups, 1, &resultVector);
    }
    iterator.allocationIndex = std::numeric_limits<int32_t>::max();
    return true;
}

bool GroupingSet::getOutput(int32_t maxOutputRows, int32_t maxOutputBytes, milvus::exec::RowContainerIterator &iterator,
                            milvus::RowVectorPtr &result) {
    if (isGlobal_) {
        return getGlobalAggregationOutput(iterator, result);
    }
    char* groups[maxOutputRows];
    const int32_t numGroups = hash_table_?hash_table_->rows()
            ->listRows(&iterator, maxOutputRows, maxOutputBytes, groups):0;
    if(numGroups == 0) {
        if (hash_table_ != nullptr) {
            hash_table_->clear();
        }
        return false;
    }
    extractGroups(folly::Range<char**>(groups, numGroups), result);
    return true;
}

std::vector<Accumulator> GroupingSet::accumulators(bool /*excludeToIntermediate*/) {
    std::vector<Accumulator> accumulators;
    accumulators.reserve(aggregates_.size());
    for(auto& aggregate: aggregates_) {
        accumulators.emplace_back(Accumulator{aggregate.function_.get(), aggregate.intermediateType_});
    }
    return accumulators;
}

void GroupingSet::ensureInputFits(const RowVectorPtr& input){
    //TODO memory check
}

void GroupingSet::extractGroups(folly::Range<char **> groups, const milvus::RowVectorPtr &result) {
    result->resize(groups.size());
    if (groups.empty()) {
        return;
    }
    RowContainer* rows = hash_table_->rows();
    auto totalKeys = rows->KeyTypes().size();
    for(auto i = 0; i < totalKeys; i++) {
        auto keyVector = result->child(i);
        rows->extractColumn(groups.data(), groups.size(), i, keyVector);
    }
    for (auto i = 0; i < aggregates_.size(); i++) {
        auto& function = aggregates_[i].function_;
        auto aggregateVector = result->child(totalKeys + i);
        function->extractValues(groups.data(), groups.size(), &aggregateVector);
    }
}


void GroupingSet::addInputForActiveRows(const RowVectorPtr& input, 
    bool mayPushdown) {
    AssertInfo(!isGlobal_, "Global aggregations should not reach add input for acitve rows");
    if (!hash_table_) {
        createHashTable();
    }
    ensureInputFits(input);

    hash_table_->prepareForGroupProbe(*lookup_, input, active_rows_, nullableKeys_);
    if (lookup_->rows_.empty()) {
        // No rows to probe. Can happen when ignoreNullKeys_ is true and all rows
        // have null keys.
        return;
    }
    hash_table_->groupProbe(*lookup_);
    auto* groups = lookup_->hits_.data();
    const auto& newGroups = lookup_->newGroups_;
    for(auto i = 0; i < aggregates_.size(); i++) {
        auto& function = aggregates_[i].function_;
        if (!newGroups.empty()) {
            function->initializeNewGroups(groups, newGroups);
        }
        if (!active_rows_.any()) {
            continue;
        }
        populateTempVectors(i, input);
        function->addRawInput(groups, active_rows_, tempVectors_, false);
    }
    tempVectors_.clear();
}

void GroupingSet::populateTempVectors(int32_t aggregateIndex, const milvus::RowVectorPtr &input) {
    const auto& channel_idxes = aggregates_[aggregateIndex].input_column_idxes_;
    tempVectors_.resize(channel_idxes.size());
    for(auto i = 0; i < channel_idxes.size(); i++) {
        tempVectors_[i] = input->child(channel_idxes[i]);
    }
}

void initializeAggregates(const std::vector<AggregateInfo>& aggregates, RowContainer& rows) {
    const auto numKeys = rows.KeyTypes().size();
    int i = 0;
    for (auto& aggregate : aggregates) {
        auto& function = aggregate.function_;
        //function->setAllocator(&rows.stringAllocator());
        const auto& rowColumn = rows.columnAt(numKeys + i);
        function->setOffsets(
            rowColumn.offset(),
            rowColumn.nullByte(),
            rowColumn.nullMask(),
            rowColumn.initializedByte(),
            rowColumn.initializedMask(),
            rows.rowSizeOffset());
        i++;
    }
}

void GroupingSet::createHashTable(){
    if (nullableKeys_) {
        hash_table_ = std::make_unique<HashTable<true>>(std::move(hashers_), accumulators(false));
    } else {
        hash_table_ = std::make_unique<HashTable<false>>(std::move(hashers_), accumulators(false));
    }

    auto& rows = *(hash_table_->rows());
    initializeAggregates(aggregates_, rows);
    auto numColumns = rows.KeyTypes().size() + aggregates_.size();
    lookup_ = std::make_unique<HashLookup>(hash_table_->hashers());
    if (!isAdaptive_ && hash_table_->hashMode() != BaseHashTable::HashMode::kHash) {
        hash_table_->forceGenericHashMode();
    }
}  

}
}