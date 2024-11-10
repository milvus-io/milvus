// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License
#pragma once

#include "common/Types.h"
#include "plan/PlanNode.h"

namespace milvus{
namespace exec{
class Aggregate {
protected:
    explicit Aggregate(DataType result_type): result_type_(result_type){}
private:
    const DataType result_type_;

    // Byte position of null flag in group row.
    int32_t nullByte_;
    uint8_t nullMask_;
    // Byte position of the initialized flag in group row.
    int32_t initializedByte_;
    uint8_t initializedMask_;
    // Offset of fixed length accumulator state in group row.
    int32_t offset_;
    // Offset of uint32_t row byte size of row. 0 if there are no
    // variable width fields or accumulators on the row.  The size is
    // capped at 4G and will stay at 4G and not wrap around if growing
    // past this. This serves to track the batch size when extracting
    // rows. A size in excess of 4G would finish the batch in any case,
    // so larger values need not be represented.
    int32_t rowSizeOffset_ = 0;

public:
    DataType resultType() const {
        return result_type_;
    }

    static std::unique_ptr<Aggregate> create(
            const std::string& name,
            plan::AggregationNode::Step step,
            const std::vector<DataType>& argTypes,
            DataType resultType);

    void setOffsets(
        int32_t offset,
        int32_t nullByte,
        uint8_t nullMask,
        int32_t initializedByte,
        int8_t initializedMask,
        int32_t rowSizeOffset) {
        setOffsetsInternal(offset, nullByte, nullMask, initializedByte, initializedMask, rowSizeOffset);        
    }

    virtual void initializeNewGroups(char** groups, folly::Range<const vector_size_t*> indices) {
        for(auto index : indices) {
            groups[index][initializedByte_] |= initializedMask_;
        }
    }

    virtual void addSingleGroupRawInput(char* group, const TargetBitmapView& activeRows,
                                        const std::vector<VectorPtr>& input, bool mayPushDown) {};

    virtual void addRawInput(char** groups, const TargetBitmapView& activeRows,
                             const std::vector<VectorPtr>& input, bool mayPushDown) {} ;

    virtual void extractValues(char** groups, int32_t numGroups, VectorPtr* result) {};

    // Returns true if the accumulator never takes more than
    // accumulatorFixedWidthSize() bytes. If this is false, the
    // accumulator needs to track its changing variable length footprint
    // using RowSizeTracker (Aggregate::trackRowSize), see ArrayAggAggregate for
    // sample usage. A group row with at least one variable length key or
    // aggregate will have a 32-bit slot at offset RowContainer::rowSize_ for
    // keeping track of per-row size. The size is relevant for keeping caps on
    // result set and spilling batch sizes with skewed data.
    virtual bool isFixedSize() const {
        return true;
    }

    // Returns the fixed number of bytes the accumulator takes on a group
    // row. Variable width accumulators will reference the variable
    // width part of the state from the fixed part.
    virtual int32_t accumulatorFixedWidthSize() const = 0;

    /// Returns the alignment size of the accumulator.  Some types such as
    /// int128_t require aligned access.  This value must be a power of 2.
    virtual int32_t accumulatorAlignmentSize() const {
        return 1;
    }

protected:
    virtual void setOffsetsInternal(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t initializedByte,
      uint8_t initializedMask,
      int32_t rowSizeOffset);

    virtual void initializeNewGroupsInternal(char** groups, folly::Range<const vector_size_t*> indices) = 0;
};

bool isRawInput(milvus::plan::AggregationNode::Step step);

bool isPartialOutput(milvus::plan::AggregationNode::Step step);

}
}
