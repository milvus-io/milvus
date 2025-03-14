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
#include "expr/FunctionSignature.h"
#include "plan/PlanNode.h"
#include "exec/QueryContext.h"
#include <folly/Synchronized.h>

namespace milvus {
namespace exec {
class Aggregate {
 protected:
    explicit Aggregate(DataType result_type) : result_type_(result_type) {
    }

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
    DataType
    resultType() const {
        return result_type_;
    }

    static std::unique_ptr<Aggregate>
    create(const std::string& name,
           plan::AggregationNode::Step step,
           const std::vector<DataType>& argTypes,
           const QueryConfig& query_config);

    void
    setOffsets(int32_t offset,
               int32_t nullByte,
               uint8_t nullMask,
               int32_t initializedByte,
               int8_t initializedMask,
               int32_t rowSizeOffset) {
        setOffsetsInternal(offset,
                           nullByte,
                           nullMask,
                           initializedByte,
                           initializedMask,
                           rowSizeOffset);
    }

    virtual void
    initializeNewGroups(char** groups,
                        folly::Range<const vector_size_t*> indices) {
        initializeNewGroupsInternal(groups, indices);
        for (auto index : indices) {
            groups[index][initializedByte_] |= initializedMask_;
        }
    }

    virtual void
    addSingleGroupRawInput(char* group,
                           const TargetBitmapView& activeRows,
                           const std::vector<VectorPtr>& input) = 0;

    virtual void
    addRawInput(char** groups,
                const TargetBitmapView& activeRows,
                const std::vector<VectorPtr>& input) = 0;

    virtual void
    extractValues(char** groups, int32_t numGroups, VectorPtr* result) = 0;

    template <typename T>
    T*
    value(char* group) const {
        AssertInfo(reinterpret_cast<uintptr_t>(group + offset_) %
                           accumulatorAlignmentSize() ==
                       0,
                   "aggregation value in the groups is not aligned");
        return reinterpret_cast<T*>(group + offset_);
    }

    bool
    isNull(char* group) const {
        return numNulls_ && (group[nullByte_] & nullMask_);
    }

    // Returns true if the accumulator never takes more than
    // accumulatorFixedWidthSize() bytes. If this is false, the
    // accumulator needs to track its changing variable length footprint
    // using RowSizeTracker (Aggregate::trackRowSize), see ArrayAggAggregate for
    // sample usage. A group row with at least one variable length key or
    // aggregate will have a 32-bit slot at offset RowContainer::rowSize_ for
    // keeping track of per-row size. The size is relevant for keeping caps on
    // result set and spilling batch sizes with skewed data.
    virtual bool
    isFixedSize() const {
        return true;
    }

    // Returns the fixed number of bytes the accumulator takes on a group
    // row. Variable width accumulators will reference the variable
    // width part of the state from the fixed part.
    virtual int32_t
    accumulatorFixedWidthSize() const = 0;

    /// Returns the alignment size of the accumulator.  Some types such as
    /// int128_t require aligned access.  This value must be a power of 2.
    virtual int32_t
    accumulatorAlignmentSize() const {
        return 1;
    }

 protected:
    virtual void
    setOffsetsInternal(int32_t offset,
                       int32_t nullByte,
                       uint8_t nullMask,
                       int32_t initializedByte,
                       uint8_t initializedMask,
                       int32_t rowSizeOffset);

    virtual void
    initializeNewGroupsInternal(char** groups,
                                folly::Range<const vector_size_t*> indices) = 0;
    // Number of null accumulators in the current state of the aggregation
    // operator for this aggregate. If 0, clearing the null as part of update
    // is not needed.
    uint64_t numNulls_ = 0;

    inline bool
    clearNull(char* group) {
        if (numNulls_) {
            uint8_t mask = group[nullByte_];
            if (mask & nullMask_) {
                group[nullByte_] = mask & ~nullMask_;
                numNulls_--;
                return true;
            }
        }
        return false;
    }

    void
    setAllNulls(char** groups, folly::Range<const vector_size_t*> indices) {
        for (auto i : indices) {
            groups[i][nullByte_] = nullMask_;
        }
        numNulls_ += indices.size();
    }
};

using AggregateFunctionFactory = std::function<std::unique_ptr<Aggregate>(
    plan::AggregationNode::Step step,
    const std::vector<DataType>& argTypes,
    const QueryConfig& config)>;

struct AggregateFunctionEntry {
    std::vector<expr::AggregateFunctionSignaturePtr> signatures;
    AggregateFunctionFactory factory;
};

const AggregateFunctionEntry*
getAggregateFunctionEntry(const std::string& name);

using AggregateFunctionMap = folly::Synchronized<
    std::unordered_map<std::string, AggregateFunctionEntry>>;

AggregateFunctionMap&
aggregateFunctions();

/// Register an aggregate function with the specified name and signatures. If
/// registerCompanionFunctions is true, also register companion aggregate and
/// scalar functions with it. When functions with `name` already exist, if
/// overwrite is true, existing registration will be replaced. Otherwise, return
/// false without overwriting the registry.
void
registerAggregateFunction(
    const std::string& name,
    const std::vector<std::shared_ptr<expr::AggregateFunctionSignature>>&
        signatures,
    const AggregateFunctionFactory& factory);

bool
isPartialOutput(milvus::plan::AggregationNode::Step step);

}  // namespace exec
}  // namespace milvus
