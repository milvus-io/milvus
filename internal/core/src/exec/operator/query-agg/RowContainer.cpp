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

#include "RowContainer.h"
#include "common/BitUtil.h"
#include "common/Vector.h"

namespace milvus {
namespace exec {

RowContainer::RowContainer(const std::vector<DataType>& keyTypes,
                           const std::vector<Accumulator>& accumulators,
                           bool ignoreNullKeys)
    : keyTypes_(keyTypes),
      accumulators_(accumulators),
      ignoreNullKeys_(ignoreNullKeys) {
    int32_t offset = 0;
    int32_t nullOffset = 0;
    bool isVariableWidth = false;
    int idx = 0;
    for (auto& type : keyTypes_) {
        bool varLength = !IsFixedSizeType(type);
        isVariableWidth |= varLength;
        if (varLength) {
            variable_offsets.emplace_back(offset);
            variable_idxes.emplace_back(idx);
        }
        offsets_.push_back(offset);
        if (type == DataType::VARCHAR || type == DataType::STRING) {
            offset += 8;  //use a pointer to store string
        } else {
            offset += GetDataTypeSize(type, 1);
        }
        nullOffsets_.push_back(nullOffset);
        if (!ignoreNullKeys_) {
            ++nullOffset;
        }
        idx++;
    }
    // Make offset at least sizeof pointer so that there is space for a
    // free list next pointer below the bit at 'freeFlagOffset_'.
    offset = std::max<int32_t>(offset, sizeof(void*));
    const int32_t firstAggregateOffset = offset;
    if (!accumulators.empty()) {
        // This moves nullOffset to the start of the next byte.
        // This is to guarantee the null and initialized bits for an aggregate
        // always appear in the same byte.
        nullOffset = (nullOffset + 7) & -8;
    }
    for (const auto& accumulator : accumulators) {
        // Initialized bit.  Set when the accumulator is initialized.
        nullOffsets_.push_back(nullOffset);
        ++nullOffset;
        // Null bit.
        nullOffsets_.push_back(nullOffset);
        ++nullOffset;
        isVariableWidth |= !accumulator.isFixedSize();
        alignment_ = combineAlignments(accumulator.alignment(), alignment_);
    }

    // Free flag.
    nullOffsets_.push_back(nullOffset);
    freeFlagOffset_ = nullOffset + firstAggregateOffset * 8;
    ++nullOffset;
    // Add 1 to the last null offset to get the number of bits.
    flagBytes_ = milvus::bits::nBytes(nullOffsets_.back() + 1);
    for (auto i = 0; i < nullOffsets_.size(); i++) {
        nullOffsets_[i] += firstAggregateOffset * 8;
    }
    offset += flagBytes_;

    for (const auto& accumulator : accumulators) {
        offset = milvus::bits::roundUp(offset, accumulator.alignment());
        offsets_.push_back(offset);
        offset += accumulator.fixedWidthSize();
    }
    if (isVariableWidth) {
        rowSizeOffset_ = offset;
        offset += sizeof(uint32_t);
    }
    fixedRowSize_ = milvus::bits::roundUp(offset, alignment_);

    // A distinct hash table has no aggregates and if the hash table has
    // no nulls, it may be that there are no null flags.
    if (!nullOffsets_.empty()) {
        // All flags like free and null flags for keys and non-keys
        // start as 0. This is also used to mark aggregates as uninitialized on row
        // creation.
        initialNulls_.resize(flagBytes_, 0x0);
    }
    size_t nullOffsetsPos = 0;
    uint16_t column_sum = keyTypes_.size() + accumulators.size();
    for (auto i = 0; i < offsets_.size(); i++) {
        rowColumns_.emplace_back(offsets_[i],
                                 (!ignoreNullKeys_ || i >= keyTypes_.size())
                                     ? nullOffsets_[nullOffsetsPos]
                                     : RowColumn::kNotNullOffset);
        // offsets_ contains the offsets for keys, then accumulators
        // This captures the case where i is the index of an accumulator.
        if (!accumulators.empty() && i >= keyTypes_.size() && i < column_sum) {
            nullOffsetsPos += kNumAccumulatorFlags;
        } else {
            ++nullOffsetsPos;
        }
    }
}

char*
RowContainer::initializeRow(char* row) {
    std::memset(row, 0, fixedRowSize_);
    return row;
}

char*
RowContainer::newRow() {
    char* row = new char[fixedRowSize_];
    if (rows_.size() < numRows_ + 1) {
        rows_.reserve(numRows_ + 1024);
    }
    rows_.emplace_back(row);
    ++numRows_;
    return initializeRow(row);
}

void
RowContainer::store(const milvus::ColumnVectorPtr& column_data,
                    milvus::vector_size_t index,
                    char* row,
                    int32_t column_index) {
    auto numKeys = keyTypes_.size();
    bool isKey = column_index < numKeys;
    if (isKey && ignoreNullKeys_) {
        MILVUS_DYNAMIC_TYPE_DISPATCH(storeNoNulls,
                                     keyTypes_[column_index],
                                     column_data,
                                     index,
                                     row,
                                     offsets_[column_index]);
    } else {
        AssertInfo(isKey || accumulators_.empty(),
                   "Should only store into rows for key");
        auto rowColumn = rowColumns_[column_index];
        MILVUS_DYNAMIC_TYPE_DISPATCH(storeWithNull,
                                     keyTypes_[column_index],
                                     column_data,
                                     index,
                                     row,
                                     rowColumn.offset(),
                                     rowColumn.nullByte(),
                                     rowColumn.nullMask());
    }
}

Accumulator::Accumulator(bool isFixedSize, int32_t fixedSize, int32_t alignment)
    : isFixedSize_{isFixedSize}, fixedSize_{fixedSize}, alignment_{alignment} {
}

Accumulator::Accumulator(milvus::exec::Aggregate* aggregate)
    : isFixedSize_(aggregate->isFixedSize()),
      fixedSize_{aggregate->accumulatorFixedWidthSize()},
      alignment_(aggregate->accumulatorAlignmentSize()) {
    AssertInfo(aggregate != nullptr,
               "Input aggregate for accumulator cannot be nullptr!");
}

}  // namespace exec
}  // namespace milvus
