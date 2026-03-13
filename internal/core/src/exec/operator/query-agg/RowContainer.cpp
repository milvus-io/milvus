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

#include <string.h>
#include <cstdint>

#include "common/BitUtil.h"
#include "common/Vector.h"
#include "exec/operator/query-agg/Aggregate.h"

namespace milvus {
namespace exec {

RowContainer::RowContainer(const std::vector<DataType>& keyTypes,
                           const std::vector<Accumulator>& accumulators)
    : keyTypes_(keyTypes), accumulators_(accumulators) {
    int32_t offset = 0;
    bool isVariableWidth = false;
    int idx = 0;
    for (auto type : keyTypes_) {
        bool varLength = !IsFixedSizeType(type);
        isVariableWidth |= varLength;
        if (varLength) {
            variable_offsets_.emplace_back(offset);
            variable_idxes_.emplace_back(idx);
        }
        offsets_.push_back(offset);
        if (type == DataType::VARCHAR || type == DataType::STRING) {
            offset += 8;  //use a pointer to store string
        } else {
            offset += GetDataTypeSize(type, 1);
        }
        idx++;
    }
    const int32_t firstAggregateOffset = offset;
    for (const auto& accumulator : accumulators) {
        isVariableWidth |= !accumulator.isFixedSize();
        alignment_ = combineAlignments(accumulator.alignment(), alignment_);
    }

    // set up 1 null-bit for each key or accumulator
    auto null_bit_count = keyTypes_.size() + accumulators.size();
    flagBytes_ = milvus::bits::nBytes(null_bit_count);
    offset += flagBytes_;

    for (const auto& accumulator : accumulators) {
        offset = milvus::bits::roundUp(offset, accumulator.alignment());
        offsets_.push_back(offset);
        offset += accumulator.fixedWidthSize();
    }
    AssertInfo(offsets_.size() == keyTypes_.size() + accumulators.size(),
               "wrong size of offsets in RowContainer");
    if (isVariableWidth) {
        rowSizeOffset_ = offset;
        offset += sizeof(uint32_t);
    }
    fixedRowSize_ = milvus::bits::roundUp(offset, alignment_);
    for (auto i = 0; i < offsets_.size(); i++) {
        rowColumns_.emplace_back(offsets_[i], firstAggregateOffset * 8 + i);
    }
}

RowContainer::~RowContainer() {
    clear();
}

char*
RowContainer::initializeRow(char* row) {
    std::memset(row, 0, fixedRowSize_);
    return row;
}

char*
RowContainer::newRow() {
    char* row = new char[fixedRowSize_];
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

Accumulator::Accumulator(bool isFixedSize, int32_t fixedSize, int32_t alignment)
    : isFixedSize_(isFixedSize), fixedSize_(fixedSize), alignment_(alignment) {
}

Accumulator::Accumulator(milvus::exec::Aggregate* aggregate)
    : isFixedSize_(false), fixedSize_(0), alignment_(0) {
    AssertInfo(aggregate != nullptr,
               "Input aggregate for accumulator cannot be nullptr!");
    isFixedSize_ = aggregate->isFixedSize();
    fixedSize_ = aggregate->accumulatorFixedWidthSize();
    alignment_ = aggregate->accumulatorAlignmentSize();
}

}  // namespace exec
}  // namespace milvus
