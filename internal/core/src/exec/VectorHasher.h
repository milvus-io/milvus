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

#include "common/Vector.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"

namespace milvus {
namespace exec {
class VectorHasher {
 public:
    VectorHasher(DataType data_type, column_index_t column_idx)
        : channel_type_(data_type), channel_idx_(column_idx) {
    }

    static std::unique_ptr<VectorHasher>
    create(DataType data_type, column_index_t col_idx) {
        return std::make_unique<VectorHasher>(data_type, col_idx);
    }

    column_index_t
    ChannelIndex() const {
        return channel_idx_;
    }

    DataType
    ChannelDataType() const {
        return channel_type_;
    }

    void
    hash(bool mix,
         const TargetBitmapView& activeRows,
         std::vector<uint64_t>& result);

    static constexpr uint64_t kNullHash = 1;

    static bool
    typeSupportValueIds(DataType type) {
        switch (type) {
            case DataType::BOOL:
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
            case DataType::VARCHAR:
            case DataType::STRING:
                return true;
            default:
                return false;
        }
    }

    template <DataType type>
    void
    hashValues(const ColumnVectorPtr& column_data,
               const TargetBitmapView& activeRows,
               bool mix,
               uint64_t* result);

    void
    setColumnData(const ColumnVectorPtr& column_data) {
        column_data_ = column_data;
    }

    const ColumnVectorPtr&
    columnData() const {
        return column_data_;
    }

 private:
    const column_index_t channel_idx_;
    const DataType channel_type_;
    ColumnVectorPtr column_data_;
};

std::vector<std::unique_ptr<VectorHasher>>
createVectorHashers(const RowTypePtr& rowType,
                    const std::vector<expr::FieldAccessTypeExprPtr>& exprs);

static std::unique_ptr<VectorHasher>
create(DataType dataType, column_index_t column_idx) {
    return std::make_unique<VectorHasher>(dataType, column_idx);
}

}  // namespace exec
}  // namespace milvus
