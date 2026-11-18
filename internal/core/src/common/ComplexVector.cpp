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

#include <memory>
#include <optional>
#include <vector>

#include "Vector.h"
#include "common/EasyAssert.h"
#include "common/Types.h"

namespace milvus {

VectorPtr
MakeColumnVector(DataType type, size_t size, std::optional<size_t> null_count) {
    return DispatchVectorType(type, [&]<typename T>() -> VectorPtr {
        return std::make_shared<ColumnVector<T>>(type, size, null_count);
    });
}

VectorPtr
MakeColumnVector(FieldDataPtr values, TargetBitmap valid, size_t null_count) {
    AssertInfo(values != nullptr, "Column values cannot be null");
    return DispatchVectorType(
        values->get_data_type(), [&]<typename T>() -> VectorPtr {
            return std::make_shared<ColumnVector<T>>(
                std::move(values), std::move(valid), null_count);
        });
}

void
BaseVector::prepareForReuse() {
    null_count_ = std::nullopt;
}

void
RowVector::resize(milvus::vector_size_t new_size, bool setNotNull) {
    BaseVector::resize(new_size, setNotNull);
    // Always propagate resize to all children to maintain the invariant that
    // child->size() == row->size(). This ensures consistency when shrinking
    // as well as growing, preventing children from being longer than the row.
    for (auto& child : childrens()) {
        child->resize(new_size, setNotNull);
    }
}

}  // namespace milvus
