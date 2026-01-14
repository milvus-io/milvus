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

#include "Vector.h"

namespace milvus {

void
BaseVector::prepareForReuse(milvus::VectorPtr& vector,
                            milvus::vector_size_t size) {
    // Guard against null shared_ptr dereference
    AssertInfo(vector != nullptr,
               "BaseVector::prepareForReuse: vector cannot be null");

    // Use use_count() instead of unique() (deprecated in C++17, removed in C++20)
    if (vector.use_count() != 1) {
        // When vector is non-unique (shared by multiple owners), create a new
        // instance of the same dynamic type using the virtual factory method to
        // preserve the subclass type (e.g., ColumnVector, RowVector) instead of
        // creating a BaseVector.
        vector = vector->cloneEmpty(size);
    } else {
        // When vector is unique (only one owner), reuse it by resetting state
        // and resizing. This preserves subclass state (e.g., ColumnVector's
        // values_, RowVector's children_values_).
        vector->prepareForReuse();
        vector->resize(size);
    }
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
