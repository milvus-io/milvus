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
    if (!vector.unique()) {
        vector = std::make_shared<BaseVector>(
            vector->type(), size, vector->nullCount());
    } else {
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
    const auto oldSize = size();
    BaseVector::resize(new_size, setNotNull);
    for (auto& child : childrens()) {
        if (new_size > oldSize) {
            child->resize(new_size, setNotNull);
        }
    }
}

}  // namespace milvus
