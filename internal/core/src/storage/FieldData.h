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

#include <string>
#include <memory>

#include "storage/FieldDataInterface.h"

namespace milvus::storage {

template <typename Type>
class FieldData : public FieldDataImpl<Type, true> {
 public:
    static_assert(IsScalar<Type> || std::is_same_v<Type, PkType>);
    explicit FieldData(DataType data_type, int64_t buffered_num_rows = 0)
        : FieldDataImpl<Type, true>::FieldDataImpl(
              1, data_type, buffered_num_rows) {
    }
};

template <>
class FieldData<std::string> : public FieldDataStringImpl {
 public:
    static_assert(IsScalar<std::string> || std::is_same_v<std::string, PkType>);
    explicit FieldData(DataType data_type, int64_t buffered_num_rows = 0)
        : FieldDataStringImpl(data_type, buffered_num_rows) {
    }
};

template <>
class FieldData<Json> : public FieldDataJsonImpl {
 public:
    static_assert(IsScalar<std::string> || std::is_same_v<std::string, PkType>);
    explicit FieldData(DataType data_type, int64_t buffered_num_rows = 0)
        : FieldDataJsonImpl(data_type, buffered_num_rows) {
    }
};

template <>
class FieldData<FloatVector> : public FieldDataImpl<float, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       int64_t buffered_num_rows = 0)
        : FieldDataImpl<float, false>::FieldDataImpl(
              dim, data_type, buffered_num_rows) {
    }
};

template <>
class FieldData<BinaryVector> : public FieldDataImpl<uint8_t, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       int64_t buffered_num_rows = 0)
        : binary_dim_(dim),
          FieldDataImpl(dim / 8, data_type, buffered_num_rows) {
        Assert(dim % 8 == 0);
    }

    int64_t
    get_dim() const {
        return binary_dim_;
    }

 private:
    int64_t binary_dim_;
};

using FieldDataPtr = std::shared_ptr<FieldDataBase>;

}  // namespace milvus::storage