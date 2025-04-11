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
#include <utility>

#include <oneapi/tbb/concurrent_queue.h>

#include "common/FieldDataInterface.h"
#include "common/Channel.h"
#include "parquet/arrow/reader.h"

namespace milvus {

template <typename Type>
class FieldData : public FieldDataImpl<Type, true> {
 public:
    static_assert(IsScalar<Type> || std::is_same_v<Type, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataImpl<Type, true>::FieldDataImpl(
              1, data_type, nullable, buffered_num_rows) {
    }
    static_assert(IsScalar<Type> || std::is_same_v<Type, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       FixedVector<Type>&& inner_data)
        : FieldDataImpl<Type, true>::FieldDataImpl(
              1, data_type, nullable, std::move(inner_data)) {
    }
};

template <>
class FieldData<std::string> : public FieldDataStringImpl {
 public:
    static_assert(IsScalar<std::string> || std::is_same_v<std::string, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataStringImpl(data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<Json> : public FieldDataJsonImpl {
 public:
    static_assert(IsScalar<std::string> || std::is_same_v<std::string, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataJsonImpl(data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<TIMESTAMP> : public FieldDataImpl<int64_t, true> {
 public:
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataImpl<int64_t, true>::FieldDataImpl(1, data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<Array> : public FieldDataArrayImpl {
 public:
    static_assert(IsScalar<Array> || std::is_same_v<std::string, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataArrayImpl(data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<FloatVector> : public FieldDataImpl<float, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       int64_t buffered_num_rows = 0)
        : FieldDataImpl<float, false>::FieldDataImpl(
              dim, data_type, false, buffered_num_rows) {
    }
};

template <>
class FieldData<BinaryVector> : public FieldDataImpl<uint8_t, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       int64_t buffered_num_rows = 0)
        : FieldDataImpl(dim / 8, data_type, false, buffered_num_rows),
          binary_dim_(dim) {
        Assert(dim % 8 == 0);
    }

    int64_t
    get_dim() const override {
        return binary_dim_;
    }

 private:
    int64_t binary_dim_;
};

template <>
class FieldData<Float16Vector> : public FieldDataImpl<float16, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       int64_t buffered_num_rows = 0)
        : FieldDataImpl<float16, false>::FieldDataImpl(
              dim, data_type, false, buffered_num_rows) {
    }
};

template <>
class FieldData<BFloat16Vector> : public FieldDataImpl<bfloat16, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       int64_t buffered_num_rows = 0)
        : FieldDataImpl<bfloat16, false>::FieldDataImpl(
              dim, data_type, false, buffered_num_rows) {
    }
};

template <>
class FieldData<SparseFloatVector> : public FieldDataSparseVectorImpl {
 public:
    explicit FieldData(DataType data_type, int64_t buffered_num_rows = 0)
        : FieldDataSparseVectorImpl(data_type, buffered_num_rows) {
    }
};

template <>
class FieldData<Int8Vector> : public FieldDataImpl<int8, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       int64_t buffered_num_rows = 0)
        : FieldDataImpl<int8, false>::FieldDataImpl(
              dim, data_type, false, buffered_num_rows) {
    }
};

using FieldDataPtr = std::shared_ptr<FieldDataBase>;
using FieldDataChannel = Channel<FieldDataPtr>;
using FieldDataChannelPtr = std::shared_ptr<FieldDataChannel>;

struct ArrowDataWrapper {
    ArrowDataWrapper() = default;
    ArrowDataWrapper(std::shared_ptr<arrow::RecordBatchReader> reader,
                     std::shared_ptr<parquet::arrow::FileReader> arrow_reader,
                     std::shared_ptr<uint8_t[]> file_data)
        : reader(std::move(reader)),
          arrow_reader(std::move(arrow_reader)),
          file_data(std::move(file_data)) {
    }
    std::shared_ptr<arrow::RecordBatchReader> reader;
    // file reader must outlive the record batch reader
    std::shared_ptr<parquet::arrow::FileReader> arrow_reader;
    // underlying file data memory, must outlive the arrow reader
    std::shared_ptr<uint8_t[]> file_data;
};
using ArrowReaderChannel = Channel<std::shared_ptr<milvus::ArrowDataWrapper>>;

FieldDataPtr
InitScalarFieldData(const DataType& type, bool nullable, int64_t cap_rows);

}  // namespace milvus