// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include "Structure.h"


namespace zilliz {
namespace knowhere {

ArrayPtr
ConstructInt64ArraySmart(uint8_t *data, int64_t size) {
    // TODO: magic
    std::vector<BufferPtr> id_buf{nullptr, MakeMutableBufferSmart(data, size)};
    auto type = std::make_shared<arrow::Int64Type>();
    auto id_array_data = arrow::ArrayData::Make(type, size / sizeof(int64_t), id_buf);
    return std::make_shared<NumericArray<arrow::Int64Type>>(id_array_data);
}

ArrayPtr
ConstructFloatArraySmart(uint8_t *data, int64_t size) {
    // TODO: magic
    std::vector<BufferPtr> id_buf{nullptr, MakeMutableBufferSmart(data, size)};
    auto type = std::make_shared<arrow::FloatType>();
    auto id_array_data = arrow::ArrayData::Make(type, size / sizeof(float), id_buf);
    return std::make_shared<NumericArray<arrow::FloatType>>(id_array_data);
}

TensorPtr
ConstructFloatTensorSmart(uint8_t *data, int64_t size, std::vector<int64_t> shape) {
    auto buffer = MakeMutableBufferSmart(data, size);
    auto float_type = std::make_shared<arrow::FloatType>();
    return std::make_shared<Tensor>(float_type, buffer, shape);
}

ArrayPtr
ConstructInt64Array(uint8_t *data, int64_t size) {
    // TODO: magic
    std::vector<BufferPtr> id_buf{nullptr, MakeMutableBuffer(data, size)};
    auto type = std::make_shared<arrow::Int64Type>();
    auto id_array_data = arrow::ArrayData::Make(type, size / sizeof(int64_t), id_buf);
    return std::make_shared<NumericArray<arrow::Int64Type>>(id_array_data);
}

ArrayPtr
ConstructFloatArray(uint8_t *data, int64_t size) {
    // TODO: magic
    std::vector<BufferPtr> id_buf{nullptr, MakeMutableBuffer(data, size)};
    auto type = std::make_shared<arrow::FloatType>();
    auto id_array_data = arrow::ArrayData::Make(type, size / sizeof(float), id_buf);
    return std::make_shared<NumericArray<arrow::FloatType>>(id_array_data);
}

TensorPtr
ConstructFloatTensor(uint8_t *data, int64_t size, std::vector<int64_t> shape) {
    auto buffer = MakeMutableBuffer(data, size);
    auto float_type = std::make_shared<arrow::FloatType>();
    return std::make_shared<Tensor>(float_type, buffer, shape);
}

FieldPtr
ConstructInt64Field(const std::string &name) {
    auto type = std::make_shared<arrow::Int64Type>();
    return std::make_shared<Field>(name, type);
}


FieldPtr
ConstructFloatField(const std::string &name) {
    auto type = std::make_shared<arrow::FloatType>();
    return std::make_shared<Field>(name, type);
}
}
}
