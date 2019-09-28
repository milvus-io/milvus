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

#include "knowhere/adapter/ArrowAdapter.h"

namespace zilliz {
namespace knowhere {

ArrayPtr
CopyArray(const ArrayPtr& origin) {
    ArrayPtr copy = nullptr;
    auto copy_data = origin->data()->Copy();
    switch (origin->type_id()) {
#define DEFINE_TYPE(type, clazz)                          \
    case arrow::Type::type: {                             \
        copy = std::make_shared<arrow::clazz>(copy_data); \
    }
        DEFINE_TYPE(BOOL, BooleanArray)
        DEFINE_TYPE(BINARY, BinaryArray)
        DEFINE_TYPE(FIXED_SIZE_BINARY, FixedSizeBinaryArray)
        DEFINE_TYPE(DECIMAL, Decimal128Array)
        DEFINE_TYPE(FLOAT, NumericArray<arrow::FloatType>)
        DEFINE_TYPE(INT64, NumericArray<arrow::Int64Type>)
        default:
            break;
    }
    return copy;
}

SchemaPtr
CopySchema(const SchemaPtr& origin) {
    std::vector<std::shared_ptr<Field>> fields;
    for (auto& field : origin->fields()) {
        auto copy = std::make_shared<Field>(field->name(), field->type(), field->nullable(), nullptr);
        fields.emplace_back(copy);
    }
    return std::make_shared<Schema>(std::move(fields));
}

}  // namespace knowhere
}  // namespace zilliz
