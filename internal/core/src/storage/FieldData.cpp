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

#include "storage/FieldData.h"
#include "exceptions/EasyAssert.h"
#include "storage/Util.h"
#include "common/FieldMeta.h"

namespace milvus::storage {

FieldData::FieldData(const Payload& payload) {
    std::shared_ptr<arrow::ArrayBuilder> builder;
    data_type_ = payload.data_type;

    if (milvus::datatype_is_vector(data_type_)) {
        AssertInfo(payload.dimension.has_value(), "empty dimension");
        builder = CreateArrowBuilder(data_type_, payload.dimension.value());
    } else {
        builder = CreateArrowBuilder(data_type_);
    }

    AddPayloadToArrowBuilder(builder, payload);
    auto ast = builder->Finish(&array_);
    AssertInfo(ast.ok(), "builder failed to finish");
}

// TODO ::Check arrow type with data_type
FieldData::FieldData(std::shared_ptr<arrow::Array> array, DataType data_type) : array_(array), data_type_(data_type) {
}

FieldData::FieldData(const uint8_t* data, int length) : data_type_(DataType::INT8) {
    auto builder = std::make_shared<arrow::Int8Builder>();
    auto ret = builder->AppendValues(data, data + length);
    AssertInfo(ret.ok(), "append value to builder failed");
    ret = builder->Finish(&array_);
    AssertInfo(ret.ok(), "builder failed to finish");
}

bool
FieldData::get_bool_payload(int idx) const {
    AssertInfo(array_ != nullptr, "null arrow array");
    AssertInfo(array_->type()->id() == arrow::Type::type::BOOL, "inconsistent data type");
    auto array = std::dynamic_pointer_cast<arrow::BooleanArray>(array_);
    AssertInfo(idx < array_->length(), "out range of bool array");
    return array->Value(idx);
}

void
FieldData::get_one_string_payload(int idx, char** cstr, int* str_size) const {
    AssertInfo(array_ != nullptr, "null arrow array");
    AssertInfo(array_->type()->id() == arrow::Type::type::STRING, "inconsistent data type");
    auto array = std::dynamic_pointer_cast<arrow::StringArray>(array_);
    AssertInfo(idx < array->length(), "index out of range array.length");
    arrow::StringArray::offset_type length;
    *cstr = (char*)array->GetValue(idx, &length);
    *str_size = length;
}

std::unique_ptr<Payload>
FieldData::get_payload() const {
    AssertInfo(array_ != nullptr, "null arrow array");
    auto raw_data_info = std::make_unique<Payload>();
    raw_data_info->rows = array_->length();
    raw_data_info->data_type = data_type_;
    raw_data_info->raw_data = GetRawValuesFromArrowArray(array_, data_type_);
    if (milvus::datatype_is_vector(data_type_)) {
        raw_data_info->dimension = GetDimensionFromArrowArray(array_, data_type_);
    }

    return raw_data_info;
}

// TODO :: handle string type
int
FieldData::get_data_size() const {
    auto payload = get_payload();
    return GetPayloadSize(payload.get());
}

}  // namespace milvus::storage
