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

#include "storage/Util.h"
#include "exceptions/EasyAssert.h"
#include "common/Consts.h"
#include "config/ConfigChunkManager.h"

namespace milvus::storage {

StorageType
ReadMediumType(PayloadInputStream* input_stream) {
    AssertInfo(input_stream->Tell().Equals(arrow::Result<int64_t>(0)), "medium type must be parsed from stream header");
    int32_t magic_num;
    auto ret = input_stream->Read(sizeof(magic_num), &magic_num);
    AssertInfo(ret.ok(), "read input stream failed");
    if (magic_num == MAGIC_NUM) {
        return StorageType::Remote;
    }

    return StorageType::LocalDisk;
}

void
add_vector_payload(std::shared_ptr<arrow::ArrayBuilder> builder, uint8_t* values, int length) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto binary_builder = std::dynamic_pointer_cast<arrow::FixedSizeBinaryBuilder>(builder);
    auto ast = binary_builder->AppendValues(values, length);
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

// append values for numeric data
template <typename DT, typename BT>
void
add_numeric_payload(std::shared_ptr<arrow::ArrayBuilder> builder, DT* start, int length) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto numeric_builder = std::dynamic_pointer_cast<BT>(builder);
    auto ast = numeric_builder->AppendValues(start, start + length);
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

void
AddPayloadToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder, const Payload& payload) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto raw_data = const_cast<uint8_t*>(payload.raw_data);
    auto length = payload.rows;
    auto data_type = payload.data_type;

    switch (data_type) {
        case DataType::BOOL: {
            auto bool_data = reinterpret_cast<bool*>(raw_data);
            add_numeric_payload<bool, arrow::BooleanBuilder>(builder, bool_data, length);
            break;
        }
        case DataType::INT8: {
            auto int8_data = reinterpret_cast<int8_t*>(raw_data);
            add_numeric_payload<int8_t, arrow::Int8Builder>(builder, int8_data, length);
            break;
        }
        case DataType::INT16: {
            auto int16_data = reinterpret_cast<int16_t*>(raw_data);
            add_numeric_payload<int16_t, arrow::Int16Builder>(builder, int16_data, length);
            break;
        }
        case DataType::INT32: {
            auto int32_data = reinterpret_cast<int32_t*>(raw_data);
            add_numeric_payload<int32_t, arrow::Int32Builder>(builder, int32_data, length);
            break;
        }
        case DataType::INT64: {
            auto int64_data = reinterpret_cast<int64_t*>(raw_data);
            add_numeric_payload<int64_t, arrow::Int64Builder>(builder, int64_data, length);
            break;
        }
        case DataType::FLOAT: {
            auto float_data = reinterpret_cast<float*>(raw_data);
            add_numeric_payload<float, arrow::FloatBuilder>(builder, float_data, length);
            break;
        }
        case DataType::DOUBLE: {
            auto double_data = reinterpret_cast<double_t*>(raw_data);
            add_numeric_payload<double, arrow::DoubleBuilder>(builder, double_data, length);
            break;
        }
        case DataType::VECTOR_BINARY:
        case DataType::VECTOR_FLOAT: {
            add_vector_payload(builder, const_cast<uint8_t*>(raw_data), length);
            break;
        }
        default: {
            PanicInfo("unsupported data type");
        }
    }
}

void
AddOneStringToArrowBuilder(std::shared_ptr<arrow::ArrayBuilder> builder, const char* str, int str_size) {
    AssertInfo(builder != nullptr, "empty arrow builder");
    auto string_builder = std::dynamic_pointer_cast<arrow::StringBuilder>(builder);
    arrow::Status ast;
    if (str == nullptr || str_size < 0) {
        ast = string_builder->AppendNull();
    } else {
        ast = string_builder->Append(str, str_size);
    }
    AssertInfo(ast.ok(), "append value to arrow builder failed");
}

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::BOOL: {
            return std::make_shared<arrow::BooleanBuilder>();
        }
        case DataType::INT8: {
            return std::make_shared<arrow::Int8Builder>();
        }
        case DataType::INT16: {
            return std::make_shared<arrow::Int16Builder>();
        }
        case DataType::INT32: {
            return std::make_shared<arrow::Int32Builder>();
        }
        case DataType::INT64: {
            return std::make_shared<arrow::Int64Builder>();
        }
        case DataType::FLOAT: {
            return std::make_shared<arrow::FloatBuilder>();
        }
        case DataType::DOUBLE: {
            return std::make_shared<arrow::DoubleBuilder>();
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            return std::make_shared<arrow::StringBuilder>();
        }
        default: {
            PanicInfo("unsupported numeric data type");
        }
    }
}

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(DataType data_type, int dim) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::VECTOR_FLOAT: {
            AssertInfo(dim > 0, "invalid dim value");
            return std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow::fixed_size_binary(dim * sizeof(float)));
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0 && dim > 0, "invalid dim value");
            return std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow::fixed_size_binary(dim / 8));
        }
        default: {
            PanicInfo("unsupported vector data type");
        }
    }
}

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::BOOL: {
            return arrow::schema({arrow::field("val", arrow::boolean())});
        }
        case DataType::INT8: {
            return arrow::schema({arrow::field("val", arrow::int8())});
        }
        case DataType::INT16: {
            return arrow::schema({arrow::field("val", arrow::int16())});
        }
        case DataType::INT32: {
            return arrow::schema({arrow::field("val", arrow::int32())});
        }
        case DataType::INT64: {
            return arrow::schema({arrow::field("val", arrow::int64())});
        }
        case DataType::FLOAT: {
            return arrow::schema({arrow::field("val", arrow::float32())});
        }
        case DataType::DOUBLE: {
            return arrow::schema({arrow::field("val", arrow::float64())});
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            return arrow::schema({arrow::field("val", arrow::utf8())});
        }
        default: {
            PanicInfo("unsupported numeric data type");
        }
    }
}

std::shared_ptr<arrow::Schema>
CreateArrowSchema(DataType data_type, int dim) {
    switch (static_cast<DataType>(data_type)) {
        case DataType::VECTOR_FLOAT: {
            AssertInfo(dim > 0, "invalid dim value");
            return arrow::schema({arrow::field("val", arrow::fixed_size_binary(dim * sizeof(float)))});
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0 && dim > 0, "invalid dim value");
            return arrow::schema({arrow::field("val", arrow::fixed_size_binary(dim / 8))});
        }
        default: {
            PanicInfo("unsupported vector data type");
        }
    }
}

// TODO ::handle string type
int64_t
GetPayloadSize(const Payload* payload) {
    switch (payload->data_type) {
        case DataType::BOOL:
            return payload->rows * sizeof(bool);
        case DataType::INT8:
            return payload->rows * sizeof(int8_t);
        case DataType::INT16:
            return payload->rows * sizeof(int16_t);
        case DataType::INT32:
            return payload->rows * sizeof(int32_t);
        case DataType::INT64:
            return payload->rows * sizeof(int64_t);
        case DataType::FLOAT:
            return payload->rows * sizeof(float);
        case DataType::DOUBLE:
            return payload->rows * sizeof(double);
        case DataType::VECTOR_FLOAT: {
            Assert(payload->dimension.has_value());
            return payload->rows * payload->dimension.value() * sizeof(float);
        }
        case DataType::VECTOR_BINARY: {
            Assert(payload->dimension.has_value());
            return payload->rows * payload->dimension.value();
        }
        default:
            PanicInfo("unsupported data type");
    }
}

const uint8_t*
GetRawValuesFromArrowArray(std::shared_ptr<arrow::Array> data, DataType data_type) {
    switch (data_type) {
        case DataType::INT8: {
            AssertInfo(data->type()->id() == arrow::Type::type::INT8, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::Int8Array>(data);
            return reinterpret_cast<const uint8_t*>(array->raw_values());
        }
        case DataType::INT16: {
            AssertInfo(data->type()->id() == arrow::Type::type::INT16, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::Int16Array>(data);
            return reinterpret_cast<const uint8_t*>(array->raw_values());
        }
        case DataType::INT32: {
            AssertInfo(data->type()->id() == arrow::Type::type::INT32, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::Int32Array>(data);
            return reinterpret_cast<const uint8_t*>(array->raw_values());
        }
        case DataType::INT64: {
            AssertInfo(data->type()->id() == arrow::Type::type::INT64, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::Int64Array>(data);
            return reinterpret_cast<const uint8_t*>(array->raw_values());
        }
        case DataType::FLOAT: {
            AssertInfo(data->type()->id() == arrow::Type::type::FLOAT, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::FloatArray>(data);
            return reinterpret_cast<const uint8_t*>(array->raw_values());
        }
        case DataType::DOUBLE: {
            AssertInfo(data->type()->id() == arrow::Type::type::DOUBLE, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::DoubleArray>(data);
            return reinterpret_cast<const uint8_t*>(array->raw_values());
        }
        case DataType::VECTOR_FLOAT: {
            AssertInfo(data->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(data);
            return reinterpret_cast<const uint8_t*>(array->raw_values());
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(data->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(data);
            return reinterpret_cast<const uint8_t*>(array->raw_values());
        }
        default:
            PanicInfo("unsupported data type");
    }
}

int
GetDimensionFromArrowArray(std::shared_ptr<arrow::Array> data, DataType data_type) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT: {
            AssertInfo(data->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(data);
            return array->byte_width() / sizeof(float);
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(data->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY, "inconsistent data type");
            auto array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(data);
            return array->byte_width() * 8;
        }
        default:
            PanicInfo("unsupported data type");
    }
}

std::string
GenLocalIndexPathPrefix(int64_t build_id, int64_t index_version) {
    return milvus::ChunkMangerConfig::GetLocalBucketName() + "/" + std::string(INDEX_ROOT_PATH) + "/" +
           std::to_string(build_id) + "/" + std::to_string(index_version) + "/";
}

std::string
GetLocalIndexPathPrefixWithBuildID(int64_t build_id) {
    return milvus::ChunkMangerConfig::GetLocalBucketName() + "/" + std::string(INDEX_ROOT_PATH) + "/" +
           std::to_string(build_id);
}

std::string
GenRawDataPathPrefix(int64_t segment_id, int64_t field_id) {
    return milvus::ChunkMangerConfig::GetLocalBucketName() + "/" + std::string(RAWDATA_ROOT_PATH) + "/" +
           std::to_string(segment_id) + "/" + std::to_string(field_id) + "/";
}

std::string
GetLocalRawDataPathPrefixWithBuildID(int64_t segment_id) {
    return milvus::ChunkMangerConfig::GetLocalBucketName() + "/" + std::string(RAWDATA_ROOT_PATH) + "/" +
           std::to_string(segment_id);
}

}  // namespace milvus::storage
