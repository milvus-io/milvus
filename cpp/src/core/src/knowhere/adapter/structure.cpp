////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "knowhere/adapter/structure.h"


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
