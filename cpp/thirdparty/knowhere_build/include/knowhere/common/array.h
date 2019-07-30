#pragma once

#include "arrow/array.h"
#include "knowhere/common/schema.h"


namespace zilliz {
namespace knowhere {

using ArrayData = arrow::ArrayData;
using ArrayDataPtr = std::shared_ptr<ArrayData>;

using Array = arrow::Array;
using ArrayPtr = std::shared_ptr<Array>;

using BooleanArray = arrow::BooleanArray;
using BooleanArrayPtr = std::shared_ptr<arrow::BooleanArray>;

template<typename DType>
using NumericArray = arrow::NumericArray<DType>;
template<typename DType>
using NumericArrayPtr = std::shared_ptr<arrow::NumericArray<DType>>;

using BinaryArray = arrow::BinaryArray;
using BinaryArrayPtr = std::shared_ptr<arrow::BinaryArray>;

using FixedSizeBinaryArray = arrow::FixedSizeBinaryArray;
using FixedSizeBinaryArrayPtr = std::shared_ptr<arrow::FixedSizeBinaryArray>;

using Decimal128Array = arrow::Decimal128Array;
using Decimal128ArrayPtr = std::shared_ptr<arrow::Decimal128Array>;


} // namespace knowhere
} // namespace zilliz
