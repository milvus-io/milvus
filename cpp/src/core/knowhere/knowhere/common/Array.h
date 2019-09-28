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

#pragma once

#include <arrow/array.h>
#include <memory>

#include "Schema.h"

namespace zilliz {
namespace knowhere {

using ArrayData = arrow::ArrayData;
using ArrayDataPtr = std::shared_ptr<ArrayData>;

using Array = arrow::Array;
using ArrayPtr = std::shared_ptr<Array>;

using BooleanArray = arrow::BooleanArray;
using BooleanArrayPtr = std::shared_ptr<arrow::BooleanArray>;

template <typename DType>
using NumericArray = arrow::NumericArray<DType>;
template <typename DType>
using NumericArrayPtr = std::shared_ptr<arrow::NumericArray<DType>>;

using BinaryArray = arrow::BinaryArray;
using BinaryArrayPtr = std::shared_ptr<arrow::BinaryArray>;

using FixedSizeBinaryArray = arrow::FixedSizeBinaryArray;
using FixedSizeBinaryArrayPtr = std::shared_ptr<arrow::FixedSizeBinaryArray>;

using Decimal128Array = arrow::Decimal128Array;
using Decimal128ArrayPtr = std::shared_ptr<arrow::Decimal128Array>;

}  // namespace knowhere
}  // namespace zilliz
