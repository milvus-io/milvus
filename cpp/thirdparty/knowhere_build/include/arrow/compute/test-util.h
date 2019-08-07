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

#ifndef ARROW_COMPUTE_TEST_UTIL_H
#define ARROW_COMPUTE_TEST_UTIL_H

#include <memory>
#include <vector>

#include <gmock/gmock.h>

#include "arrow/array.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"

namespace arrow {
namespace compute {

class ComputeFixture {
 public:
  ComputeFixture() : ctx_(default_memory_pool()) {}

 protected:
  FunctionContext ctx_;
};

class MockUnaryKernel : public UnaryKernel {
 public:
  MOCK_METHOD3(Call, Status(FunctionContext* ctx, const Datum& input, Datum* out));
  MOCK_CONST_METHOD0(out_type, std::shared_ptr<DataType>());
};

class MockBinaryKernel : public BinaryKernel {
 public:
  MOCK_METHOD4(Call, Status(FunctionContext* ctx, const Datum& left, const Datum& right,
                            Datum* out));
  MOCK_CONST_METHOD0(out_type, std::shared_ptr<DataType>());
};

template <typename Type, typename T>
std::shared_ptr<Array> _MakeArray(const std::shared_ptr<DataType>& type,
                                  const std::vector<T>& values,
                                  const std::vector<bool>& is_valid) {
  std::shared_ptr<Array> result;
  if (is_valid.size() > 0) {
    ArrayFromVector<Type, T>(type, is_valid, values, &result);
  } else {
    ArrayFromVector<Type, T>(type, values, &result);
  }
  return result;
}

template <typename Type, typename Enable = void>
struct DatumEqual {};

template <typename Type>
struct DatumEqual<Type, typename std::enable_if<IsFloatingPoint<Type>::value>::type> {
  static constexpr double kArbitraryDoubleErrorBound = 1.0;
  using ScalarType = typename TypeTraits<Type>::ScalarType;

  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {
    ASSERT_EQ(lhs.kind(), rhs.kind());
    if (lhs.kind() == Datum::SCALAR) {
      auto left = internal::checked_cast<const ScalarType*>(lhs.scalar().get());
      auto right = internal::checked_cast<const ScalarType*>(rhs.scalar().get());
      ASSERT_EQ(left->is_valid, right->is_valid);
      ASSERT_EQ(left->type->id(), right->type->id());
      ASSERT_NEAR(left->value, right->value, kArbitraryDoubleErrorBound);
    }
  }
};

template <typename Type>
struct DatumEqual<Type, typename std::enable_if<!IsFloatingPoint<Type>::value>::type> {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {
    ASSERT_EQ(lhs.kind(), rhs.kind());
    if (lhs.kind() == Datum::SCALAR) {
      auto left = internal::checked_cast<const ScalarType*>(lhs.scalar().get());
      auto right = internal::checked_cast<const ScalarType*>(rhs.scalar().get());
      ASSERT_EQ(left->is_valid, right->is_valid);
      ASSERT_EQ(left->type->id(), right->type->id());
      ASSERT_EQ(left->value, right->value);
    }
  }
};

}  // namespace compute
}  // namespace arrow

#endif
