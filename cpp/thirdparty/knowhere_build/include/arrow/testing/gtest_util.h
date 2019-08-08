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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

template <typename T>
class Result;

namespace internal {

// Helpers for the ASSERT* macros below

inline Status GenericToStatus(const Status& st) { return st; }

template <typename T>
inline Status GenericToStatus(const Result<T>& res) {
  return res.status();
}

}  // namespace internal
}  // namespace arrow

// NOTE: failing must be inline in the macros below, to get correct file / line number
// reporting on test failures.

#define ASSERT_RAISES(ENUM, expr)                                                     \
  do {                                                                                \
    auto _res = (expr);                                                               \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res);                   \
    if (!_st.Is##ENUM()) {                                                            \
      FAIL() << "Expected '" ARROW_STRINGIFY(expr) "' to fail with " ARROW_STRINGIFY( \
                    ENUM) ", but got "                                                \
             << _st.ToString();                                                       \
    }                                                                                 \
  } while (false)

#define ASSERT_RAISES_WITH_MESSAGE(ENUM, message, expr)                               \
  do {                                                                                \
    auto _res = (expr);                                                               \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res);                   \
    if (!_st.Is##ENUM()) {                                                            \
      FAIL() << "Expected '" ARROW_STRINGIFY(expr) "' to fail with " ARROW_STRINGIFY( \
                    ENUM) ", but got "                                                \
             << _st.ToString();                                                       \
    }                                                                                 \
    ASSERT_EQ((message), _st.ToString());                                             \
  } while (false)

#define ASSERT_OK(expr)                                                       \
  do {                                                                        \
    auto _res = (expr);                                                       \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res);           \
    if (!_st.ok()) {                                                          \
      FAIL() << "'" ARROW_STRINGIFY(expr) "' failed with " << _st.ToString(); \
    }                                                                         \
  } while (false)

#define ASSERT_OK_NO_THROW(expr) ASSERT_NO_THROW(ASSERT_OK(expr))

#define ARROW_EXPECT_OK(expr)                                       \
  do {                                                              \
    auto _res = (expr);                                             \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res); \
    EXPECT_TRUE(_st.ok());                                          \
  } while (false)

#define ABORT_NOT_OK(expr)                                          \
  do {                                                              \
    auto _res = (expr);                                             \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res); \
    if (ARROW_PREDICT_FALSE(!_st.ok())) {                           \
      _st.Abort();                                                  \
    }                                                               \
  } while (false);

namespace arrow {

// ----------------------------------------------------------------------
// Useful testing::Types declarations

typedef ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type,
                         Int16Type, Int32Type, Int64Type, FloatType, DoubleType>
    NumericArrowTypes;

class Array;
class ChunkedArray;
class Column;
class RecordBatch;
class Table;

namespace compute {
struct Datum;
}

using Datum = compute::Datum;

using ArrayVector = std::vector<std::shared_ptr<Array>>;

#define ASSERT_ARRAYS_EQUAL(lhs, rhs) AssertArraysEqual((lhs), (rhs))
#define ASSERT_BATCHES_EQUAL(lhs, rhs) AssertBatchesEqual((lhs), (rhs))

ARROW_EXPORT void AssertArraysEqual(const Array& expected, const Array& actual);
ARROW_EXPORT void AssertBatchesEqual(const RecordBatch& expected,
                                     const RecordBatch& actual);
ARROW_EXPORT void AssertChunkedEqual(const ChunkedArray& expected,
                                     const ChunkedArray& actual);
ARROW_EXPORT void AssertChunkedEqual(const ChunkedArray& actual,
                                     const ArrayVector& expected);
ARROW_EXPORT void AssertBufferEqual(const Buffer& buffer,
                                    const std::vector<uint8_t>& expected);
ARROW_EXPORT void AssertBufferEqual(const Buffer& buffer, const std::string& expected);
ARROW_EXPORT void AssertBufferEqual(const Buffer& buffer, const Buffer& expected);
ARROW_EXPORT void AssertSchemaEqual(const Schema& lhs, const Schema& rhs);

ARROW_EXPORT void AssertTablesEqual(const Table& expected, const Table& actual,
                                    bool same_chunk_layout = true);

ARROW_EXPORT void AssertDatumsEqual(const Datum& expected, const Datum& actual);

template <typename C_TYPE>
void AssertNumericDataEqual(const C_TYPE* raw_data,
                            const std::vector<C_TYPE>& expected_values) {
  for (auto expected : expected_values) {
    ASSERT_EQ(expected, *raw_data);
    ++raw_data;
  }
}

ARROW_EXPORT void CompareBatch(const RecordBatch& left, const RecordBatch& right,
                               bool compare_metadata = true);

// Check if the padding of the buffers of the array is zero.
// Also cause valgrind warnings if the padding bytes are uninitialized.
ARROW_EXPORT void AssertZeroPadded(const Array& array);

// Check if the valid buffer bytes are initialized
// and cause valgrind warnings otherwise.
ARROW_EXPORT void TestInitialized(const Array& array);

template <typename BuilderType>
void FinishAndCheckPadding(BuilderType* builder, std::shared_ptr<Array>* out) {
  ASSERT_OK(builder->Finish(out));
  AssertZeroPadded(**out);
  TestInitialized(**out);
}

#define DECL_T() typedef typename TestFixture::T T;

#define DECL_TYPE() typedef typename TestFixture::Type Type;

// ArrayFromJSON: construct an Array from a simple JSON representation

ARROW_EXPORT
std::shared_ptr<Array> ArrayFromJSON(const std::shared_ptr<DataType>&,
                                     const std::string& json);

// ArrayFromVector: construct an Array from vectors of C values

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ArrayFromVector(const std::shared_ptr<DataType>& type,
                     const std::vector<bool>& is_valid, const std::vector<C_TYPE>& values,
                     std::shared_ptr<Array>* out) {
  auto type_id = TYPE::type_id;
  ASSERT_EQ(type_id, type->id())
      << "template parameter and concrete DataType instance don't agree";

  std::unique_ptr<ArrayBuilder> builder_ptr;
  ASSERT_OK(MakeBuilder(default_memory_pool(), type, &builder_ptr));
  // Get the concrete builder class to access its Append() specializations
  auto& builder = dynamic_cast<typename TypeTraits<TYPE>::BuilderType&>(*builder_ptr);

  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder.Append(values[i]));
    } else {
      ASSERT_OK(builder.AppendNull());
    }
  }
  ASSERT_OK(builder.Finish(out));
}

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ArrayFromVector(const std::shared_ptr<DataType>& type,
                     const std::vector<C_TYPE>& values, std::shared_ptr<Array>* out) {
  auto type_id = TYPE::type_id;
  ASSERT_EQ(type_id, type->id())
      << "template parameter and concrete DataType instance don't agree";

  std::unique_ptr<ArrayBuilder> builder_ptr;
  ASSERT_OK(MakeBuilder(default_memory_pool(), type, &builder_ptr));
  // Get the concrete builder class to access its Append() specializations
  auto& builder = dynamic_cast<typename TypeTraits<TYPE>::BuilderType&>(*builder_ptr);

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_OK(builder.Append(values[i]));
  }
  ASSERT_OK(builder.Finish(out));
}

// Overloads without a DataType argument, for parameterless types

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ArrayFromVector(const std::vector<bool>& is_valid, const std::vector<C_TYPE>& values,
                     std::shared_ptr<Array>* out) {
  auto type = TypeTraits<TYPE>::type_singleton();
  ArrayFromVector<TYPE, C_TYPE>(type, is_valid, values, out);
}

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ArrayFromVector(const std::vector<C_TYPE>& values, std::shared_ptr<Array>* out) {
  auto type = TypeTraits<TYPE>::type_singleton();
  ArrayFromVector<TYPE, C_TYPE>(type, values, out);
}

// ChunkedArrayFromVector: construct a ChunkedArray from vectors of C values

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ChunkedArrayFromVector(const std::shared_ptr<DataType>& type,
                            const std::vector<std::vector<bool>>& is_valid,
                            const std::vector<std::vector<C_TYPE>>& values,
                            std::shared_ptr<ChunkedArray>* out) {
  ArrayVector chunks;
  ASSERT_EQ(is_valid.size(), values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    std::shared_ptr<Array> array;
    ArrayFromVector<TYPE, C_TYPE>(type, is_valid[i], values[i], &array);
    chunks.push_back(array);
  }
  *out = std::make_shared<ChunkedArray>(chunks);
}

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ChunkedArrayFromVector(const std::shared_ptr<DataType>& type,
                            const std::vector<std::vector<C_TYPE>>& values,
                            std::shared_ptr<ChunkedArray>* out) {
  ArrayVector chunks;
  for (size_t i = 0; i < values.size(); ++i) {
    std::shared_ptr<Array> array;
    ArrayFromVector<TYPE, C_TYPE>(type, values[i], &array);
    chunks.push_back(array);
  }
  *out = std::make_shared<ChunkedArray>(chunks);
}

// Overloads without a DataType argument, for parameterless types

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ChunkedArrayFromVector(const std::vector<std::vector<bool>>& is_valid,
                            const std::vector<std::vector<C_TYPE>>& values,
                            std::shared_ptr<ChunkedArray>* out) {
  auto type = TypeTraits<TYPE>::type_singleton();
  ChunkedArrayFromVector<TYPE, C_TYPE>(type, is_valid, values, out);
}

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ChunkedArrayFromVector(const std::vector<std::vector<C_TYPE>>& values,
                            std::shared_ptr<ChunkedArray>* out) {
  auto type = TypeTraits<TYPE>::type_singleton();
  ChunkedArrayFromVector<TYPE, C_TYPE>(type, values, out);
}

template <typename T>
static inline Status GetBitmapFromVector(const std::vector<T>& is_valid,
                                         std::shared_ptr<Buffer>* result) {
  size_t length = is_valid.size();

  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(AllocateEmptyBitmap(length, &buffer));

  uint8_t* bitmap = buffer->mutable_data();
  for (size_t i = 0; i < static_cast<size_t>(length); ++i) {
    if (is_valid[i]) {
      BitUtil::SetBit(bitmap, i);
    }
  }

  *result = buffer;
  return Status::OK();
}

template <typename T>
inline void BitmapFromVector(const std::vector<T>& is_valid,
                             std::shared_ptr<Buffer>* out) {
  ASSERT_OK(GetBitmapFromVector(is_valid, out));
}

template <typename T>
void AssertSortedEquals(std::vector<T> u, std::vector<T> v) {
  std::sort(u.begin(), u.end());
  std::sort(v.begin(), v.end());
  ASSERT_EQ(u, v);
}

}  // namespace arrow
