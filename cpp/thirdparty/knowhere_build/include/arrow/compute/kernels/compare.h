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

#include <memory>

#include "arrow/compute/kernel.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;
struct Scalar;
class Status;

namespace compute {

struct Datum;
class FunctionContext;

/// CompareFunction is an interface for Comparisons
///
/// Comparisons take an array and emits a selection vector. The selection vector
/// is given in the form of a bitmask as a BooleanArray result.
class ARROW_EXPORT CompareFunction {
 public:
  /// Compare an array with a scalar argument.
  virtual Status Compare(const ArrayData& array, const Scalar& scalar,
                         ArrayData* output) const = 0;

  Status Compare(const ArrayData& array, const Scalar& scalar,
                 std::shared_ptr<ArrayData>* output) {
    return Compare(array, scalar, output->get());
  }

  virtual Status Compare(const Scalar& scalar, const ArrayData& array,
                         ArrayData* output) const = 0;

  Status Compare(const Scalar& scalar, const ArrayData& array,
                 std::shared_ptr<ArrayData>* output) {
    return Compare(scalar, array, output->get());
  }

  /// Compare an array with an array argument.
  virtual Status Compare(const ArrayData& lhs, const ArrayData& rhs,
                         ArrayData* output) const = 0;

  Status Compare(const ArrayData& lhs, const ArrayData& rhs,
                 std::shared_ptr<ArrayData>* output) {
    return Compare(lhs, rhs, output->get());
  }

  /// By default, CompareFunction emits a result bitmap.
  virtual std::shared_ptr<DataType> out_type() const { return boolean(); }

  virtual ~CompareFunction() {}
};

/// \brief BinaryKernel bound to a select function
class ARROW_EXPORT CompareBinaryKernel : public BinaryKernel {
 public:
  explicit CompareBinaryKernel(std::shared_ptr<CompareFunction>& select)
      : compare_function_(select) {}

  Status Call(FunctionContext* ctx, const Datum& left, const Datum& right,
              Datum* out) override;

  static int64_t out_length(const Datum& left, const Datum& right) {
    if (left.kind() == Datum::ARRAY) return left.length();
    if (right.kind() == Datum::ARRAY) return right.length();

    return 0;
  }

  std::shared_ptr<DataType> out_type() const override;

 private:
  std::shared_ptr<CompareFunction> compare_function_;
};

enum CompareOperator {
  EQUAL,
  NOT_EQUAL,
  GREATER,
  GREATER_EQUAL,
  LESS,
  LESS_EQUAL,
};

template <typename T, CompareOperator Op>
struct Comparator;

template <typename T>
struct Comparator<T, CompareOperator::EQUAL> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs == rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::NOT_EQUAL> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs != rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::GREATER> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs > rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::GREATER_EQUAL> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs >= rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::LESS> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs < rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::LESS_EQUAL> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs <= rhs; }
};

struct CompareOptions {
  explicit CompareOptions(CompareOperator op) : op(op) {}

  enum CompareOperator op;
};

/// \brief Return a Compare CompareFunction
///
/// \param[in] context FunctionContext passing context information
/// \param[in] type required to specialize the kernel
/// \param[in] options required to specify the compare operator
///
/// \since 0.14.0
/// \note API not yet finalized
ARROW_EXPORT
std::shared_ptr<CompareFunction> MakeCompareFunction(FunctionContext* context,
                                                     const DataType& type,
                                                     struct CompareOptions options);

/// \brief Compare a numeric array with a scalar.
///
/// \param[in] context the FunctionContext
/// \param[in] left datum to compare, must be an Array
/// \param[in] right datum to compare, must be a Scalar of the same type than
///            left Datum.
/// \param[in] options compare options
/// \param[out] out resulting datum
///
/// Note on floating point arrays, this uses ieee-754 compare semantics.
///
/// \since 0.14.0
/// \note API not yet finalized
ARROW_EXPORT
Status Compare(FunctionContext* context, const Datum& left, const Datum& right,
               struct CompareOptions options, Datum* out);

}  // namespace compute
}  // namespace arrow
