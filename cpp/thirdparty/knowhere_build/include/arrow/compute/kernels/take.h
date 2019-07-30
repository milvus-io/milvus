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
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

namespace compute {

class FunctionContext;

struct ARROW_EXPORT TakeOptions {};

/// \brief Take from an array of values at indices in another array
///
/// The output array will be of the same type as the input values
/// array, with elements taken from the values array at the given
/// indices. If an index is null then the taken element will be null.
///
/// For example given values = ["a", "b", "c", null, "e", "f"] and
/// indices = [2, 1, null, 3], the output will be
/// = [values[2], values[1], null, values[3]]
/// = ["c", "b", null, null]
///
/// \param[in] ctx the FunctionContext
/// \param[in] values array from which to take
/// \param[in] indices which values to take
/// \param[in] options options
/// \param[out] out resulting array
ARROW_EXPORT
Status Take(FunctionContext* ctx, const Array& values, const Array& indices,
            const TakeOptions& options, std::shared_ptr<Array>* out);

/// \brief Take from an array of values at indices in another array
///
/// \param[in] ctx the FunctionContext
/// \param[in] values datum from which to take
/// \param[in] indices which values to take
/// \param[in] options options
/// \param[out] out resulting datum
ARROW_EXPORT
Status Take(FunctionContext* ctx, const Datum& values, const Datum& indices,
            const TakeOptions& options, Datum* out);

/// \brief BinaryKernel implementing Take operation
class ARROW_EXPORT TakeKernel : public BinaryKernel {
 public:
  explicit TakeKernel(const std::shared_ptr<DataType>& type, TakeOptions options = {})
      : type_(type) {}

  /// \brief BinaryKernel interface
  ///
  /// delegates to subclasses via Take()
  Status Call(FunctionContext* ctx, const Datum& values, const Datum& indices,
              Datum* out) override;

  /// \brief output type of this kernel (identical to type of values taken)
  std::shared_ptr<DataType> out_type() const override { return type_; }

  /// \brief factory for TakeKernels
  ///
  /// \param[in] value_type constructed TakeKernel will support taking
  ///            values of this type
  /// \param[in] index_type constructed TakeKernel will support taking
  ///            with indices of this type
  /// \param[out] out created kernel
  static Status Make(const std::shared_ptr<DataType>& value_type,
                     const std::shared_ptr<DataType>& index_type,
                     std::unique_ptr<TakeKernel>* out);

  /// \brief single-array implementation
  virtual Status Take(FunctionContext* ctx, const Array& values, const Array& indices,
                      std::shared_ptr<Array>* out) = 0;

 protected:
  std::shared_ptr<DataType> type_;
};
}  // namespace compute
}  // namespace arrow
