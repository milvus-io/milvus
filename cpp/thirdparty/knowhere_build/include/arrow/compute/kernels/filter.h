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

/// \brief Filter an array with a boolean selection filter
///
/// The output array will be populated with values from the input at positions
/// where the selection filter is not 0. Nulls in the filter will result in nulls
/// in the output.
///
/// For example given values = ["a", "b", "c", null, "e", "f"] and
/// filter = [0, 1, 1, 0, null, 1], the output will be
/// = ["b", "c", null, "f"]
///
/// \param[in] ctx the FunctionContext
/// \param[in] values array to filter
/// \param[in] filter indicates which values should be filtered out
/// \param[out] out resulting array
ARROW_EXPORT
Status Filter(FunctionContext* ctx, const Array& values, const Array& filter,
              std::shared_ptr<Array>* out);

/// \brief Filter an array with a boolean selection filter
///
/// \param[in] ctx the FunctionContext
/// \param[in] values datum to filter
/// \param[in] filter indicates which values should be filtered out
/// \param[out] out resulting datum
ARROW_EXPORT
Status Filter(FunctionContext* ctx, const Datum& values, const Datum& filter, Datum* out);

/// \brief BinaryKernel implementing Filter operation
class ARROW_EXPORT FilterKernel : public BinaryKernel {
 public:
  explicit FilterKernel(const std::shared_ptr<DataType>& type) : type_(type) {}

  /// \brief BinaryKernel interface
  ///
  /// delegates to subclasses via Filter()
  Status Call(FunctionContext* ctx, const Datum& values, const Datum& filter,
              Datum* out) override;

  /// \brief output type of this kernel (identical to type of values filtered)
  std::shared_ptr<DataType> out_type() const override { return type_; }

  /// \brief factory for FilterKernels
  ///
  /// \param[in] value_type constructed FilterKernel will support filtering
  ///            values of this type
  /// \param[out] out created kernel
  static Status Make(const std::shared_ptr<DataType>& value_type,
                     std::unique_ptr<FilterKernel>* out);

  /// \brief single-array implementation
  virtual Status Filter(FunctionContext* ctx, const Array& values,
                        const BooleanArray& filter, int64_t length,
                        std::shared_ptr<Array>* out) = 0;

 protected:
  std::shared_ptr<DataType> type_;
};

}  // namespace compute
}  // namespace arrow
