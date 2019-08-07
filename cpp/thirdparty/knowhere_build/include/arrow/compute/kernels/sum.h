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

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;
class Status;

namespace compute {

struct Datum;
class FunctionContext;
class AggregateFunction;

/// \brief Return a Sum Kernel
///
/// \param[in] type required to specialize the kernel
/// \param[in] context the FunctionContext
///
/// \since 0.13.0
/// \note API not yet finalized
ARROW_EXPORT
std::shared_ptr<AggregateFunction> MakeSumAggregateFunction(const DataType& type,
                                                            FunctionContext* context);

/// \brief Sum values of a numeric array.
///
/// \param[in] context the FunctionContext
/// \param[in] value datum to sum, expecting Array or ChunkedArray
/// \param[out] out resulting datum
///
/// \since 0.13.0
/// \note API not yet finalized
ARROW_EXPORT
Status Sum(FunctionContext* context, const Datum& value, Datum* out);

/// \brief Sum values of a numeric array.
///
/// \param[in] context the FunctionContext
/// \param[in] array to sum
/// \param[out] out resulting datum
///
/// \since 0.13.0
/// \note API not yet finalized
ARROW_EXPORT
Status Sum(FunctionContext* context, const Array& array, Datum* out);

}  // namespace compute
}  // namespace arrow
