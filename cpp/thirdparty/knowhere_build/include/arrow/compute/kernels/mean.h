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
#include <type_traits>

#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;

namespace compute {

struct Datum;
class FunctionContext;
class AggregateFunction;

ARROW_EXPORT
std::shared_ptr<AggregateFunction> MakeMeanAggregateFunction(const DataType& type,
                                                             FunctionContext* context);

/// \brief Compute the mean of a numeric array.
///
/// \param[in] context the FunctionContext
/// \param[in] value datum to compute the mean, expecting Array
/// \param[out] mean datum of the computed mean as a DoubleScalar
///
/// \since 0.13.0
/// \note API not yet finalized
ARROW_EXPORT
Status Mean(FunctionContext* context, const Datum& value, Datum* mean);

/// \brief Compute the mean of a numeric array.
///
/// \param[in] context the FunctionContext
/// \param[in] array to compute the mean
/// \param[out] mean datum of the computed mean as a DoubleScalar
///
/// \since 0.13.0
/// \note API not yet finalized
ARROW_EXPORT
Status Mean(FunctionContext* context, const Array& array, Datum* mean);

}  // namespace compute
};  // namespace arrow
