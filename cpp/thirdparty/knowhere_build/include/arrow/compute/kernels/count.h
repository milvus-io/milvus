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

/// \class CountOptions
///
/// The user control the Count kernel behavior with this class. By default, the
/// it will count all non-null values.
struct ARROW_EXPORT CountOptions {
  enum mode {
    // Count all non-null values.
    COUNT_ALL = 0,
    // Count all null values.
    COUNT_NULL,
  };

  explicit CountOptions(enum mode count_mode) : count_mode(count_mode) {}

  enum mode count_mode = COUNT_ALL;
};

/// \brief Return Count function aggregate
ARROW_EXPORT
std::shared_ptr<AggregateFunction> MakeCount(FunctionContext* context,
                                             const CountOptions& options);

/// \brief Count non-null (or null) values in an array.
///
/// \param[in] context the FunctionContext
/// \param[in] options counting options, see CountOptions for more information
/// \param[in] datum to count
/// \param[out] out resulting datum
///
/// \since 0.13.0
/// \note API not yet finalized
ARROW_EXPORT
Status Count(FunctionContext* context, const CountOptions& options, const Datum& datum,
             Datum* out);

/// \brief Count non-null (or null) values in an array.
///
/// \param[in] context the FunctionContext
/// \param[in] options counting options, see CountOptions for more information
/// \param[in] array to count
/// \param[out] out resulting datum
///
/// \since 0.13.0
/// \note API not yet finalized
ARROW_EXPORT
Status Count(FunctionContext* context, const CountOptions& options, const Array& array,
             Datum* out);

}  // namespace compute
}  // namespace arrow
