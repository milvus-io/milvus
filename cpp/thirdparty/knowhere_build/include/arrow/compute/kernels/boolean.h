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

#ifndef ARROW_COMPUTE_KERNELS_BOOLEAN_H
#define ARROW_COMPUTE_KERNELS_BOOLEAN_H

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

struct Datum;
class FunctionContext;

/// \brief Invert the values of a boolean datum
/// \param[in] context the FunctionContext
/// \param[in] value datum to invert
/// \param[out] out resulting datum
///
/// \since 0.11.0
/// \note API not yet finalized
ARROW_EXPORT
Status Invert(FunctionContext* context, const Datum& value, Datum* out);

/// \brief Element-wise AND of two boolean datums
/// \param[in] context the FunctionContext
/// \param[in] left left operand (array)
/// \param[in] right right operand (array)
/// \param[out] out resulting datum
///
/// \since 0.11.0
/// \note API not yet finalized
ARROW_EXPORT
Status And(FunctionContext* context, const Datum& left, const Datum& right, Datum* out);

/// \brief Element-wise OR of two boolean datums
/// \param[in] context the FunctionContext
/// \param[in] left left operand (array)
/// \param[in] right right operand (array)
/// \param[out] out resulting datum
///
/// \since 0.11.0
/// \note API not yet finalized
ARROW_EXPORT
Status Or(FunctionContext* context, const Datum& left, const Datum& right, Datum* out);

/// \brief Element-wise XOR of two boolean datums
/// \param[in] context the FunctionContext
/// \param[in] left left operand (array)
/// \param[in] right right operand (array)
/// \param[out] out resulting datum
///
/// \since 0.11.0
/// \note API not yet finalized
ARROW_EXPORT
Status Xor(FunctionContext* context, const Datum& left, const Datum& right, Datum* out);

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNELS_CAST_H
