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
#include <vector>

#include "arrow/compute/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Status;

namespace compute {

/// \brief An operation is a node in a computation graph, taking input data
/// expression dependencies and emitting an output expression
class ARROW_EXPORT Operation : public std::enable_shared_from_this<Operation> {
 public:
  virtual ~Operation() = default;

  /// \brief Check input expression arguments and output the type of resulting
  /// expression that this operation produces. If the input arguments are
  /// invalid, error Status is returned
  /// \param[out] out the returned well-typed expression
  /// \return success or failure
  virtual Status ToExpr(ExprPtr* out) const = 0;

  /// \brief Return the input expressions used to instantiate the
  /// operation. The default implementation returns an empty vector
  /// \return a vector of expressions
  virtual std::vector<ExprPtr> input_args() const;
};

}  // namespace compute
}  // namespace arrow
