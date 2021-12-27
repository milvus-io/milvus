// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <tuple>
#include <vector>
#include <boost/container/vector.hpp>

#include "Expr.h"

namespace milvus::query {

template <typename T>
struct TermExprImpl : TermExpr {
    boost::container::vector<T> terms_;
};

template <typename T>
struct UnaryRangeExprImpl : UnaryRangeExpr {
    T value_;
};

template <typename T>
struct BinaryRangeExprImpl : BinaryRangeExpr {
    T lower_value_;
    T upper_value_;
};

}  // namespace milvus::query
