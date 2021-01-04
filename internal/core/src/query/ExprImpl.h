// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#include "Expr.h"
#include <tuple>
#include <vector>
#include <boost/container/vector.hpp>

namespace milvus::query {
template <typename T>
struct TermExprImpl : TermExpr {
    boost::container::vector<T> terms_;
};

template <typename T>
struct RangeExprImpl : RangeExpr {
    std::vector<std::tuple<OpType, T>> conditions_;
};

}  // namespace milvus::query
