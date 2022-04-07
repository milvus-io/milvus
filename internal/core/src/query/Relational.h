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

#include "common/VectorTrait.h"

#include <functional>
#include <string>

namespace milvus::query {
template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, FundamentalTag, FundamentalTag) {
    return Op{}(t, u);
}

template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, FundamentalTag, StringTag) {
    PanicInfo("incompitible data type");
}

template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, StringTag, FundamentalTag) {
    PanicInfo("incompitible data type");
}

template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, StringTag, StringTag) {
    return Op{}(t, u);
}

template <typename Op>
struct Relational {
    template <typename T, typename U>
    bool
    operator()(const T& t, const U& u) const {
        return RelationalImpl<Op, T, U>(t, u, typename TagDispatchTrait<T>::Tag{}, typename TagDispatchTrait<U>::Tag{});
    }

    template <typename... T>
    bool
    operator()(const T&...) const {
        PanicInfo("incompatible operands");
    }
};

template <OpType op>
struct MatchOp {
    template <typename T, typename U>
    bool
    operator()(const T& t, const U& u) {
        return Match(t, u, op);
    }
};

}  // namespace milvus::query
