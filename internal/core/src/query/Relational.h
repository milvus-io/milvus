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

#include <functional>
#include <string>
#include <type_traits>

#include "common/Utils.h"
#include "common/VectorTrait.h"
#include "common/EasyAssert.h"
#include "query/Utils.h"

namespace milvus::query {
template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, FundamentalTag, FundamentalTag) {
    // UUID heterogenous guard: UUID only comparable with UUID. For any
    // cross-type where exactly one side is UUID, the comparison is
    // heterogeneous and must not instantiate Op{}(UUID, non-UUID) which has
    // no operator== etc. Return false for equality/ordering, true for
    // inequality, to keep boost::apply_visitor instantiation valid.
    if constexpr ((std::is_same_v<T, milvus::UUID> &&
                   !std::is_same_v<U, milvus::UUID>) ||
                  (!std::is_same_v<T, milvus::UUID> &&
                   std::is_same_v<U, milvus::UUID>)) {
        if constexpr (std::is_same_v<Op, std::equal_to<void>>) {
            return false;
        } else if constexpr (std::is_same_v<Op, std::not_equal_to<void>>) {
            return true;
        } else if constexpr (std::is_same_v<Op, std::greater<void>> ||
                             std::is_same_v<Op, std::greater_equal<void>> ||
                             std::is_same_v<Op, std::less<void>> ||
                             std::is_same_v<Op, std::less_equal<void>>) {
            return false;
        } else {
            // MatchOp and other non-ordering ops: incompatible with UUID cross-type
            return false;
        }
    } else {
        return Op{}(t, u);
    }
}

template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, FundamentalTag, StringTag) {
    ThrowInfo(DataTypeInvalid, "incompitible data type");
}

template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, StringTag, FundamentalTag) {
    ThrowInfo(DataTypeInvalid, "incompitible data type");
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
        return RelationalImpl<Op, T, U>(t,
                                        u,
                                        typename TagDispatchTrait<T>::Tag{},
                                        typename TagDispatchTrait<U>::Tag{});
    }

    template <typename... T>
    bool
    operator()(const T&...) const {
        ThrowInfo(OpTypeInvalid, "incompatible operands");
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
