// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <tuple>

namespace milvus::engine::metax {

enum JoinType {
    from_,
    inner_join_,
    left_join_,
    right_join_,
    full_join_
};

template <typename... Args>
struct From {
    using tuple_type = std::tuple<Args...>;
    static const constexpr JoinType jvalue = from_;

    tuple_type args_;
};

template <typename... Args>
const constexpr JoinType From<Args...>::jvalue;

template <typename... Args>
inline From<Args...>
From_(Args... args) {
    static_assert(sizeof...(args) > 0, "From clause must contain at least one table");
    return {std::make_tuple(std::forward<Args>(args)...)};
}

////////////////////
template <typename... Args>
struct InnerJoin {
    using tuple_type = std::tuple<Args...>;
    static const constexpr JoinType jvalue = inner_join_;

    tuple_type args_;
};

template <typename... Args>
const constexpr JoinType InnerJoin<Args...>::jvalue;

template <typename... Args>
inline InnerJoin<Args...>
InnerJoin_(Args... args) {
    // TODO(yhz): Currently only support join with two tables
    static_assert(sizeof...(args) == 2, "Inner Join clause must and only contain two table");
    return {std::make_tuple(std::forward<Args>(args)...)};
}

template<typename... Args>
const constexpr decltype(&InnerJoin_<Args...>) Join_ = InnerJoin_<Args...>;

////////////////////////////
template <typename... Args>
struct LeftJoin {
    using tuple_type = std::tuple<Args...>;
    static const constexpr JoinType jvalue = left_join_;

    tuple_type args_;
};
template <typename... Args>
const constexpr JoinType LeftJoin<Args...>::jvalue;

template <typename... Args>
inline LeftJoin<Args...>
LeftJoin_(Args... args) {
    // TODO(yhz): Currently only support join with two tables
    static_assert(sizeof...(args) == 2, "Left Join clause must and only contain two table");
    return {std::make_tuple(std::forward<Args>(args)...)};
}

template <typename... Args>
struct RightJoin {
    using tuple_type = std::tuple<Args...>;
    static const constexpr JoinType jvalue = right_join_;

    tuple_type args_;
};
template <typename... Args>
const constexpr JoinType RightJoin<Args...>::jvalue;

template <typename... Args>
inline RightJoin<Args...>
RightJoin_(Args... args) {
    // TODO(yhz): Currently only support join with two tables
    static_assert(sizeof...(args) == 2, "Left Join clause must and only contain two table");
    return {std::make_tuple(std::forward<Args>(args)...)};
}

template <typename... Args>
struct FullJoin {
    using tuple_type = std::tuple<Args...>;
    static const constexpr JoinType jvalue = full_join_;

    tuple_type args_;
};
template <typename... Args>
const constexpr JoinType FullJoin<Args...>::jvalue;

template <typename... Args>
inline FullJoin<Args...>
FullJoin_(Args... args) {
    // TODO(yhz): Currently only support join with two tables
    static_assert(sizeof...(args) == 2, "Full Join clause must and only contain two table");
    return {std::make_tuple(std::forward<Args>(args)...)};
}

}  // namespace milvus::engine::metax