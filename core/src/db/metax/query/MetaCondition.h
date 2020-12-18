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
#include <type_traits>

namespace milvus::engine::metax {

////////////// One
template <typename Cond>
struct One {
    using cond = remove_cr_t<Cond>;

    cond cond_;
};

template <typename Cond>
struct is_cond_one : std::false_type {};

template <typename Cond>
struct is_cond_one<One<Cond>> : std::true_type {};

template <typename Cond>
One<Cond>
One_(Cond cond) {
    return {std::move(cond)};
}

/////////////// And
template <typename...Args>
struct And {
    using cond_tuple = std::tuple<Args...>;

    cond_tuple conds_;
};

template <typename... Args>
struct is_cond_and : std::false_type {};

template <typename... Args>
struct is_cond_and<And<Args...>> : std::true_type {};

template <typename...Args>
And<Args...>
And_(Args&&... args) {
    return {std::make_tuple(std::forward<Args>(args)...)};
}

//////////////// Or
template <typename...Args>
struct Or {
    using cond_tuple = std::tuple<Args...>;

    cond_tuple conds_;
};

template <typename...Args>
struct is_cond_or : std::false_type {};

template <typename...Args>
struct is_cond_or<Or<Args...>> : std::true_type {};

template <typename...Args>
Or<Args...>
Or_(Args&&... args) {
    return {std::make_tuple(std::forward<Args>(args)...)};
}

template <typename... Args>
struct Where {
    using condition_type = std::tuple<Args...>;

    condition_type condition_;
};

template <typename... Args>
struct is_cond_clause_where : std::false_type {};

template <typename... Args>
struct is_cond_clause_where<Where<Args...>> : std::true_type {};

template <typename... Args>
constexpr bool is_cond_clause_where_v = is_cond_clause_where<Args...>::value;

template <typename... Args>
inline Where<Args...>
Where_(Args... args) {
    return {std::make_tuple(std::forward<Args>(args)...)};
}

template <typename... Args>
struct On {
    using condition_type = std::tuple<Args...>;

    condition_type condition_;
};

template <typename... Args>
inline On<Args...>
On_(Args... args) {
    return {std::make_tuple(std::forward<Args>(args)...)};
}

template <typename... Args>
struct is_cond_clause_on : std::false_type {};

template <typename... Args>
struct is_cond_clause_on<On<Args...>> : std::true_type {};

template <typename... Args>
constexpr bool is_cond_clause_on_v = is_cond_clause_on<Args...>::value;

//template <typename... Args>
//struct is_cond_clause : std::false_type {};

template <typename... Args>
struct is_cond_clause : std::false_type {};

template <typename... Args>
struct is_cond_clause<On<Args...>> : std::true_type {};

template <typename... Args>
struct is_cond_clause<Where<Args...>> : std::true_type {};

template <typename... Args>
constexpr bool is_cond_clause_v = is_cond_clause<Args...>::value;
}  // namespace milvus::engine::metax
