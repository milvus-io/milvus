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

#include <memory>
#include <tuple>
#include <type_traits>

#include "db/metax/MetaTraits.h"
#include "db/metax/query/MetaCondition.h"
#include "db/metax/query/MetaConditionQuery.h"
#include "db/metax/query/MetaJoin.h"

namespace milvus::engine::metax {

///////// Column
template <typename...Args>
struct is_column : std::false_type {};

template <typename R, typename F>
struct is_column<Column<R, F>> : std::true_type {};

template <typename...Args>
constexpr bool is_column_v = is_column<Args...>::value;

///////// Columns
template <typename...Args>
struct is_columns : std::false_type {};

template <typename...Args>
struct is_columns<Columns<Args...>> : std::true_type {};

template <typename...Args>
constexpr bool is_columns_v = is_columns<Args...>::value;

///////////////// Table
template <typename Arg>
struct is_table : std::false_type {};

template <typename Arg>
struct is_table<Table<Arg>> : std::true_type {};

template <typename Arg>
constexpr bool is_table_v = is_table<Arg>::value;

/////////////// Tables
template <typename... Args>
struct is_tables : std::false_type {};

template <typename... Args>
struct is_tables<Tables<Args...>> : std::true_type {};

template <typename... Args>
constexpr bool is_tables_v = is_tables<Args...>::value;

/////////////// Conditions
//template <typename C,   /*!< Columns */
//          typename T,   /*!< From or Join */
//          typename W    /*!< Where */
//          >
//struct Select {
//    C cols_;
//    T tables_;
//    W where_;
//};
//
//template <typename C, typename T, typename W>
//Select<C, T, W>
//Select_(C c, T t, W w) {
//    return {std::move(c), std::move(t), std::move(w)};
//}

template <typename... Args>
struct is_join : std::false_type {};

template <typename... Args>
struct is_join<InnerJoin<Args...>> : std::true_type {};

template <typename... Args>
struct is_join<LeftJoin<Args...>> : std::true_type {};

template <typename... Args>
struct is_join<RightJoin<Args...>> : std::true_type {};

template <typename... Args>
struct is_join<FullJoin<Args...>> : std::true_type {};

template <typename... Args>
struct is_from : std::false_type {};

template <typename... Args>
struct is_from<From<Args...>> : std::true_type {};

//template <typename... Args>
//struct is_table_relation : std::false_type {};

template <typename... Args>
struct is_table_relation : is_join<Args...> {};

template <typename... Args>
struct is_table_relation<From<Args...>> : std::true_type {};

template <typename... Args>
constexpr bool is_table_relation_v = is_table_relation<Args...>::value;
}  // namespace milvus::engine::metax
