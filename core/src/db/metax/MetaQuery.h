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
template <typename R, typename F>
struct Column {
    using RType = remove_cr_t<R>;
    using FType = remove_cr_t<F>;
    using VType = remove_cr_t<typename F::ValueType>;

    static const constexpr char* ResName = R::Name;
    static const constexpr char* FieldName = F::Name;

    VType value_;
};

template<typename R, typename F>
const constexpr char* Column<R, F>::ResName;

template<typename R, typename F>
const constexpr char* Column<R, F>::FieldName;

template <typename R, typename F>
Column<R, F>
Col_() {
    return {typename Column<R, F>::VType{}};
}

template <typename R, typename F>
Column<R, F>
Col_(typename Column<R, F>::VType v) {
    return {v};
}

//template <typename T>
//struct is_column : std::false_type {};

//template <template<typename, typename> typename H, typename R, typename F>
//struct is_column : std::integral_constant<bool,
//                                          decay_equal_v<H<R, F>,
//                                                        Column<R, F>
//                                                        >
//                                          > {};

///////// Columns
template <typename ... Cols>
struct Columns {
    using type = std::tuple<Cols...>;
//    using template_type = Columns;
    static constexpr size_t size = std::tuple_size<std::tuple<Cols...>>::value;

    type cols_;
};

template<typename...Cols>

constexpr size_t Columns<Cols...>::size;

//template <typename T>
//struct is_columns : std::false_type {};

//template <template<typename...> typename H, typename... Args>
//struct is_columns : std::integral_constant<bool, decay_equal_v<H<Args...>, Columns<Args...>>> {};

template <typename ... Args>
Columns<Args...>
Cols_(Args...cols) {
    return {std::make_tuple(std::forward<Args>(cols)...)};
}

template <typename R>
struct Table;

template <typename T>
Table<T>
AllCols_(Table<T> t) {
    return {std::move(t)};
}

/////////////// Columns helper
//template<class T, template<class> class C>
//struct is_clumns;
//template<template<class> class C>
//struct is_columns<Columns<>, C> {;

template<class T, template<class> class C>
struct columns_helper;

//template<template<class> class C>
//struct columns_helper<Columns<>, C> {
//    using element_types = std::tuple<>;
//    static constexpr const int size = 0;
//};

template<class H, class... Args, template<class> class C>
struct columns_helper<Columns<H, Args...>, C> {
    static constexpr const int value = C<H>::value + columns_helper<std::tuple<Args...>, C>::size;
};

/////////////// Table
template <typename R>
struct Table {
    using type = remove_cr_t<R>;
    static const constexpr char* name = R::Name;
};

template <typename R>
const constexpr char* Table<R>::name;

template <typename R>
Table<R>
Tab_() {
    return {};
}

/////////////// Tables
template <typename ... Tabs>
struct Tables {
    using type = std::tuple<Tabs...>;
//    using template_type = Columns;
    static constexpr size_t size = std::tuple_size<std::tuple<Tabs...>>::value;

    type tabs_;
};

template<typename...Tabs>
constexpr size_t Columns<Tabs...>::size;

//////////////

/**
 * select(Cols(Col<Collection, IdField>(), Col<Collection, NameField>()),
 *        From<Collection, Partitions>(),
 *        Where(And_(In_<Collection, IdField>({0, 1, 2}), Range_()))
 *        )
 */


}  // namespace milvus::engine::metax
