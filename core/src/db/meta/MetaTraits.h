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
#include <type_traits>

namespace milvus::engine::meta {

template <typename T, typename U>
struct decay_equal : std::is_same<typename std::decay<T>::type, U>::type {};

template <typename T, typename U>
constexpr bool decay_equal_v = decay_equal<T, U>::value;

/**
 * Reomve const and reference property
 * @tparam T
 */
template <typename T>
struct remove_cr {
    typedef typename std::remove_const<std::remove_reference_t<T>>::type type;
};

template <typename T>
using remove_cr_t = typename remove_cr<T>::type;

template <typename Base, typename Derived>
struct is_decay_base_of : std::is_base_of<remove_cr_t<Base>, remove_cr_t<Derived>> {};

template <typename Base, typename Derived>
constexpr bool is_decay_base_of_v = is_decay_base_of<Base, Derived>::value;

template <typename T>
struct is_shared_ptr : std::false_type {};

template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};

template <typename T>
constexpr bool is_shared_ptr_v = is_shared_ptr<T>::value;

//////////////////////////////////////////////////////////
template <typename T>
struct is_unique_ptr : std::false_type {};

template <typename T>
struct is_unique_ptr<std::unique_ptr<T>> : std::true_type {};

template <typename T>
constexpr bool is_unique_ptr_v = is_unique_ptr<T>::value;

//////// remove shared_ptr
template <typename T>
struct remove_shared_ptr {
    using type = T;
};

template <typename T>
struct remove_shared_ptr<std::shared_ptr<T>> {
    using type = T;
};

template <typename T>
using remove_shared_ptr_t = typename remove_shared_ptr<T>::type;

//////// remove unique_ptr
template <typename T>
struct remove_unique_ptr {
    using type = T;
};

template <typename T>
struct remove_unique_ptr<std::unique_ptr<T>> {
    using type = T;
};

template <typename T>
using remove_unique_ptr_t = typename remove_unique_ptr<T>::type;

}  // namespace milvus::engine::meta
