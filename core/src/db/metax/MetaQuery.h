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

namespace milvus::engine::metax {

template <typename R, typename F>
struct Column {
    using RType = remove_cr_t<R>;
    using FType = remove_cr_t<F>;
    using VType = remove_cr_t<typename F::ValueType>;

    VType value_;
};

template <typename ... Cols>
struct Columns {
    using type = std::tuple<Cols...>;
    static constexpr size_t size = std::tuple_size<std::tuple<Cols...>>::value;
};

template<typename...Cols>
constexpr size_t Columns<Cols...>::size;

}  // namespace milvus::engine::metax
