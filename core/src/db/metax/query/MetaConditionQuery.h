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

#include <set>
#include "db/metax/MetaTraits.h"

namespace milvus::engine::metax {

enum RangeType {
    eq_,    /*!< Equal */
    neq_,   /*!< Not equal */
    gt_,    /*!< Greater than */
    gte_,   /*!< Greater than or equal */
    lt_,    /*!< Less than */
    lte_    /*!< Less than or equal */
};

template <typename L, typename R>
struct Range {
    using LType = remove_cr_t<L>;
    using RType = remove_cr_t<R>;

    L l_cond_;
    R r_cond_;
    RangeType range_type_;
};

template <typename L, typename R>
Range<L, R>
Range_(L l, R r, RangeType rt) {
    return {std::move(l), std::move(r), rt};
}

template <typename L, typename R>
inline Range<L, R>
operator==(L l, R r) {
    return {std::move(l), std::move(r), eq_};
}

template <typename L, typename R>
inline Range<L, R>
operator!=(L l, R r) {
    return {std::move(l), std::move(r), neq_};
}

template <typename L, typename R>
inline Range<L, R>
operator>(L l, R r) {
    return {std::move(l), std::move(r), gt_};
}

template <typename L, typename R>
inline Range<L, R>
operator>=(L l, R r) {
    return {std::move(l), std::move(r), gte_};
}

template <typename L, typename R>
inline Range<L, R>
operator<(L l, R r) {
    return {std::move(l), std::move(r), lt_};
}

template <typename L, typename R>
inline Range<L, R>
operator<=(L l, R r) {
    return {std::move(l), std::move(r), lte_};
}

//////////////////////////
template <template<typename...> typename H, typename V, typename C>
struct In {
    using CType = remove_cr_t<C>;
    using VType = remove_cr_t<V>;
    using HVType = remove_cr_t<H<V>>;

    C col_;
    H<V> value_;
};

template <typename C, typename V>
In<std::set, V, C>
In_(C c, std::set<V> ins) {
    return {std::move(c), std::move(ins)};
}

template <typename C, typename V>
In<std::vector, V, C>
In_(C c, std::vector<V> ins) {
    return {std::move(c), std::move(ins)};
}

template <typename C, typename V>
In<std::initializer_list, V, C>
In_(C c, std::initializer_list<V> ins) {
    return {std::move(c), std::move(ins)};
}

}  // namespace milvus::engine::metax
