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
#include <string>

#include "db/metax/MetaTraits.h"
#include "db/metax/backend/convertor/MetaBaseConvertor.h"

namespace milvus::engine::metax {

template <typename T>
inline std::string
value2sqlstring(const T& t, MetaConvertorPtr convertor) {
    if constexpr (decay_equal_v<T, int64_t>) {
        return convertor->int2str(t);
    } else if constexpr (decay_equal_v<T, uint64_t>) {
        return convertor->uint2str(t);
    } else if constexpr (decay_equal_v<T, std::string>) {
        return convertor->str2str(t);
    } else if constexpr (decay_equal_v<T, json>) {
        return convertor->json2str(t);
    } else {
        throw std::runtime_error("Unknown value type");
    }
}

template <typename F>
inline void
convert2raw(const F& f, Raw& raw, MetaConvertorPtr convertor) {
    if (f.Filled()) {
        std::string out = value2sqlstring<typename F::VType>(f.Get(), convertor);
        raw.insert(std::make_pair(std::string(F::Name), out));
    }
}

}  // namespace milvus::engine::metax
