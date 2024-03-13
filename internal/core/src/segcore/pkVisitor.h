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
#include <string>
#include "common/EasyAssert.h"

namespace milvus::segcore {

struct Int64PKVisitor {
    template <typename T>
    int64_t
    operator()(T t) const {
        PanicInfo(Unsupported, "invalid int64 pk value");
    }
};

template <>
inline int64_t
Int64PKVisitor::operator()<int64_t>(int64_t t) const {
    return t;
}

struct StrPKVisitor {
    template <typename T>
    std::string
    operator()(T t) const {
        PanicInfo(Unsupported, "invalid string pk value");
    }
};

template <>
inline std::string
StrPKVisitor::operator()<std::string>(std::string t) const {
    return t;
}

}  // namespace milvus::segcore
