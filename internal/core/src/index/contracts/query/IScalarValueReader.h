// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include "common/Types.h"

// Reverse lookup, exposed independently of predicate support. ReaderCaps reports
// whether lookup is cheap; the consumer chooses between it and the raw column.
// This pure mixin does not inherit IIndexReaderBase.

namespace milvus::index {

// Owning result type: string_view becomes string; other types remain T.
template <typename T>
struct OwnedType {
    using type = T;
};

template <>
struct OwnedType<std::string_view> {
    using type = std::string;
};

template <typename T>
using owned_t = typename OwnedType<T>::type;

template <typename T>
class IScalarValueReader {
 public:
    virtual ~IScalarValueReader() = default;

    // The returned value owns its bytes. Compressed structures such as marisa
    // reconstruct values in a call-local agent, so a returned view would dangle.
    // Borrowed input types do not imply borrowed lookup results.
    virtual std::optional<owned_t<T>>
    Lookup(int64_t offset) const = 0;

    // Batch reverse lookup. Two reasons this is a callback rather than a
    // returned container: (1) the implementation may cluster the offsets by its
    // internal layout, and (2) unlike `Lookup`, a view IS fine here — the
    // implementation can keep its agent alive until the callback returns, so
    // `const T*` only has to be valid for the duration of the call. The output
    // is meant to plug into columnar-format's `TakeResult` convention.
    virtual void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t i, const T*, bool valid)>& out)
        const = 0;
};

}  // namespace milvus::index
