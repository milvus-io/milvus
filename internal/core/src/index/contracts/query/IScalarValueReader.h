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

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/Types.h"
#include "common/EasyAssert.h"
#include "common/ValidityView.h"

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
    using ViewsConsumer = std::function<void(
        int64_t first, const T* values, ValidityView validity, int64_t count)>;
    virtual ~IScalarValueReader() = default;

    // The returned value owns its bytes. Compressed structures such as marisa
    // reconstruct values in a call-local agent, so a returned view would dangle.
    // Borrowed input types do not imply borrowed lookup results.
    virtual std::optional<owned_t<T>>
    Lookup(int64_t offset) const = 0;

    // Batch reverse lookup over the same offset domain as Lookup. count has no
    // mask-width limit; empty input permits offsets == nullptr and performs no
    // callbacks. Each requested lane is reported exactly once, in ascending
    // input index i, even when offsets themselves are repeated or unordered.
    // A valid lane has a non-null value pointer; for an invalid lane the pointer
    // must not be dereferenced. Invalid arguments and reader/callback failures
    // propagate as errors, not as successful all-invalid results.
    //
    // The implementation may cluster physical reads by its layout, but must
    // restore input-lane order before invoking callbacks. This lets existing
    // batch consumers preserve their logical-position cursors without an
    // additional value-copying/reordering adapter. Values are lent
    // ONLY for the duration of the individual callback: both the pointer and,
    // for string_view, its character storage may be reused by the next lane
    // (e.g. a trie reader's call-local agent). Evaluate synchronously or copy a
    // value that must outlive that callback; retaining a view until Gather
    // returns is not safe in general. The caller keeps the reader and backing
    // alive for the entire call. The output plugs into the columnar-format
    // TakeResult convention without requiring per-offset owning Lookup copies.
    virtual void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t i, const T*, bool valid)>& out)
        const = 0;

    // Ordered bounded value batches. All views/backing remain valid through
    // ONE consumer call; an empty validity view means all lanes are valid.
    // Like Gather, input order/duplicates are preserved and count is unbounded.
    // Stable-storage readers override this to lend views without owning copies.
    // The compatibility adapter owns transient strings before the next Gather
    // callback can reuse its buffer (e.g. Marisa's local agent).
    virtual void
    GatherViews(const int64_t* offsets,
                int64_t count,
                const ViewsConsumer& consume) const {
        AssertInfo(count >= 0 && (count == 0 || offsets != nullptr),
                   "value batch received invalid offsets/count");
        constexpr int64_t kBatchRows = 64;
        std::array<T, kBatchRows> values;
        std::array<bool, kBatchRows> valid;
        std::array<std::string,
                   std::is_same_v<T, std::string_view> ? kBatchRows : 0>
            owners;
        for (int64_t first = 0; first < count;) {
            const auto size = std::min(kBatchRows, count - first);
            int64_t emitted = 0;
            bool all_valid = true;
            Gather(
                offsets + first,
                size,
                [&](int64_t lane, const T* value, bool known) {
                    AssertInfo(
                        lane == emitted++ && lane < size && (!known || value),
                        "value Gather returned an invalid ordered lane");
                    valid[lane] = known;
                    all_valid &= known;
                    if (!known) {
                        values[lane] = T{};
                    } else if constexpr (std::is_same_v<T, std::string_view>) {
                        owners[lane].assign(value->data(), value->size());
                        values[lane] = owners[lane];
                    } else {
                        values[lane] = *value;
                    }
                });
            AssertInfo(emitted == size, "value Gather omitted requested lanes");
            consume(first,
                    values.data(),
                    all_valid ? ValidityView{}
                              : ValidityView::FromExpanded(valid.data()),
                    size);
            first += size;
        }
    }
};

}  // namespace milvus::index
