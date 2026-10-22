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

#include <optional>

#include "common/Types.h"
#include "common/BitsetView.h"

// Null predicates, independent of point/range support. Scalar predicate and
// candidate families provide this mixin even when they do not implement
// IScalarPredicateReader<T>; RTree is one example. There is no ReaderCaps bit
// for it. A standalone text-match artifact does not imply a null reader.

namespace milvus::index {

class INullReader {
 public:
    virtual ~INullReader() = default;

    // 1 = hit. Bitmap size is Count(), in the reader's own coordinate domain.
    virtual TargetBitmap
    IsNull() const = 0;

    virtual TargetBitmap
    IsNotNull() const = 0;

    // Borrowed dense validity in this reader's coordinate domain. nullopt means
    // unsupported; an engaged zero-size view means all Count() coordinates are
    // valid. Otherwise size() equals Count(), with no ID mapping. The view must
    // never outlive the pinned reader and its storage must remain immutable.
    // Mutable/path-specific readers keep the owned IsNotNull() fallback.
    virtual std::optional<BitsetView>
    GetValidityView() const {
        return std::nullopt;
    }
};

}  // namespace milvus::index
