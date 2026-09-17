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

#include "common/EasyAssert.h"
#include "common/Types.h"

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

    // Validity projected into the CALLER's absolute row space, which may be
    // wider than Count().
    //
    // An index can legitimately cover fewer rows than the segment the caller
    // is filtering: a growing index publishes snapshots behind the insert
    // cursor, and legacy R-Tree files can carry null offsets past the indexed
    // row count because older builders dropped non-null empty/corrupt
    // geometries. A consumer that sized a row-addressed bitmap by its own
    // active row count cannot use the Count()-sized bitmap from IsNotNull()
    // directly.
    //
    // The default projects this reader's own bitmap: rows below Count() keep
    // their recorded validity, and rows at or beyond it are set to 1 --
    // "not indexed, so not decided here; the consumer must refine". Setting
    // them to 0 would silently drop live rows. Rows above row_count are
    // dropped. An implementation that persists absolute null offsets
    // independently of Count() may override this to project them directly.
    virtual TargetBitmap
    IsNotNull(int64_t row_count) const {
        AssertInfo(row_count >= 0,
                   "validity row count must be non-negative, got {}",
                   row_count);
        auto bitmap = IsNotNull();
        bitmap.resize(static_cast<size_t>(row_count), /*init=*/true);
        return bitmap;
    }
};

}  // namespace milvus::index
