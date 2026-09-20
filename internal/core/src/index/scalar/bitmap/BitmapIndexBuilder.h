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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include <roaring/roaring.hh>

#include "common/Array.h"
#include "common/Types.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/scalar/bitmap/BitmapIndexReader.h"
#include "storage/artifact/Artifact.h"

// Bitmap builder. Retains the value-to-postings map needed to finalize the
// index; input batching does not make its accumulated state streaming-only.

namespace milvus::index {

struct BitmapBuildParams {
    // Build over ARRAY elements rather than rows. Persisted.
    bool nested{false};
    bool nullable{false};
    DataType value_type{DataType::NONE};
};

template <typename T>
class BitmapIndexBuilder final : public IArtifactBuilder<ScalarBuildInput<T>> {
 public:
    explicit BitmapIndexBuilder(BitmapBuildParams params);

    ~BitmapIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<T>& input) &&
        override;

 private:
    using StoredT = owned_t<T>;

    BitmapBuildParams params_;
    BitmapRoaringPostingMap<StoredT> postings_;
    TargetBitmap validity_;
    size_t total_num_rows_{0};
};

// Ordinary ARRAY indexes consume one ArrayView per source row. This keeps the
// posting coordinate in the row domain: every element of one array points to
// the same row, while empty and null arrays still advance the row coordinate.
// Nested ARRAY indexes use the typed builders above after caller-side
// flattening, so their coordinates are consecutive element offsets.
class BitmapArrayIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<ArrayView>> {
 public:
    class Impl;

    explicit BitmapArrayIndexBuilder(BitmapBuildParams params);
    ~BitmapArrayIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<ArrayView>& input) &&
        override;

 private:
    BitmapBuildParams params_;
    std::unique_ptr<Impl> impl_;
    TargetBitmap validity_;
    size_t total_num_rows_{0};
};

}  // namespace milvus::index
