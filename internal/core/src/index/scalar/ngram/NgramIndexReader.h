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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/INullReader.h"

// INgramReader and INullReader over the ngram engine. Candidate results are a
// superset (caps.exact = false), not exact pattern matches. The execution layer
// fetches original values and verifies candidates; this reader must not call
// the executor or expose a Phase2 callback.

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

class NgramIndexReader final : public IIndexReaderBase,
                               public INgramReader,
                               public INullReader {
 public:
    NgramIndexReader(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        DataType value_type,
        uintptr_t min_gram,
        uintptr_t max_gram,
        size_t avg_row_size,
        bool mmap,
        size_t engine_bytes);

    ~NgramIndexReader() override;

    // ---- IIndexReaderBase ----------------------------------------

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    cachinglayer::ResourceUsage
    CellByteSize() const override;

    // ---- INgramReader --------------------------------------------

    // Per-literal eligibility, including min_gram and extractable regex fragments.
    // Ask after pinning; a load-time capability bit cannot encode this answer.
    bool
    CanHandle(std::string_view literal, PatternOp op) const override;

    // Phase 1 only. AND-merged into `candidates`, which the caller has already
    // sized and initialized.
    void
    Candidates(std::string_view literal,
               PatternOp op,
               TargetBitmap& candidates) const override;

    // ---- INullReader -----------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    // Cost policy: for a very selective pre-filter, tokenize and intersect
    // iteratively instead of running the full ngram match query.
    bool
    ShouldUseBatchStrategy(double pre_filter_hit_rate) const;

    void
    ApplyIterativeNgramFilter(const std::vector<std::string>& sorted_terms,
                              size_t total_count,
                              TargetBitmap& bitset) const;

    // Mmap readers keep the unique materialized directory alive. Heap readers
    // own a Tantivy RamDirectory copy and therefore leave this null.
    std::shared_ptr<storage::LocalDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    // Validity materialized once at open instead of replaying the null-offset
    // vector on every IsNull/IsNotNull call, which sit on the query hot path.
    // The offset vector is not retained: it would duplicate the same
    // information at 8 bytes per null row. `all_valid_` leaves the bitmap empty
    // rather than allocating rows/8 bytes of all-ones for a non-nullable field
    // or a field with no nulls.
    bool all_valid_{false};
    TargetBitmap valid_bitmap_;
    DataType value_type_{DataType::VARCHAR};

    uintptr_t min_gram_{0};
    uintptr_t max_gram_{0};

    // Persisted alongside the index (`NGRAM_AVG_ROW_SIZE_FILE_NAME`,
    // NgramInvertedIndex.cpp:55) and used only by the cost policy above.
    size_t avg_row_size_{0};

    bool mmap_{false};
    // Exact retained staged bytes on mmap, or the managed-file payload copied
    // into Tantivy's RamDirectory. Rust reader/allocator overhead is not
    // exposed by the binding and is intentionally not replaced by an estimate.
    size_t engine_bytes_{0};
    uint32_t count_{0};
};

}  // namespace milvus::index
