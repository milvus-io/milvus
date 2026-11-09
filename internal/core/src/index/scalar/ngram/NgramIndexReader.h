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
#include <mutex>
#include <string>
#include <string_view>
#include <vector>
#include <variant>
#include "cachinglayer/CacheSlot.h"

#include "common/Types.h"
#include "index/contracts/IIndex.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/INullReader.h"

// INgramReader and INullReader over the ngram engine. Candidate results are
// generally a superset (caps.exact = false); a complete gram is exact for
// InnerMatch. The execution layer refines the other cases; this reader must not
// call the executor or expose a Phase2 callback.

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

/** @brief Owns the Tantivy engine, validity and optional backing directory. */
struct NgramIndexStorage {
    NgramIndexStorage(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        DataType value_type,
        uintptr_t min_gram,
        uintptr_t max_gram,
        size_t avg_row_size,
        bool mmap,
        size_t engine_bytes);
    int64_t
    MemoryUsage() const;
    cachinglayer::ResourceUsage
    CellByteSize() const;

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
    std::shared_ptr<const TargetBitmap> json_exists;
    DataType value_type_{DataType::VARCHAR};

    uintptr_t min_gram_{0};
    uintptr_t max_gram_{0};

    // Persisted as `ngram_avg_row_size` and used only by the cost policy above.
    size_t avg_row_size_{0};

    bool mmap_{false};
    // Exact retained staged bytes on mmap, or the managed-file payload copied
    // into Tantivy's RamDirectory. Rust reader/allocator overhead is not
    // exposed by the binding and is intentionally not replaced by an estimate.
    size_t engine_bytes_{0};
    uint32_t count_{0};
};

class NgramIndexReader;

/** @brief Shares one prepared storage version across request readers. */
class NgramIndex final : public IIndex,
                         public std::enable_shared_from_this<NgramIndex> {
 public:
    NgramIndex(std::shared_ptr<storage::LocalDirectory> directory,
               std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
               std::shared_ptr<const std::vector<size_t>> null_offsets,
               DataType value_type,
               uintptr_t min_gram,
               uintptr_t max_gram,
               size_t avg_row_size,
               bool mmap,
               size_t engine_bytes);
    explicit NgramIndex(std::shared_ptr<const NgramIndexStorage> storage);
    NgramIndex(std::shared_ptr<cachinglayer::CacheSlot<NgramIndexStorage>> slot,
               IndexMetadata metadata,
               uintptr_t min_gram,
               uintptr_t max_gram);
    IndexMetadata
    Metadata() const override;
    IIndexReaderBasePtr
    CreateReader(milvus::OpContext* op_ctx) const override;
    void
    CancelWarmup() const override;

 private:
    friend class NgramIndexReader;
    friend class JsonPathIndex;
    std::shared_ptr<const NgramIndexStorage>
    PinStorage(milvus::OpContext* op_ctx) const;
    std::pair<uintptr_t, uintptr_t>
    GramBounds() const;
    struct CachedStorage {
        std::shared_ptr<cachinglayer::CacheSlot<NgramIndexStorage>> slot;
        IndexMetadata metadata;
        uintptr_t min_gram;
        uintptr_t max_gram;
    };
    std::variant<std::shared_ptr<const NgramIndexStorage>, CachedStorage>
        storage_;
};

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

    void
    Prefetch() const override {
        (void)GetStorage();
    }

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    // ---- INgramReader --------------------------------------------

    // Per-literal eligibility, including min_gram and extractable regex fragments.
    // Ask after pinning; a load-time capability bit cannot encode this answer.
    bool
    CanHandle(std::string_view literal, PatternOp op) const override;

    bool
    CandidatesAreExact(std::string_view literal, PatternOp op) const override;

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
    friend class NgramIndex;
    NgramIndexReader(std::shared_ptr<const NgramIndex> index,
                     milvus::OpContext* op_ctx);

    // Cost policy: for a very selective pre-filter, tokenize and intersect
    // iteratively instead of running the full ngram match query.
    bool
    ShouldUseBatchStrategy(const NgramIndexStorage& storage,
                           double pre_filter_hit_rate) const;

    // AND-merges every literal's candidates using the cost policy above.
    void
    ApplyLiterals(const NgramIndexStorage& storage,
                  const std::vector<std::string>& literals,
                  TargetBitmap& candidates) const;

    void
    ApplyIterativeNgramFilter(const NgramIndexStorage& storage,
                              const std::vector<std::string>& sorted_terms,
                              size_t total_count,
                              TargetBitmap& bitset) const;

    std::shared_ptr<const NgramIndex> index_;
    milvus::OpContext* const op_ctx_;
    mutable std::once_flag storage_once_;
    mutable std::shared_ptr<const NgramIndexStorage> storage_;

    const std::shared_ptr<const NgramIndexStorage>&
    GetStorage() const;
};

}  // namespace milvus::index
