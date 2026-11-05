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

#include "cachinglayer/Utils.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <variant>

#include <marisa.h>

#include "common/Types.h"
#include "index/contracts/IIndex.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"

// Trie-backed string predicates, patterns, reverse lookup, and null queries.
// The reader is row-level only: it has no nested-mode constructor input.

namespace milvus::cachinglayer {
template <typename T>
class CacheSlot;
}

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

// Owns the loader-created mappings and their local staging files. marisa owns
// the trie mapping itself; this object owns the separate row-id/CSR mappings
// and keeps every backing path alive until the reader is destroyed.
class MarisaMmapOwner final {
 public:
    MarisaMmapOwner(char* str_ids_data,
                    size_t str_ids_bytes,
                    char* csr_data,
                    size_t csr_bytes,
                    std::shared_ptr<storage::LocalDirectory> directory);

    ~MarisaMmapOwner();

    MarisaMmapOwner(const MarisaMmapOwner&) = delete;
    MarisaMmapOwner&
    operator=(const MarisaMmapOwner&) = delete;

    const int64_t*
    StrIds() const;

    const uint32_t*
    Csr() const;

 private:
    char* str_ids_data_{nullptr};
    size_t str_ids_bytes_{0};
    char* csr_data_{nullptr};
    size_t csr_bytes_{0};
    std::shared_ptr<storage::LocalDirectory> directory_;
};

// Immutable owner/view bundle shared by readers. Owner fields precede the trie
// so the trie is destroyed before mapped paths are removed by MarisaMmapOwner.
struct MarisaIndexStorage final {
    /** @brief Validate loaded trie and row/CSR views before publication. */
    void
    Validate() const;

    cachinglayer::ResourceUsage
    CellByteSize() const;

    std::shared_ptr<MarisaMmapOwner> mmap_owner;
    std::shared_ptr<const std::vector<int64_t>> str_ids_owner;
    std::shared_ptr<const std::vector<uint32_t>> csr_index_owner;
    std::shared_ptr<const std::vector<uint32_t>> csr_offsets_owner;
    std::shared_ptr<const marisa::Trie> trie;
    std::shared_ptr<const TargetBitmap> json_exists;
    // row -> trie key id. Heap-owned or a view over mapped bytes.
    const int64_t* str_ids{nullptr};
    size_t str_ids_size{0};
    // CSR: key id -> the rows holding it.
    const uint32_t* csr_index{nullptr};
    const uint32_t* csr_offsets{nullptr};
    size_t csr_num_keys{0};
    DataType value_type{DataType::VARCHAR};
    // Exact bytes owned through mmap-backed staging files.
    size_t file_backed_bytes{0};
};

/** @brief Owns one Marisa storage version shared by request readers. */
class MarisaIndex final : public IIndex,
                          public std::enable_shared_from_this<MarisaIndex> {
 public:
    explicit MarisaIndex(std::shared_ptr<const MarisaIndexStorage> storage);

    /** @brief Retain a cold storage slot; metadata and reader creation do no I/O.
     * @param count Known row count, or nullopt for legacy artifacts without it.
     * @note The slot must load the same immutable index version on every reload.
     */
    MarisaIndex(
        std::shared_ptr<cachinglayer::CacheSlot<MarisaIndexStorage>> slot,
        DataType value_type,
        std::optional<int64_t> count);

    IndexMetadata
    Metadata() const override;

    IIndexReaderBasePtr
    CreateReader(milvus::OpContext* op_ctx) const override;

    void
    CancelWarmup() const override;

 private:
    friend class MarisaIndexReader;
    friend class JsonPathIndex;
    struct CachedStorage {
        std::shared_ptr<cachinglayer::CacheSlot<MarisaIndexStorage>> slot;
        DataType value_type;
        std::optional<int64_t> count;
    };

    // Retains the exact cache cell until the query operation finishes.
    std::shared_ptr<const MarisaIndexStorage>
    PinStorage(milvus::OpContext* op_ctx) const;

    std::variant<std::shared_ptr<const MarisaIndexStorage>, CachedStorage>
        storage_;
};

class MarisaIndexReader final : public IIndexReaderBase,
                                public IScalarPredicateReader<std::string_view>,
                                public IScalarValueReader<std::string_view>,
                                public IPatternMatchReader,
                                public INullReader {
 public:
    explicit MarisaIndexReader(
        std::shared_ptr<const MarisaIndexStorage> storage);

    ~MarisaIndexReader() override;

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

    TargetBitmap
    In(size_t n, const std::string_view* values) const override;

    TargetBitmap
    NotIn(size_t n, const std::string_view* values) const override;

    TargetBitmap
    Range(const std::string_view& value, CompareOp op) const override;

    TargetBitmap
    Range(const std::string_view& lo,
          bool lo_inc,
          const std::string_view& hi,
          bool hi_inc) const override;

    // Return an owning string: marisa reconstructs a key inside a call-local agent,
    // so a view into that agent would dangle after Lookup returns.
    std::optional<std::string>
    Lookup(int64_t offset) const override;

    // Gather may supply views while its agent stays alive through the callback.
    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<
               void(int64_t i, const std::string_view*, bool valid)>& out)
        const override;

    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override;

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    friend class MarisaIndex;
    MarisaIndexReader(std::shared_ptr<const MarisaIndex> index,
                      milvus::OpContext* op_ctx);

    const std::shared_ptr<const MarisaIndexStorage>&
    GetStorage() const {
        std::call_once(storage_once_,
                       [this] { storage_ = index_->PinStorage(op_ctx_); });
        return storage_;
    }

    std::shared_ptr<const MarisaIndex> index_;
    milvus::OpContext* const op_ctx_;
    mutable std::once_flag storage_once_;
    mutable std::shared_ptr<const MarisaIndexStorage> storage_;
};

}  // namespace milvus::index
