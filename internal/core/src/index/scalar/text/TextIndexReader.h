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
#include <cstddef>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>
#include <variant>
#include "cachinglayer/CacheSlot.h"

#include "common/Types.h"
#include "index/SharedValidityBitmap.h"
#include "index/contracts/IIndex.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ITextMatchReader.h"

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

// Text-match reader over a composed, stable tantivy snapshot. Construction and
// loading are separate from querying. Validity is frozen with the same
// snapshot so match results and SQL validity always share one row generation.

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

/** @brief Owns a Tantivy snapshot, validity and its backing directory. */
struct TextIndexStorage {
    TextIndexStorage(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        int64_t count,
        DataType value_type,
        bool file_backed,
        size_t payload_bytes);

    // Growing snapshots share complete words and own a private partial word.
    TextIndexStorage(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        SharedValidityBitmap::Snapshot validity,
        int64_t count,
        DataType value_type,
        bool file_backed,
        size_t payload_bytes);

    int64_t
    MemoryUsage() const;
    cachinglayer::ResourceUsage
    CellByteSize() const;

    // Destroy the engine before the optional mmap directory owner.
    std::shared_ptr<storage::LocalDirectory> directory_;

    // Composed immutable engine snapshot. Writer locks, commit timing, and append
    // state belong to the growing owner, not this query reader.
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;

    SharedValidityBitmap::Snapshot validity_;

    int64_t count_{0};
    DataType value_type_{DataType::NONE};
    bool file_backed_{false};
    size_t payload_bytes_{0};
};

class TextIndexReader;

/** @brief Shares a text storage version across request readers. */
class TextIndex final : public IIndex,
                        public std::enable_shared_from_this<TextIndex> {
 public:
    TextIndex(std::shared_ptr<storage::LocalDirectory> directory,
              std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
              std::shared_ptr<const std::vector<size_t>> null_offsets,
              int64_t count,
              DataType value_type,
              bool file_backed,
              size_t payload_bytes);

    // Growing snapshots share complete words and own a private partial word.
    TextIndex(std::shared_ptr<storage::LocalDirectory> directory,
              std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
              SharedValidityBitmap::Snapshot validity,
              int64_t count,
              DataType value_type,
              bool file_backed,
              size_t payload_bytes);

    explicit TextIndex(std::shared_ptr<const TextIndexStorage> storage);
    TextIndex(std::shared_ptr<cachinglayer::CacheSlot<TextIndexStorage>> slot,
              IndexMetadata metadata);

    IndexMetadata
    Metadata() const override;
    IIndexReaderBasePtr
    CreateReader(milvus::OpContext* op_ctx) const override;
    void
    CancelWarmup() const override;

 private:
    friend class TextIndexReader;
    std::shared_ptr<const TextIndexStorage>
    PinStorage(milvus::OpContext* op_ctx) const;
    struct CachedStorage {
        std::shared_ptr<cachinglayer::CacheSlot<TextIndexStorage>> slot;
        IndexMetadata metadata;
    };
    std::variant<std::shared_ptr<const TextIndexStorage>, CachedStorage>
        storage_;
};

class TextIndexReader final : public IIndexReaderBase,
                              public ITextMatchReader,
                              public INullReader {
 public:
    // Constructed by TextIndexLoader::Create or by consuming a completed
    // TextIndexArtifact. The constructor accepts ready state rather than
    // building or loading it.
    TextIndexReader(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        int64_t count,
        DataType value_type,
        bool file_backed,
        size_t payload_bytes);

    // Growing snapshots share complete words and own a private partial word.
    TextIndexReader(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        SharedValidityBitmap::Snapshot validity,
        int64_t count,
        DataType value_type,
        bool file_backed,
        size_t payload_bytes);

    ~TextIndexReader() override;

    TextIndexReader(const TextIndexReader&) = delete;
    TextIndexReader&
    operator=(const TextIndexReader&) = delete;

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

    // ---- Resource accounting -----------------------------------------------

    // ---- ITextMatchReader ----------------------------------------

    TargetBitmap
    MatchQuery(std::string_view query,
               uint32_t min_should_match) const override;

    TargetBitmap
    PhraseMatchQuery(std::string_view query, uint32_t slop) const override;

    TargetBitmap
    FuzzyMatchQuery(std::string_view query,
                    uint32_t max_edit_distance) const override;

    // ---- INullReader ---------------------------------------------

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

    std::shared_ptr<const SharedValidityBitmap::Snapshot>
    Validity() const;

 private:
    friend class TextIndex;
    TextIndexReader(std::shared_ptr<const TextIndex> index,
                    milvus::OpContext* op_ctx);

    std::shared_ptr<const TextIndex> index_;
    milvus::OpContext* const op_ctx_;
    mutable std::once_flag storage_once_;
    mutable std::shared_ptr<const TextIndexStorage> storage_;

    const std::shared_ptr<const TextIndexStorage>&
    GetStorage() const;
};

}  // namespace milvus::index
