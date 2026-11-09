// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
#include <mutex>
#include <string>
#include <string_view>
#include <vector>
#include <variant>
#include "cachinglayer/CacheSlot.h"

#include "common/Types.h"
#include "index/contracts/IIndex.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/INullReader.h"

// The field-level JSON reader is only a router. Resolve() returns a path-bound
// reader implementing the ordinary predicate mixins. The concrete path reader
// classes stay private to the implementation so an engine-shaped class does
// not become another public contract.

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

// Immutable family state shared by root and path readers. Directory precedes
// engine so a mapped Tantivy reader is destroyed before its owned staging
// files. RAM states deliberately retain no directory.
class JsonFlatIndexStorage final {
 public:
    static std::unique_ptr<JsonFlatIndexStorage>
    Create(std::shared_ptr<storage::LocalDirectory> directory,
           std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
           std::string field_path_prefix,
           std::shared_ptr<const std::vector<size_t>> null_offsets,
           bool mmap,
           size_t engine_bytes,
           size_t engine_path_bytes);

    milvus::tantivy::TantivyIndexWrapper&
    Engine() const;

    const std::string&
    RootPath() const;

    uint32_t
    Count() const;

    int64_t
    HeapBytes() const;

    int64_t
    FileBytes() const;

    cachinglayer::ResourceUsage
    CellByteSize() const;

    TargetBitmap
    FieldIsNull() const;

    TargetBitmap
    FieldIsNotNull() const;

 private:
    JsonFlatIndexStorage(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::string field_path_prefix,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        bool mmap,
        size_t engine_bytes,
        size_t engine_path_bytes);

    std::shared_ptr<storage::LocalDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    // Field-level validity materialized once at open instead of replaying the
    // null-offset vector on every FieldIsNull/FieldIsNotNull call, which sit on
    // the query hot path. The offset vector is not retained: it would duplicate
    // the same information at 8 bytes per null row. `all_valid_` leaves the
    // bitmap empty rather than allocating rows/8 bytes of all-ones for a field
    // with no nulls.
    bool all_valid_{false};
    TargetBitmap valid_bitmap_;
    std::string field_path_prefix_;
    bool mmap_{false};
    size_t engine_bytes_{0};
    size_t engine_path_bytes_{0};
    uint32_t count_{0};
    int64_t heap_bytes_{0};
    int64_t file_bytes_{0};
};

/** @brief Owns one immutable JSON field index independently of its readers. */
class JsonFlatIndex final : public IIndex,
                            public std::enable_shared_from_this<JsonFlatIndex> {
 public:
    explicit JsonFlatIndex(std::shared_ptr<const JsonFlatIndexStorage> storage);
    JsonFlatIndex(
        std::shared_ptr<cachinglayer::CacheSlot<JsonFlatIndexStorage>> slot,
        std::optional<int64_t> count,
        std::string root_path);

    IndexMetadata
    Metadata() const override;

    IIndexReaderBasePtr
    CreateReader(milvus::OpContext* op_ctx) const override;

    void
    CancelWarmup() const override;

 private:
    friend class JsonFlatIndexReader;
    std::shared_ptr<const JsonFlatIndexStorage>
    PinStorage(milvus::OpContext* op_ctx) const;
    const std::string&
    RootPath() const;
    struct CachedStorage {
        std::shared_ptr<cachinglayer::CacheSlot<JsonFlatIndexStorage>> slot;
        std::optional<int64_t> count;
        std::string root_path;
    };
    std::variant<std::shared_ptr<const JsonFlatIndexStorage>, CachedStorage>
        storage_;
};

class JsonFlatIndexReader final : public IIndexReaderBase,
                                  public IJsonIndexReader,
                                  public INullReader {
 public:
    explicit JsonFlatIndexReader(
        std::shared_ptr<const JsonFlatIndexStorage> state);

    ~JsonFlatIndexReader() override;

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

    // Field-level validity. Path-bound readers intentionally expose a
    // different INullReader view: comparable values at that path.
    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

    // Paths are absolute JSON pointers in the indexed field. A non-empty root
    // prefix is stripped only on an exact or slash-boundary match. Positional
    // array segments are not representable by Tantivy's flattened JSON path;
    // Resolve returns null and CastTypesOf returns empty as a post-pin
    // consistency signal. The pre-pin route uses the inventory's root-path
    // metadata and applies the same rule without opening this reader.
    JsonResolvedReader
    Resolve(std::string_view path, JsonCastType cast_type) const override;

    // Precondition: CastTypesOf(path) is non-empty. This method cannot encode
    // the unsupported-path fallback in its bitmap-only return type, so a
    // violation is an internal consumer protocol error rather than a business
    // routing signal.
    TargetBitmap
    Exists(std::string_view path) const override;

    std::vector<JsonCastType>
    CastTypesOf(std::string_view path) const override;

 private:
    friend class JsonFlatIndex;
    JsonFlatIndexReader(std::shared_ptr<const JsonFlatIndex> index,
                        milvus::OpContext* op_ctx);

    std::shared_ptr<const JsonFlatIndex> index_;
    milvus::OpContext* const op_ctx_;
    mutable std::once_flag storage_once_;
    mutable std::shared_ptr<const JsonFlatIndexStorage> storage_;

    const std::shared_ptr<const JsonFlatIndexStorage>&
    GetStorage() const;
};

}  // namespace milvus::index
