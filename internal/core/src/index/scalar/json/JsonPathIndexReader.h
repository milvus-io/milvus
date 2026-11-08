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
#include <functional>
#include <optional>
#include <variant>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "common/JsonCastType.h"
#include "index/contracts/IIndex.h"
#include "index/contracts/query/IJsonIndexReader.h"

namespace milvus::index {

// The erased value type is the element type for ARRAY casts as well.
DataType
JsonProjectedValueTypeForCast(JsonCastType cast_type);

// Explicit JSON SORT supports numeric/string casts and BOOL array casts.
bool
IsJsonSortCast(JsonCastType cast_type);

// Immutable routing metadata for one typed JSON-path index. ARRAY casts are
// row-domain indexes whose erased ValueType is the element type. Because that
// provenance cannot be recovered from IndexMetadata, callers constructing an
// ARRAY spec must have created/opened `inner` through the ArrayView route.
class JsonProjectedIndexSpec final {
 public:
    JsonProjectedIndexSpec(std::string json_path,
                           JsonCastType cast_type,
                           std::optional<int64_t> row_count);

    const std::string&
    JsonPath() const;

    JsonCastType
    CastType() const;

    int64_t
    RowCount() const;

    bool
    Matches(std::string_view json_path, JsonCastType cast_type) const;

    void
    ValidateInner(const IndexMetadata& inner) const;

    /** @brief Validate offsets and construct the existence bitmap. */
    std::shared_ptr<const TargetBitmap>
    MakeExists(const std::vector<size_t>& offsets) const;

    void
    ValidateNonExistOffsets(const std::vector<size_t>& offsets) const;

 private:
    std::string json_path_;
    JsonCastType cast_type_;
    std::optional<int64_t> row_count_;
};

/** @brief Owns a typed child index and immutable JSON path/existence metadata. */
class JsonPathIndex final : public IIndex,
                            public std::enable_shared_from_this<JsonPathIndex> {
 public:
    JsonPathIndex(IndexPtr inner,
                  JsonProjectedIndexSpec spec,
                  const std::vector<size_t>& non_exist_offsets);

    JsonPathIndex(IndexPtr inner,
                  JsonProjectedIndexSpec spec,
                  std::shared_ptr<const TargetBitmap> exists);

    /** @brief Route JSON queries while the child storage owns the existence bitmap.
     * The callback pins the same storage cell as ordinary child operations.
     */
    using PinExists =
        std::function<std::shared_ptr<const TargetBitmap>(OpContext*)>;
    JsonPathIndex(IndexPtr inner,
                  JsonProjectedIndexSpec spec,
                  PinExists pin_exists);

    template <typename Index>
    static IndexPtr
    FromStorage(std::shared_ptr<Index> inner, JsonProjectedIndexSpec spec) {
        PinExists pin = [inner](OpContext* ctx) {
            auto storage = inner->PinStorage(ctx);
            AssertInfo(storage->json_exists != nullptr,
                       "complete JSON storage lacks existence bitmap");
            return std::shared_ptr<const TargetBitmap>(
                storage, storage->json_exists.get());
        };
        return std::make_shared<JsonPathIndex>(
            std::move(inner), std::move(spec), std::move(pin));
    }

    IndexMetadata
    Metadata() const override;

    IIndexReaderBasePtr
    CreateReader(milvus::OpContext* op_ctx) const override;

    void
    CancelWarmup() const override;

 private:
    friend class JsonPathIndexReader;
    IndexPtr inner_;
    JsonProjectedIndexSpec spec_;
    std::variant<std::shared_ptr<const TargetBitmap>, PinExists> exists_;
};

// Path router for one ordinary typed reader. Predicate/pattern/ngram
// capabilities reported by Caps() are exposed only through Resolve(); callers
// must not sibling-cast this outer object to those interfaces. The cold route
// must select IJsonIndexReader first whenever caps.json_paths is true.
class JsonPathIndexReader final : public IIndexReaderBase,
                                  public IJsonIndexReader {
 public:
    ~JsonPathIndexReader() override;

    void
    Prefetch() const override {
        inner_->Prefetch();
    }

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    JsonResolvedReader
    Resolve(std::string_view path, JsonCastType cast_type) const override;

    // Precondition: `path` is this reader's exact registered path. Unknown
    // paths must be rejected by the metadata-only route before pinning;
    // violations are internal protocol errors, not fallback signals.
    TargetBitmap
    Exists(std::string_view path) const override;

    std::vector<JsonCastType>
    CastTypesOf(std::string_view path) const override;

 private:
    friend class JsonPathIndex;
    JsonPathIndexReader(std::shared_ptr<const JsonPathIndex> index,
                        milvus::OpContext* op_ctx);

    std::shared_ptr<const JsonPathIndex> index_;
    IIndexReaderBasePtr inner_;
    OpContext* const op_ctx_;
    mutable std::once_flag exists_once_;
    mutable std::shared_ptr<const TargetBitmap> exists_;
};

}  // namespace milvus::index
