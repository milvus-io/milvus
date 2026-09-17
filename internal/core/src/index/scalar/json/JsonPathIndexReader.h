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
#include <string>
#include <string_view>
#include <vector>

#include "common/JsonCastType.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/IJsonIndexReader.h"

namespace milvus::index {

// The erased value type is the element type for ARRAY casts as well.
DataType
JsonProjectedValueTypeForCast(JsonCastType cast_type);

// Immutable routing metadata for one typed JSON-path index. ARRAY casts are
// row-domain indexes whose erased ValueType is the element type. Because that
// provenance cannot be recovered from IIndexReaderBase, callers constructing an
// ARRAY spec must have created/opened `inner` through the ArrayView route.
class JsonProjectedIndexSpec final {
 public:
    JsonProjectedIndexSpec(std::string json_path,
                           JsonCastType cast_type,
                           int64_t row_count);

    const std::string&
    JsonPath() const;

    JsonCastType
    CastType() const;

    int64_t
    RowCount() const;

    bool
    Matches(std::string_view json_path, JsonCastType cast_type) const;

    void
    ValidateInner(const IIndexReaderBase& inner) const;

    void
    ValidateNonExistOffsets(const std::vector<size_t>& offsets) const;

 private:
    std::string json_path_;
    JsonCastType cast_type_;
    int64_t row_count_{0};
};

// Path router for one ordinary typed reader. Predicate/pattern/ngram
// capabilities reported by Caps() are exposed only through Resolve(); callers
// must not sibling-cast this outer object to those interfaces. The cold route
// must select IJsonIndexReader first whenever caps.json_paths is true.
class JsonPathIndexReader final : public IIndexReaderBase,
                                  public IJsonIndexReader {
 public:
    JsonPathIndexReader(std::unique_ptr<IIndexReaderBase> inner,
                        JsonProjectedIndexSpec spec,
                        const std::vector<size_t>& non_exist_offsets);

    ~JsonPathIndexReader() override;

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
    std::unique_ptr<IIndexReaderBase> inner_;
    JsonProjectedIndexSpec spec_;
    TargetBitmap exists_;
    ReaderCaps caps_;
    DataType value_type_{DataType::NONE};
    int64_t heap_bytes_{0};
    int64_t cell_memory_bytes_{0};
    int64_t file_bytes_{0};
};

}  // namespace milvus::index
