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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <marisa.h>

#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"

// Trie-backed string predicates, patterns, reverse lookup, and null queries.
// The reader is row-level only: it has no nested-mode constructor input.

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
    std::shared_ptr<MarisaMmapOwner> mmap_owner;
    std::shared_ptr<const std::vector<int64_t>> str_ids_owner;
    std::shared_ptr<const std::vector<uint32_t>> csr_index_owner;
    std::shared_ptr<const std::vector<uint32_t>> csr_offsets_owner;
    std::shared_ptr<const marisa::Trie> trie;
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

class MarisaIndexReader final : public IIndexReaderBase,
                                public IScalarPredicateReader<std::string_view>,
                                public IScalarValueReader<std::string_view>,
                                public IPatternMatchReader,
                                public INullReader {
 public:
    explicit MarisaIndexReader(
        std::shared_ptr<const MarisaIndexStorage> storage);

    ~MarisaIndexReader() override;

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
    size_t
    LookupKeyId(std::string_view value) const;

    std::vector<size_t>
    PrefixMatchKeyIds(std::string_view prefix) const;

    // marisa's key order is not always lexicographic; the range paths take a
    // fast or a slow route depending on this (`StringIndexMarisa.cpp:813-823`).
    bool
    InLexicographicOrder() const;

    std::shared_ptr<const MarisaIndexStorage> storage_;
};

}  // namespace milvus::index
