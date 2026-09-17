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

#include "index/scalar/text/TextIndexReader.h"

#include <algorithm>
#include <limits>
#include <string>
#include <utility>

#include "common/EasyAssert.h"
#include "storage/artifact/LocalDirectory.h"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

int64_t
ToUsageBytes(size_t bytes) {
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "text reader resource size exceeds int64 domain");
    }
    return static_cast<int64_t>(bytes);
}

std::string
OwnString(std::string_view value) {
    return value.empty() ? std::string{}
                         : std::string(value.data(), value.size());
}

}  // namespace

TextIndexReader::TextIndexReader(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    int64_t count,
    DataType value_type,
    bool file_backed,
    size_t payload_bytes)
    : directory_(std::move(directory)),
      engine_(std::move(engine)),
      null_offsets_(std::move(null_offsets)),
      count_(count),
      value_type_(value_type),
      file_backed_(file_backed),
      payload_bytes_(payload_bytes) {
    AssertInfo(engine_ != nullptr, "text reader requires an engine");
    AssertInfo(null_offsets_ != nullptr,
               "text reader requires frozen null offsets");
    AssertInfo(count_ >= 0, "text reader count must be non-negative");
    AssertInfo(IsStringDataType(value_type_),
               "text reader requires a string value type, got {}",
               static_cast<int>(value_type_));
    AssertInfo(!file_backed_ || directory_ != nullptr,
               "file-backed text reader requires a directory owner");
    const auto engine_count = static_cast<int64_t>(engine_->count());
    AssertInfo(engine_count == count_,
               "text reader count {} disagrees with Tantivy count {}",
               count_,
               engine_count);
    AssertInfo(std::is_sorted(null_offsets_->begin(), null_offsets_->end()),
               "text reader null offsets must be sorted");
    AssertInfo(std::adjacent_find(null_offsets_->begin(), null_offsets_->end()) ==
                   null_offsets_->end(),
               "text reader null offsets must be unique");
    AssertInfo(null_offsets_->empty() ||
                   null_offsets_->back() < static_cast<size_t>(count_),
               "text reader null offset exceeds row count {}",
               count_);
}

TextIndexReader::~TextIndexReader() = default;

ReaderCaps
TextIndexReader::Caps() const {
    // Must match TextIndexLoader::DeriveCaps for the post-pin consistency check.
    return ReaderCaps{.text_match = true};
}

Domain
TextIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
TextIndexReader::Count() const {
    return count_;
}

DataType
TextIndexReader::ValueType() const {
    return value_type_;
}

int64_t
TextIndexReader::MemoryUsage() const {
    // Tantivy exposes managed-directory payload bytes but not an exact Rust
    // reader/analyzer heap census. Count the known C++ ownership explicitly;
    // for RAM indexes, also count the RAM-backed managed payload.
    constexpr size_t kKnownMetadataBytes =
        sizeof(TextIndexReader) + sizeof(milvus::tantivy::TantivyIndexWrapper) +
        sizeof(std::vector<size_t>);
    size_t total = kKnownMetadataBytes;
    if (null_offsets_->capacity() >
        (std::numeric_limits<size_t>::max() - total) / sizeof(size_t)) {
        ThrowInfo(DataFormatBroken, "text reader memory size overflows");
    }
    total += null_offsets_->capacity() * sizeof(size_t);
    if (directory_ != nullptr) {
        const auto directory_bytes = directory_->HeapBytes();
        if (directory_bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken, "text reader memory size overflows");
        }
        total += directory_bytes;
    }
    if (!file_backed_) {
        if (payload_bytes_ > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken, "text reader memory size overflows");
        }
        total += payload_bytes_;
    }
    return ToUsageBytes(total);
}

cachinglayer::ResourceUsage
TextIndexReader::CellByteSize() const {
    return {MemoryUsage(), file_backed_ ? ToUsageBytes(payload_bytes_) : 0};
}

TargetBitmap
TextIndexReader::PrepareBitset() const {
    return TargetBitmap(static_cast<size_t>(count_));
}

TargetBitmap
TextIndexReader::MatchQuery(std::string_view query,
                            uint32_t min_should_match) const {
    auto bitset = PrepareBitset();
    engine_->match_query(OwnString(query), min_should_match, &bitset);
    return bitset;
}

TargetBitmap
TextIndexReader::PhraseMatchQuery(std::string_view query, uint32_t slop) const {
    auto bitset = PrepareBitset();
    engine_->phrase_match_query(OwnString(query), slop, &bitset);
    return bitset;
}

TargetBitmap
TextIndexReader::FuzzyMatchQuery(std::string_view query,
                                 uint32_t max_edit_distance) const {
    auto bitset = PrepareBitset();
    engine_->fuzzy_match_query(OwnString(query), max_edit_distance, &bitset);
    return bitset;
}

TargetBitmap
TextIndexReader::IsNull() const {
    TargetBitmap result(static_cast<size_t>(count_));
    for (const auto offset : *null_offsets_) {
        result.set(offset);
    }
    return result;
}

TargetBitmap
TextIndexReader::IsNotNull() const {
    TargetBitmap result(static_cast<size_t>(count_), true);
    for (const auto offset : *null_offsets_) {
        result.reset(offset);
    }
    return result;
}

}  // namespace milvus::index
