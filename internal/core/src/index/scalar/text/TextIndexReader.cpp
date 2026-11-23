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

SharedValidityBitmap::Snapshot
SealedValidity(const std::shared_ptr<const std::vector<size_t>>& null_offsets,
               int64_t count) {
    AssertInfo(null_offsets != nullptr,
               "text reader requires frozen null offsets");
    AssertInfo(count >= 0, "text reader count must be non-negative");
    AssertInfo(std::is_sorted(null_offsets->begin(), null_offsets->end()),
               "text reader null offsets must be sorted");
    AssertInfo(std::adjacent_find(null_offsets->begin(), null_offsets->end()) ==
                   null_offsets->end(),
               "text reader null offsets must be unique");
    AssertInfo(null_offsets->empty() ||
                   null_offsets->back() < static_cast<size_t>(count),
               "text reader null offset exceeds row count {}",
               count);
    return SharedValidityBitmap::FromValidatedNullOffsets(
        static_cast<size_t>(count), *null_offsets);
}

std::string
OwnString(std::string_view value) {
    return value.empty() ? std::string{}
                         : std::string(value.data(), value.size());
}

}  // namespace

TextIndexStorage::TextIndexStorage(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    int64_t count,
    DataType value_type,
    bool file_backed,
    size_t payload_bytes)
    : TextIndexStorage(std::move(directory),
                       std::move(engine),
                       SealedValidity(null_offsets, count),
                       count,
                       value_type,
                       file_backed,
                       payload_bytes) {
}

TextIndexStorage::TextIndexStorage(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    SharedValidityBitmap::Snapshot validity,
    int64_t count,
    DataType value_type,
    bool file_backed,
    size_t payload_bytes)
    : directory_(std::move(directory)),
      engine_(std::move(engine)),
      validity_(std::move(validity)),
      count_(count),
      value_type_(value_type),
      file_backed_(file_backed),
      payload_bytes_(payload_bytes) {
    AssertInfo(engine_ != nullptr, "text reader requires an engine");
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
    AssertInfo(validity_.Count() == static_cast<size_t>(count_),
               "text reader validity count disagrees with its reader");
}

TextIndex::TextIndex(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    int64_t count,
    DataType value_type,
    bool file_backed,
    size_t payload_bytes)
    : TextIndex(std::make_shared<TextIndexStorage>(std::move(directory),
                                                   std::move(engine),
                                                   std::move(null_offsets),
                                                   count,
                                                   value_type,
                                                   file_backed,
                                                   payload_bytes)) {
}

TextIndexReader::TextIndexReader(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    int64_t count,
    DataType value_type,
    bool file_backed,
    size_t payload_bytes)
    : TextIndexReader(std::make_shared<TextIndex>(std::move(directory),
                                                  std::move(engine),
                                                  std::move(null_offsets),
                                                  count,
                                                  value_type,
                                                  file_backed,
                                                  payload_bytes),
                      nullptr) {
}

TextIndex::TextIndex(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    SharedValidityBitmap::Snapshot validity,
    int64_t count,
    DataType value_type,
    bool file_backed,
    size_t payload_bytes)
    : TextIndex(std::make_shared<TextIndexStorage>(std::move(directory),
                                                   std::move(engine),
                                                   std::move(validity),
                                                   count,
                                                   value_type,
                                                   file_backed,
                                                   payload_bytes)) {
}

TextIndexReader::TextIndexReader(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    SharedValidityBitmap::Snapshot validity,
    int64_t count,
    DataType value_type,
    bool file_backed,
    size_t payload_bytes)
    : TextIndexReader(std::make_shared<TextIndex>(std::move(directory),
                                                  std::move(engine),
                                                  std::move(validity),
                                                  count,
                                                  value_type,
                                                  file_backed,
                                                  payload_bytes),
                      nullptr) {
}

TextIndex::TextIndex(std::shared_ptr<const TextIndexStorage> storage)
    : storage_(std::move(storage)) {
    AssertInfo(
        std::get<std::shared_ptr<const TextIndexStorage>>(storage_) != nullptr,
        "text index requires storage");
}

TextIndex::TextIndex(
    std::shared_ptr<cachinglayer::CacheSlot<TextIndexStorage>> slot,
    IndexMetadata metadata)
    : storage_(CachedStorage{std::move(slot), metadata}) {
    AssertInfo(std::get<CachedStorage>(storage_).slot != nullptr,
               "text index requires a cache slot");
    AssertInfo(!metadata.count || *metadata.count >= 0,
               "text count must not be negative");
}

std::shared_ptr<const TextIndexStorage>
TextIndex::PinStorage(milvus::OpContext* op_ctx) const {
    if (const auto* resident =
            std::get_if<std::shared_ptr<const TextIndexStorage>>(&storage_))
        return *resident;
    const auto& cached = std::get<CachedStorage>(storage_);
    std::shared_ptr<const TextIndexStorage> storage;
    if (const auto* resident = cached.slot->TryBorrowCell(0)) {
        storage = {cached.slot, resident};
    } else {
        auto pin =
            cachinglayer::SemiInlineGet(cached.slot->PinCells(op_ctx, {0}));
        const auto* cell = pin->get_ith_cell(0);
        storage = {std::move(pin), cell};
    }
    AssertInfo(storage != nullptr &&
                   (!cached.metadata.count ||
                    storage->count_ == *cached.metadata.count) &&
                   storage->value_type_ == cached.metadata.value_type,
               "text storage disagrees with index metadata");
    return storage;
}

void
TextIndex::CancelWarmup() const {
    if (const auto* cached = std::get_if<CachedStorage>(&storage_))
        cached->slot->CancelWarmup();
}

IndexMetadata
TextIndex::Metadata() const {
    if (const auto* cached = std::get_if<CachedStorage>(&storage_))
        return cached->metadata;
    const auto& storage =
        *std::get<std::shared_ptr<const TextIndexStorage>>(storage_);
    return {ReaderCaps{.text_match = true},
            Domain::Row,
            storage.value_type_,
            storage.count_};
}

IIndexReaderBasePtr
TextIndex::CreateReader(milvus::OpContext* op_ctx) const {
    return IIndexReaderBasePtr(new TextIndexReader(shared_from_this(), op_ctx));
}

TextIndexReader::TextIndexReader(std::shared_ptr<const TextIndex> index,
                                 milvus::OpContext* op_ctx)
    : index_(std::move(index)), op_ctx_(op_ctx) {
}

int64_t
TextIndexStorage::MemoryUsage() const {
    // Tantivy exposes managed-directory payload bytes but not an exact Rust
    // reader/analyzer heap census. Count the known C++ ownership explicitly;
    // for RAM indexes, also count the RAM-backed managed payload.
    constexpr size_t kKnownMetadataBytes =
        sizeof(TextIndexStorage) + sizeof(milvus::tantivy::TantivyIndexWrapper);
    size_t total = kKnownMetadataBytes;
    const auto add_bytes = [&](size_t bytes) {
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken, "text reader memory size overflows");
        }
        total += bytes;
    };
    // sizeof(TextIndexStorage) already includes the snapshot itself. Shared
    // buffers are counted once per storage version, as the prior shared offsets were.
    add_bytes(validity_.BufferBytes());
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
TextIndexStorage::CellByteSize() const {
    return {MemoryUsage(), file_backed_ ? ToUsageBytes(payload_bytes_) : 0};
}

const std::shared_ptr<const TextIndexStorage>&
TextIndexReader::GetStorage() const {
    std::call_once(storage_once_,
                   [this] { storage_ = index_->PinStorage(op_ctx_); });
    return storage_;
}

TextIndexReader::~TextIndexReader() = default;

ReaderCaps
TextIndexReader::Caps() const {
    return index_->Metadata().caps;
}

Domain
TextIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
TextIndexReader::Count() const {
    if (const auto count = index_->Metadata().count)
        return *count;
    return GetStorage()->count_;
}

DataType
TextIndexReader::ValueType() const {
    return index_->Metadata().value_type;
}

TargetBitmap
TextIndexReader::MatchQuery(std::string_view query,
                            uint32_t min_should_match) const {
    const auto& storage = GetStorage();
    TargetBitmap bitset(static_cast<size_t>(storage->count_));
    storage->engine_->match_query(OwnString(query), min_should_match, &bitset);
    return bitset;
}

TargetBitmap
TextIndexReader::PhraseMatchQuery(std::string_view query, uint32_t slop) const {
    const auto& storage = GetStorage();
    TargetBitmap bitset(static_cast<size_t>(storage->count_));
    storage->engine_->phrase_match_query(OwnString(query), slop, &bitset);
    return bitset;
}

TargetBitmap
TextIndexReader::FuzzyMatchQuery(std::string_view query,
                                 uint32_t max_edit_distance) const {
    const auto& storage = GetStorage();
    TargetBitmap bitset(static_cast<size_t>(storage->count_));
    storage->engine_->fuzzy_match_query(
        OwnString(query), max_edit_distance, &bitset);
    return bitset;
}

std::shared_ptr<const SharedValidityBitmap::Snapshot>
TextIndexReader::Validity() const {
    const auto& storage = GetStorage();
    const auto* validity = &storage->validity_;
    return {storage, validity};
}

TargetBitmap
TextIndexReader::IsNull() const {
    auto result = IsNotNull();
    result.flip();
    return result;
}

TargetBitmap
TextIndexReader::IsNotNull() const {
    return GetStorage()->validity_.ToBitmap();
}

}  // namespace milvus::index
