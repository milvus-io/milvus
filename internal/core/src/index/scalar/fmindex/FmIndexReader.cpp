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

#include "index/scalar/fmindex/FmIndexReader.h"

#include <cmath>
#include <filesystem>
#include <limits>
#include <mutex>
#include <sys/mman.h>
#include <utility>

#include "common/EasyAssert.h"
#include "index/fmindex/FMIndex.h"

namespace milvus::index {
namespace {

int64_t
SaturatingSize(size_t value) {
    return value > static_cast<size_t>(std::numeric_limits<int64_t>::max())
               ? std::numeric_limits<int64_t>::max()
               : static_cast<int64_t>(value);
}

void
SaturatingAdd(size_t& total, size_t value) {
    if (value > std::numeric_limits<size_t>::max() - total) {
        total = std::numeric_limits<size_t>::max();
    } else {
        total += value;
    }
}

const uint8_t*
Bytes(std::string_view value) {
    return reinterpret_cast<const uint8_t*>(value.data());
}

}  // namespace

FmIndexMappedFile::FmIndexMappedFile(void* data,
                                     size_t mapped_bytes,
                                     std::string staging_directory)
    : data_(data),
      mapped_bytes_(mapped_bytes),
      staging_directory_(std::move(staging_directory)) {
    AssertInfo(data_ != nullptr && data_ != MAP_FAILED,
               "FM-index mapping must have a valid address");
    AssertInfo(mapped_bytes_ != 0, "FM-index mapping must not be empty");
    AssertInfo(!staging_directory_.empty(),
               "FM-index mapping must own a staging directory");
}

FmIndexMappedFile::~FmIndexMappedFile() {
    if (data_ != nullptr && data_ != MAP_FAILED && mapped_bytes_ != 0) {
        ::munmap(data_, mapped_bytes_);
    }
    if (!staging_directory_.empty()) {
        std::error_code ignored;
        std::filesystem::remove_all(staging_directory_, ignored);
    }
}

const uint8_t*
FmIndexMappedFile::Data() const {
    return static_cast<const uint8_t*>(data_);
}

size_t
FmIndexMappedFile::MappedBytes() const {
    return mapped_bytes_;
}

size_t
FmIndexMappedFile::HeapBytes() const {
    size_t total = sizeof(FmIndexMappedFile);
    const auto inline_capacity = std::string{}.capacity();
    if (staging_directory_.capacity() > inline_capacity) {
        const auto path_bytes =
            staging_directory_.capacity() == std::numeric_limits<size_t>::max()
                ? std::numeric_limits<size_t>::max()
                : staging_directory_.capacity() + 1;
        SaturatingAdd(total, path_bytes);
    }
    return total;
}

FmIndexStorage::FmIndexStorage(
    std::shared_ptr<const FmIndexMappedFile> mapped_file,
    std::shared_ptr<const fmindex::FMIndex> engine,
    TargetBitmap null_bitmap,
    int64_t total_rows,
    int64_t total_tokens,
    DataType value_type,
    bool nullable,
    int64_t memory_usage,
    int64_t file_bytes)
    : mapped_file_(std::move(mapped_file)),
      engine_(std::move(engine)),
      null_bitmap_(std::move(null_bitmap)),
      total_rows_(total_rows),
      total_tokens_(total_tokens),
      value_type_(value_type),
      nullable_(nullable),
      memory_usage_(memory_usage),
      file_bytes_(file_bytes) {
}

std::shared_ptr<const FmIndexStorage>
FmIndexStorage::Create(std::shared_ptr<const FmIndexMappedFile> mapped_file,
                       std::shared_ptr<const fmindex::FMIndex> engine,
                       TargetBitmap null_bitmap,
                       int64_t total_rows,
                       DataType value_type,
                       bool nullable,
                       FmIndexStateOrigin origin) {
    const auto persisted = origin == FmIndexStateOrigin::Persisted;
    if (engine == nullptr || !engine->valid()) {
        if (persisted) {
            ThrowInfo(DataFormatBroken,
                      "FM-index blob failed structural validation");
        }
        AssertInfo(false, "FM-index state requires a valid engine");
    }
    if (total_rows < 0) {
        if (persisted) {
            ThrowInfo(DataFormatBroken,
                      "FM-index row count must not be negative");
        }
        AssertInfo(false, "FM-index state row count must not be negative");
    }
    if (engine->document_count() != static_cast<size_t>(total_rows)) {
        if (persisted) {
            ThrowInfo(DataFormatBroken,
                      "FM-index metadata rows {} disagree with blob documents "
                      "{}",
                      total_rows,
                      engine->document_count());
        }
        AssertInfo(false,
                   "FM-index state row/document counts disagree: {} vs {}",
                   total_rows,
                   engine->document_count());
    }
    if (null_bitmap.size() != static_cast<size_t>(total_rows)) {
        if (persisted) {
            ThrowInfo(DataFormatBroken,
                      "FM-index null bitmap has {} rows; expected {}",
                      null_bitmap.size(),
                      total_rows);
        }
        AssertInfo(false,
                   "FM-index state null bitmap size disagrees with row count");
    }
    if (!nullable && !null_bitmap.none()) {
        if (persisted) {
            ThrowInfo(DataFormatBroken,
                      "non-nullable FM-index contains null rows");
        }
        AssertInfo(false, "non-nullable FM-index state contains null rows");
    }
    AssertInfo(IsStringDataType(value_type),
               "FM-index state requires a string value type");
    if (engine->bwt_size() == 0 ||
        engine->bwt_size() - 1 < static_cast<size_t>(total_rows)) {
        if (persisted) {
            ThrowInfo(DataFormatBroken,
                      "FM-index internal text is shorter than its document "
                      "count");
        }
        AssertInfo(false,
                   "FM-index internal text is shorter than its document count");
    }

    const auto tokens =
        engine->bwt_size() - 1 - static_cast<size_t>(total_rows);
    size_t heap_bytes = sizeof(FmIndexStorage);
    SaturatingAdd(heap_bytes, sizeof(fmindex::FMIndex));
    SaturatingAdd(heap_bytes, sizeof(std::once_flag));
    SaturatingAdd(heap_bytes, engine->resident_heap_bytes());
    SaturatingAdd(heap_bytes, null_bitmap.size_in_bytes());
    int64_t file_bytes = 0;
    if (mapped_file != nullptr) {
        SaturatingAdd(heap_bytes, mapped_file->HeapBytes());
        file_bytes = SaturatingSize(mapped_file->MappedBytes());
    }
    return std::shared_ptr<const FmIndexStorage>(
        new FmIndexStorage(std::move(mapped_file),
                           std::move(engine),
                           std::move(null_bitmap),
                           total_rows,
                           SaturatingSize(tokens),
                           value_type,
                           nullable,
                           SaturatingSize(heap_bytes),
                           file_bytes));
}

const fmindex::FMIndex&
FmIndexStorage::Engine() const {
    return *engine_;
}

const TargetBitmap&
FmIndexStorage::NullBitmap() const {
    return null_bitmap_;
}

int64_t
FmIndexStorage::Count() const {
    return total_rows_;
}

int64_t
FmIndexStorage::TotalTokens() const {
    return total_tokens_;
}

DataType
FmIndexStorage::ValueType() const {
    return value_type_;
}

bool
FmIndexStorage::Nullable() const {
    return nullable_;
}

int64_t
FmIndexStorage::MemoryUsage() const {
    return memory_usage_;
}

int64_t
FmIndexStorage::FileBytes() const {
    return file_bytes_;
}

FmIndexReader::FmIndexReader(std::shared_ptr<const FmIndexStorage> storage,
                             double cost_ratio)
    : storage_(std::move(storage)), cost_ratio_(cost_ratio) {
    AssertInfo(storage_ != nullptr, "FM-index reader requires shared storage");
    AssertInfo(std::isfinite(cost_ratio_) && cost_ratio_ >= 0,
               "FM-index cost ratio must be finite and non-negative");
}

FmIndexReader::~FmIndexReader() = default;

ReaderCaps
FmIndexReader::Caps() const {
    return ReaderCaps{.pattern_match = true};
}

Domain
FmIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
FmIndexReader::Count() const {
    return storage_->Count();
}

DataType
FmIndexReader::ValueType() const {
    return storage_->ValueType();
}

int64_t
FmIndexReader::MemoryUsage() const {
    size_t total = sizeof(FmIndexReader);
    SaturatingAdd(total, static_cast<size_t>(storage_->MemoryUsage()));
    return SaturatingSize(total);
}

cachinglayer::ResourceUsage
FmIndexReader::CellByteSize() const {
    return {MemoryUsage(), storage_->FileBytes()};
}

TargetBitmap
FmIndexReader::PatternMatch(std::string_view pattern, PatternOp op) const {
    if (pattern.empty()) {
        switch (op) {
            case PatternOp::PrefixMatch:
            case PatternOp::PostfixMatch:
            case PatternOp::InnerMatch:
                return IsNotNull();
            default:
                break;
        }
    }

    switch (op) {
        case PatternOp::PrefixMatch:
            return DocsToBitmap(storage_->Engine().LocatePrefixDocs(
                Bytes(pattern), pattern.size()));
        case PatternOp::PostfixMatch:
            return DocsToBitmap(storage_->Engine().LocateSuffixDocs(
                Bytes(pattern), pattern.size()));
        case PatternOp::InnerMatch: {
            TargetBitmap result(static_cast<size_t>(storage_->Count()), false);
            const auto rows = static_cast<uint64_t>(storage_->Count());
            storage_->Engine().VisitMatchingDocs(
                Bytes(pattern), pattern.size(), [&](uint64_t document) {
                    if (document < rows) {
                        result.set(static_cast<size_t>(document));
                    }
                });
            return result;
        }
        case PatternOp::Match:
        case PatternOp::RegexMatch:
            ThrowInfo(Unsupported,
                      "FM-index does not support general LIKE or regex");
        default:
            ThrowInfo(UnexpectedError,
                      "invalid FM-index pattern operation {}",
                      static_cast<int>(op));
    }
}

bool
FmIndexReader::ShouldUseForOp(PatternOp op, std::string_view pattern) const {
    switch (op) {
        case PatternOp::PrefixMatch:
        case PatternOp::PostfixMatch:
        case PatternOp::InnerMatch:
            break;
        case PatternOp::Match:
        case PatternOp::RegexMatch:
            return false;
        default:
            return false;
    }
    if (pattern.empty()) {
        return true;
    }
    const auto occurrences = PatternCount(pattern, op);
    if (occurrences < 0) {
        return true;
    }
    if (occurrences == 0) {
        return true;
    }
    return static_cast<double>(occurrences) *
               static_cast<double>(storage_->Engine().sa_sample_rate()) <
           cost_ratio_ * static_cast<double>(storage_->TotalTokens());
}

TargetBitmap
FmIndexReader::IsNull() const {
    return storage_->NullBitmap().clone();
}

TargetBitmap
FmIndexReader::IsNotNull() const {
    auto result = storage_->NullBitmap().clone();
    result.flip();
    return result;
}

int64_t
FmIndexReader::PatternCount(std::string_view pattern, PatternOp op) const {
    size_t count = 0;
    switch (op) {
        case PatternOp::PrefixMatch:
            count = storage_->Engine().CountPrefixDocs(Bytes(pattern),
                                                       pattern.size());
            break;
        case PatternOp::PostfixMatch:
            count = storage_->Engine().CountSuffixDocs(Bytes(pattern),
                                                       pattern.size());
            break;
        case PatternOp::InnerMatch:
            count = storage_->Engine().Count(Bytes(pattern), pattern.size());
            break;
        case PatternOp::Match:
        case PatternOp::RegexMatch:
            return -1;
        default:
            return -1;
    }
    return SaturatingSize(count);
}

TargetBitmap
FmIndexReader::DocsToBitmap(const std::vector<uint64_t>& docs) const {
    TargetBitmap result(static_cast<size_t>(storage_->Count()), false);
    const auto rows = static_cast<uint64_t>(storage_->Count());
    for (const auto document : docs) {
        if (document < rows) {
            result.set(static_cast<size_t>(document));
        }
    }
    return result;
}

}  // namespace milvus::index
