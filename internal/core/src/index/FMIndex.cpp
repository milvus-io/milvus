// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "index/FMIndex.h"

#include <fcntl.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <new>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <folly/ScopeGuard.h>

#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Slice.h"
#include "common/Tracer.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "knowhere/binaryset.h"
#include "log/Log.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Util.h"

namespace milvus::index {

namespace {

// bytes(s): view a string's data as raw bytes for the FM-index library.
inline const uint8_t*
bytes(const std::string& s) {
    return reinterpret_cast<const uint8_t*>(s.data());
}

// Trailing slack appended to the mmap'd blob file so any word-granular read at
// the very end of the FM-index stays inside the mapping.
constexpr size_t kFMIndexMmapPadding = 64;

// Pack a per-row null bitmap (bit i set iff row i is null) into bytes, LSB-first
// within each byte, for on-disk persistence. total_rows bits -> ceil/8 bytes.
inline std::vector<uint8_t>
PackNullBitmap(const TargetBitmap& null_bitmap, int64_t total_rows) {
    const auto packed_size = static_cast<size_t>(total_rows / 8) +
                             static_cast<size_t>(total_rows % 8 != 0);
    std::vector<uint8_t> packed(packed_size, 0);
    for (int64_t i = 0; i < total_rows; i++) {
        if (null_bitmap[i]) {
            packed[i >> 3] |= static_cast<uint8_t>(1u << (i & 0x07));
        }
    }
    return packed;
}

bool
IsSupportedFMIndexDataType(proto::schema::DataType data_type) {
    return data_type == proto::schema::DataType::String ||
           data_type == proto::schema::DataType::VarChar;
}

void
BuildFMIndexLibrary(fmindex::FMIndex& fm,
                    const std::vector<std::string_view>& docs,
                    uint32_t sa_sample_rate,
                    uint32_t block_bytes,
                    int64_t field_id) {
    try {
        fm.Build(docs,
                 sa_sample_rate,
                 /*case_insensitive=*/false,
                 /*force_wide=*/false,
                 block_bytes);
    } catch (const SegcoreError&) {
        throw;
    } catch (const std::bad_alloc& e) {
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "failed to build FM index for field {}: {}",
                  field_id,
                  e.what());
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::IndexBuildError,
                  "failed to build FM index for field {}: {}",
                  field_id,
                  e.what());
    }
}

template <typename T>
T
ReadRequiredFMIndexMeta(const storage::IndexEntryReader& reader,
                        const char* key) {
    if (!reader.HasMeta(key)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "corrupt FM index: required metadata '{}' is missing",
                  key);
    }
    try {
        return reader.GetMeta<T>(key);
    } catch (const SegcoreError&) {
        throw;
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "corrupt FM index: metadata '{}' has invalid type/value: {}",
                  key,
                  e.what());
    }
}

void
CreateParentDirectories(const std::string& path) {
    auto parent = std::filesystem::path(path).parent_path();
    if (parent.empty()) {
        return;
    }
    std::error_code ec;
    std::filesystem::create_directories(parent, ec);
    if (ec) {
        ThrowInfo(ErrorCode::FileCreateFailed,
                  "failed to create FM index directory {}: {}",
                  parent.string(),
                  ec.message());
    }
}

}  // namespace

FMIndex::FMIndex(const storage::FileManagerContext& ctx,
                 const FMIndexParams& params)
    : ScalarIndex<std::string>(FMINDEX_INDEX_TYPE) {
    schema_ = ctx.fieldDataMeta.field_schema;
    field_id_ = ctx.fieldDataMeta.field_id;
    build_sa_sample_rate_ =
        params.sa_sample_rate == 0 ? 8 : params.sa_sample_rate;
    block_bytes_ = params.block_bytes == 0 ? 64 : params.block_bytes;
    // Invalid context = unit test (no remote storage): skip file managers.
    if (ctx.Valid()) {
        this->file_manager_ = std::make_shared<MemFileManager>(ctx);
        disk_file_manager_ =
            std::make_shared<storage::DiskFileManagerImpl>(ctx);
    }
}

void
FMIndex::Build(const Config& config) {
    auto field_datas = storage::CacheRawDataAndFillMissing(
        std::static_pointer_cast<MemFileManager>(this->file_manager_), config);
    BuildWithFieldData(field_datas);
}

void
FMIndex::BuildWithFieldData(const std::vector<FieldDataPtr>& datas) {
    if (!IsSupportedFMIndexDataType(schema_.data_type())) {
        ThrowInfo(ErrorCode::DataTypeInvalid,
                  "FM index only supports String/VarChar, got data type {}",
                  schema_.data_type());
    }
    LOG_INFO("Start to build FM index, field id: {}", field_id_);

    std::vector<std::string_view> docs;
    std::vector<bool> valid;
    for (const auto& data : datas) {
        if (data == nullptr) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "cannot build FM index for field {}: field data is null",
                      field_id_);
        }
        if (data->get_data_type() != DataType::STRING &&
            data->get_data_type() != DataType::VARCHAR) {
            ThrowInfo(
                ErrorCode::DataFormatBroken,
                "cannot build FM index for field {}: field data type {} does "
                "not match String/VarChar schema",
                field_id_,
                data->get_data_type());
        }
        if (!schema_.nullable() && data->get_null_count() != 0) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "cannot build FM index for non-nullable field {}: input "
                      "contains {} null rows",
                      field_id_,
                      data->get_null_count());
        }
        auto n = data->get_num_rows();
        for (int64_t i = 0; i < n; i++) {
            if (schema_.nullable() && !data->is_valid(i)) {
                docs.emplace_back();
                valid.push_back(false);
            } else {
                auto* s = static_cast<const std::string*>(data->RawValue(i));
                if (s == nullptr) {
                    ThrowInfo(ErrorCode::DataFormatBroken,
                              "cannot build FM index for field {}: row {} has "
                              "no string value",
                              field_id_,
                              i);
                }
                docs.emplace_back(*s);
                valid.push_back(true);
            }
        }
    }

    if (docs.size() >
        static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(ErrorCode::IndexBuildError,
                  "cannot build FM index for field {}: row count {} exceeds "
                  "int64 capacity",
                  field_id_,
                  docs.size());
    }
    total_rows_ = static_cast<int64_t>(docs.size());

    // Build the null bitmap (bit i set iff row i is null).
    null_bitmap_ = TargetBitmap(total_rows_);
    for (int64_t i = 0; i < total_rows_; i++) {
        if (!valid[i]) {
            null_bitmap_.set(i);
        }
    }

    // The string_views point into the FieldData which stays alive through the
    // call; the library copies internally during Build.
    BuildFMIndexLibrary(
        fm_, docs, build_sa_sample_rate_, block_bytes_, field_id_);

    // Eagerly fix the guard's token total now (derived from the built blob), on
    // this single-threaded build path, so the concurrent const query path never
    // races on a lazy write.
    ComputeTotalTokens();

    LOG_INFO("FM index build done, field id: {}, total rows: {}",
             field_id_,
             total_rows_);
}

void
FMIndex::BuildWithRawDataForUT(size_t n,
                               const void* values,
                               const Config& config) {
    schema_.set_data_type(proto::schema::DataType::VarChar);

    auto* strs = static_cast<const std::string*>(values);
    std::vector<std::string_view> docs;
    docs.reserve(n);
    for (size_t i = 0; i < n; i++) {
        docs.emplace_back(strs[i]);
    }

    total_rows_ = static_cast<int64_t>(n);
    // UT raw-data path has no nulls (all rows valid).
    null_bitmap_ = TargetBitmap(total_rows_);

    BuildFMIndexLibrary(
        fm_, docs, build_sa_sample_rate_, block_bytes_, field_id_);

    // total_tokens is derived from the built blob, so compute after Build.
    ComputeTotalTokens();
}

TargetBitmap
FMIndex::DocsToBitmap(const std::vector<uint64_t>& docs) const {
    TargetBitmap bitset(total_rows_);
    for (auto d : docs) {
        bitset.set(d);
    }
    return bitset;
}

const TargetBitmap
FMIndex::PatternMatch(const std::string& pattern, proto::plan::OpType op) {
    tracer::AutoSpan span("FMIndex::PatternMatch", tracer::GetRootSpan());
    // Empty pattern: the planner lowers a match-anything LIKE (`LIKE '%'`,
    // `LIKE '%%'`, ...) to an anchored op with an empty literal — PrefixMatch(""),
    // InnerMatch("") or PostfixMatch(""). Every string trivially has "" as a
    // prefix, substring and suffix, so the correct result is ALL non-null rows.
    // The FM-index library short-circuits an empty needle to ZERO hits, so
    // falling through to Locate*/Matching* below would wrongly return the empty
    // set; answer it directly with IsNotNull(). Null rows are excluded, matching
    // SQL `NULL LIKE '%'` = NULL (not true).
    if (pattern.empty()) {
        switch (op) {
            case proto::plan::OpType::PrefixMatch:
            case proto::plan::OpType::PostfixMatch:
            case proto::plan::OpType::InnerMatch:
                return IsNotNull();
            default:
                break;  // fall through so the op switch throws OpTypeInvalid.
        }
    }
    // The executor passes the RAW literal (PrefixMatch gets "abc" not "abc%"),
    // so pass the pattern verbatim. These results are EXACT (no recheck).
    switch (op) {
        case proto::plan::OpType::PrefixMatch:
            return DocsToBitmap(
                fm_.LocatePrefixDocs(bytes(pattern), pattern.size()));
        case proto::plan::OpType::PostfixMatch:
            return DocsToBitmap(
                fm_.LocateSuffixDocs(bytes(pattern), pattern.size()));
        case proto::plan::OpType::InnerMatch: {
            // Streaming visitor, NOT MatchingDocs: the latter materializes
            // O(occurrences) temporaries (positions + (doc,offset) pairs +
            // doc ids), which for a high-frequency pattern (LIKE '%a%' over
            // repetitive text) can transiently allocate GBs. The visitor
            // dedups straight into the result bitmap — O(rows/8) memory
            // regardless of the occurrence count.
            TargetBitmap bitset(total_rows_);
            fm_.VisitMatchingDocs(bytes(pattern),
                                  pattern.size(),
                                  [&](uint64_t d) { bitset.set(d); });
            return bitset;
        }
        default:
            // `op` is selected by the executor's index-routing logic, not read
            // directly from the user request. Reaching this branch therefore
            // means an internal routing contract was violated; keep it a
            // system error instead of projecting OpTypeInvalid as InputError.
            ThrowInfo(ErrorCode::Unsupported,
                      "FM index PatternMatch does not support op type {}; "
                      "the executor should have declined this index",
                      op);
    }
}

const TargetBitmap
FMIndex::In(size_t n, const std::string* values) {
    // FMINDEX does not accelerate the equality family: ShouldUseOp declines
    // Equal/NotEqual (and the TermExpr gate declines IN/NOT IN), so the executor
    // never routes here — a set of exact values is served by the raw-data scan
    // or an equality-oriented index (INVERTED). Exact equality via FM would need
    // a per-row length array whose only purpose was this now-declined path; it
    // was dropped to keep the on-disk footprint at ~1x the text. Throwing (like
    // Range) makes an accidental future route fail loudly instead of silently.
    ThrowInfo(ErrorCode::Unsupported,
              "FMINDEX does not support In(); equality is declined by "
              "ShouldUseOp and never routed to this index");
}

const TargetBitmap
FMIndex::NotIn(size_t n, const std::string* values) {
    ThrowInfo(ErrorCode::Unsupported,
              "FMINDEX does not support NotIn(); equality is declined by "
              "ShouldUseOp and never routed to this index");
}

const TargetBitmap
FMIndex::IsNull() {
    tracer::AutoSpan span("FMIndex::IsNull", tracer::GetRootSpan());
    return null_bitmap_.clone();
}

TargetBitmap
FMIndex::IsNotNull() {
    tracer::AutoSpan span("FMIndex::IsNotNull", tracer::GetRootSpan());
    TargetBitmap bitset = null_bitmap_.clone();
    bitset.flip();
    return bitset;
}

BinarySet
FMIndex::Serialize(const Config& config) {
    // The FM-index persists through the V3 packed-file path (WriteEntries).
    // V1 BinarySet serialization is not used; return an empty set.
    BinarySet res_set;
    milvus::Disassemble(res_set);
    return res_set;
}

IndexStatsPtr
FMIndex::Upload(const Config& config) {
    LOG_INFO("Start to upload FM index, field id: {}, total rows: {}",
             field_id_,
             total_rows_);
    return UploadUnified(config);
}

void
FMIndex::Load(milvus::tracer::TraceContext ctx, const Config& config) {
    LoadUnified(config);
}

void
FMIndex::WriteEntries(storage::IndexEntryWriter* writer) {
    AssertInfo(writer != nullptr, "FM index entry writer is null");
    if (!fm_.valid() || total_rows_ < 0 ||
        fm_.document_count() != static_cast<size_t>(total_rows_) ||
        null_bitmap_.size() != static_cast<size_t>(total_rows_)) {
        ThrowInfo(ErrorCode::IndexBuildError,
                  "cannot serialize inconsistent FM index for field {}: "
                  "valid={}, rows={}, documents={}, null-bitmap-size={}",
                  field_id_,
                  fm_.valid(),
                  total_rows_,
                  fm_.document_count(),
                  null_bitmap_.size());
    }

    size_t blob_size = 0;
    if (disk_file_manager_ != nullptr) {
        // Stream the flat blob through a temp file: SerializeToFile writes the
        // large arrays straight to disk (only the small header is buffered) and
        // the writer's fd overload streams from there — avoiding fm_.Serialize()
        // materializing a second ~1x-index-size buffer in RAM at upload time.
        auto tmp_path = disk_file_manager_->GetLocalIndexObjectPrefix() +
                        std::string("/") + FMINDEX_BLOB_FILE_NAME + ".tmp";
        CreateParentDirectories(tmp_path);
        auto serialize_status = fm_.SerializeToFile(tmp_path);
        if (serialize_status ==
            fmindex::FMIndex::SerializeFileStatus::OpenFailed) {
            ThrowInfo(ErrorCode::FileCreateFailed,
                      "failed to create FM index temp file {}: {}",
                      tmp_path,
                      strerror(errno));
        }
        if (serialize_status ==
            fmindex::FMIndex::SerializeFileStatus::WriteFailed) {
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "failed to write FM index temp file {}: {}",
                      tmp_path,
                      strerror(errno));
        }
        // Remove the temp file on ANY exit — success or a throw from file_size /
        // open / WriteEntry below — so a failed upload never leaks it.
        auto tmp_guard = folly::makeGuard([&tmp_path]() {
            std::error_code ec;
            std::filesystem::remove(tmp_path, ec);
        });
        std::error_code size_ec;
        blob_size = std::filesystem::file_size(tmp_path, size_ec);
        if (size_ec) {
            ThrowInfo(ErrorCode::FileReadFailed,
                      "failed to stat FM index temp file {}: {}",
                      tmp_path,
                      size_ec.message());
        }
        {
            // A freshly opened O_RDONLY fd sits at offset 0, so (unlike the
            // write-then-rewind fd other indexes reuse) no lseek is needed.
            int fd = open(tmp_path.c_str(), O_RDONLY);
            if (fd < 0) {
                ThrowInfo(ErrorCode::FileOpenFailed,
                          "failed to reopen FM index temp file {}: {}",
                          tmp_path,
                          strerror(errno));
            }
            auto fd_guard = folly::makeGuard([fd]() { close(fd); });
            writer->WriteEntry(FMINDEX_BLOB_FILE_NAME, fd, blob_size);
        }
    } else {
        // No local file manager (unit-test path without remote storage): fall
        // back to the in-memory blob.
        std::string blob = fm_.Serialize();
        blob_size = blob.size();
        writer->WriteEntry(FMINDEX_BLOB_FILE_NAME, blob.data(), blob.size());
    }

    // Null bitmap (bit i set iff row i is null), needed so IsNull/IsNotNull and
    // the empty-pattern (`LIKE '%'`) path can exclude null rows after load, and
    // so a null row stays distinct from a genuine empty string (both are empty
    // documents inside the FM blob). Persisted bit-packed at 1 bit/row — and,
    // like BitmapIndex's valid_bitset_, ONLY for nullable fields; a non-nullable
    // field has no null rows, so it writes nothing (zero side-file, ~1x text).
    if (schema_.nullable()) {
        auto packed = PackNullBitmap(null_bitmap_, total_rows_);
        writer->WriteEntry(
            FMINDEX_NULL_BITMAP_FILE_NAME, packed.data(), packed.size());
    }

    writer->PutMeta(FMINDEX_META_TOTAL_ROWS, total_rows_);
    writer->PutMeta(FMINDEX_META_NULLABLE, schema_.nullable());

    LOG_INFO(
        "WriteEntries FM index done, field id: {}, total rows: {}, blob size: "
        "{}",
        field_id_,
        total_rows_,
        blob_size);
}

void
FMIndex::LoadEntries(storage::IndexEntryReader& reader, const Config& config) {
    if (!IsSupportedFMIndexDataType(schema_.data_type())) {
        // This schema came from persisted segment/index metadata, not from the
        // current query. A mismatch here is corrupt internal state and must not
        // be projected as a non-retriable caller InputError (DataTypeInvalid).
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "corrupt FM index: field {} has unsupported persisted data "
                  "type {}, expected String/VarChar",
                  field_id_,
                  schema_.data_type());
    }
    total_rows_ =
        ReadRequiredFMIndexMeta<int64_t>(reader, FMINDEX_META_TOTAL_ROWS);
    bool nullable =
        ReadRequiredFMIndexMeta<bool>(reader, FMINDEX_META_NULLABLE);
    if (total_rows_ < 0) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "corrupt FM index: total_rows is negative ({})",
                  total_rows_);
    }
    if (nullable != schema_.nullable()) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "corrupt FM index: persisted nullable={} does not match "
                  "field schema nullable={}",
                  nullable,
                  schema_.nullable());
    }
    if (!reader.HasEntry(FMINDEX_BLOB_FILE_NAME)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "corrupt FM index: blob entry '{}' is missing",
                  FMINDEX_BLOB_FILE_NAME);
    }

    // Load the FM-index blob. With mmap enabled (default), stream it to a local
    // file, map it, and view it in place (LoadView, zero-copy) — the FM-index
    // serves queries straight out of the mapping. Otherwise copy the bytes into
    // an owned buffer (Deserialize).
    is_mmap_ = GetValueFromConfig<bool>(config, ENABLE_MMAP).value_or(true);
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);

    if (is_mmap_ && disk_file_manager_ != nullptr) {
        mmap_filepath_ = disk_file_manager_->GetLocalIndexObjectPrefix() +
                         std::string("/") + FMINDEX_BLOB_FILE_NAME;
        CreateParentDirectories(mmap_filepath_);
        // Until mmap succeeds (below), the destructor does NOT own the staged
        // file, so remove it on any early throw — ReadEntryStream / FileWriter
        // Write/Finish / a failed mmap() — instead of leaking it. Dismissed once
        // the mapping succeeds, after which the destructor unmaps and unlinks.
        bool mmap_committed = false;
        auto stage_guard = folly::makeGuard([&]() {
            if (!mmap_committed) {
                std::error_code ec;
                std::filesystem::remove(mmap_filepath_, ec);
            }
        });
        size_t blob_size = 0;
        {
            auto file_writer = storage::FileWriter(
                mmap_filepath_,
                storage::io::GetPriorityFromLoadPriority(load_priority));
            reader.ReadEntryStream(FMINDEX_BLOB_FILE_NAME,
                                   [&](const uint8_t* data, size_t len) {
                                       file_writer.Write(data, len);
                                       blob_size += len;
                                   });
            std::vector<uint8_t> pad(kFMIndexMmapPadding, 0);
            file_writer.Write(pad.data(), pad.size());
            file_writer.Finish();
        }
        if (blob_size >
            std::numeric_limits<size_t>::max() - kFMIndexMmapPadding) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "corrupt FM index: blob size {} overflows mmap size",
                      blob_size);
        }
        mmap_size_ = blob_size + kFMIndexMmapPadding;
        int fd = open(mmap_filepath_.c_str(), O_RDONLY);
        if (fd < 0) {
            ThrowInfo(ErrorCode::FileOpenFailed,
                      "failed to open staged FM index file {}: {}",
                      mmap_filepath_,
                      strerror(errno));
        }
        auto fd_guard = folly::makeGuard([fd]() { close(fd); });
        mmap_data_ = static_cast<char*>(
            mmap(nullptr, mmap_size_, PROT_READ, MAP_PRIVATE, fd, 0));
        if (mmap_data_ == MAP_FAILED) {
            // stage_guard removes the staged file as the exception unwinds (mmap
            // never committed); strerror(errno) is read before that runs.
            ThrowInfo(ErrorCode::MmapError,
                      "failed to mmap FM index: {}",
                      strerror(errno));
        }
        // The move keeps the views pointing at mmap_data_ (the vector buffers
        // move by address); the mapping is unmapped in the destructor.
        fm_ = fmindex::FMIndex::LoadView(
            reinterpret_cast<const uint8_t*>(mmap_data_), blob_size);
        mmap_committed = true;  // destructor now owns the mapped file
    } else {
        // Move the reader entry buffer straight into the index's owned backing
        // store (owned_blob_ = std::move) — no intermediate std::string / copy,
        // so the non-mmap load peaks at ~1 blob + directories instead of ~3.
        auto blob_entry = reader.ReadEntry(FMINDEX_BLOB_FILE_NAME);
        fm_ = fmindex::FMIndex::Deserialize(std::move(blob_entry.data));
    }
    if (!fm_.valid()) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "corrupt FM index: failed structural validation of blob");
    }
    if (fm_.document_count() != static_cast<size_t>(total_rows_)) {
        ThrowInfo(
            ErrorCode::DataFormatBroken,
            "corrupt FM index: metadata total_rows={} does not match blob "
            "document count={}",
            total_rows_,
            fm_.document_count());
    }

    // Rebuild the null bitmap. A non-nullable field wrote no entry (no null rows
    // are possible), so its bitmap stays all-zeros. A nullable field wrote a
    // bit-packed bitmap of exactly ceil(total_rows/8) bytes; validate the size
    // (a short/corrupt entry would leave later reads out of bounds) and unpack
    // it. This is the sole carrier of null-ness after load — inside the FM blob
    // a null row and a genuine empty string are the same empty document.
    null_bitmap_ = TargetBitmap(total_rows_);
    if (nullable) {
        if (!reader.HasEntry(FMINDEX_NULL_BITMAP_FILE_NAME)) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "corrupt FM index: nullable field is missing null bitmap "
                      "entry");
        }
        auto null_entry = reader.ReadEntry(FMINDEX_NULL_BITMAP_FILE_NAME);
        size_t expected = static_cast<size_t>(total_rows_ / 8) +
                          static_cast<size_t>(total_rows_ % 8 != 0);
        if (null_entry.data.size() != expected) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "corrupt FM index: null bitmap entry is {} bytes, "
                      "expected {} (ceil(total_rows {} / 8))",
                      null_entry.data.size(),
                      expected,
                      total_rows_);
        }
        const auto* packed =
            reinterpret_cast<const uint8_t*>(null_entry.data.data());
        if (expected != 0 && total_rows_ % 8 != 0) {
            uint8_t valid_tail_mask =
                static_cast<uint8_t>((1u << (total_rows_ % 8)) - 1u);
            if ((packed[expected - 1] & ~valid_tail_mask) != 0) {
                ThrowInfo(ErrorCode::DataFormatBroken,
                          "corrupt FM index: null bitmap has set bits beyond "
                          "total_rows {}",
                          total_rows_);
            }
        }
        for (int64_t i = 0; i < total_rows_; i++) {
            if (packed[i >> 3] & (1u << (i & 0x07))) {
                null_bitmap_.set(i);
            }
        }
    }

    // Guard token total, derived from the loaded blob (bwt_size); needs fm_ valid
    // (asserted above), so compute it here at the end of the load path.
    ComputeTotalTokens();

    LOG_INFO("LoadEntries FM index done, field id: {}, total rows: {}",
             field_id_,
             total_rows_);
}

}  // namespace milvus::index
