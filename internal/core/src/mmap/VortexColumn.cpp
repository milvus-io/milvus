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

#include "mmap/VortexColumn.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>

#include <fmt/format.h>

#include "arrow/array.h"
#include "arrow/array/array_binary.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Utils.h"
#include "common/ChunkWriter.h"
#include "common/Common.h"
#include "common/EasyAssert.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/format/vortex/vortex_footer_reader.h"
#include "milvus-storage/format/vortex/vortex_translater.h"
#include "storage/Util.h"

namespace milvus {

namespace {

[[noreturn]] void
ThrowVortexStatus(const arrow::Status& status,
                  ErrorCode fallback_code,
                  std::string_view action) {
    auto code = fallback_code;
    // The Vortex bridge also uses IOError for decode failures, so the caller
    // owns the fallback classification instead of mapping IOError globally.
    if (status.IsOutOfMemory()) {
        code = ErrorCode::MemAllocateFailed;
    } else if (status.IsCancelled()) {
        code = ErrorCode::FollyCancel;
    }
    ThrowInfo(code, "{}: {}", action, status.ToString());
}

struct VortexArrowTypeCompatibility {
    bool compatible = false;
    bool direct_data_scan = false;
};

bool
IsArrowStringLikeType(arrow::Type::type type) {
    return type == arrow::Type::STRING || type == arrow::Type::BINARY ||
           type == arrow::Type::LARGE_STRING ||
           type == arrow::Type::LARGE_BINARY ||
           type == arrow::Type::STRING_VIEW || type == arrow::Type::BINARY_VIEW;
}

bool
IsArrowBinaryLikeType(arrow::Type::type type) {
    return type == arrow::Type::BINARY || type == arrow::Type::LARGE_BINARY ||
           type == arrow::Type::BINARY_VIEW;
}

bool
IsArrowArrayLikeType(arrow::Type::type type) {
    return type == arrow::Type::LIST || type == arrow::Type::LARGE_LIST ||
           type == arrow::Type::LIST_VIEW || type == arrow::Type::BINARY ||
           type == arrow::Type::LARGE_BINARY ||
           type == arrow::Type::BINARY_VIEW;
}

VortexArrowTypeCompatibility
GetVortexArrowTypeCompatibility(
    DataType data_type, const std::shared_ptr<arrow::DataType>& arrow_type) {
    if (arrow_type == nullptr) {
        return {};
    }

    if (ChunkedColumnInterface::IsPrimitiveDataType(data_type)) {
        if (arrow_type->Equals(*GetArrowDataType(data_type))) {
            return {true, true};
        }
        // The existing raw materialization path normalizes Arrow timestamps
        // to Milvus TIMESTAMPTZ's int64 microsecond representation. Keep that
        // compatibility, but do not expose an unnormalized timestamp buffer
        // through the zero-copy data scan.
        if (data_type == DataType::TIMESTAMPTZ &&
            arrow_type->id() == arrow::Type::TIMESTAMP) {
            return {true, false};
        }
        return {};
    }

    if (ChunkedColumnInterface::IsChunkedVariableColumnDataType(data_type)) {
        const auto compatible = IsArrowStringLikeType(arrow_type->id());
        // Geometry string encodings are WKT. The existing materialization
        // path converts them to Milvus's WKB representation, so only binary
        // geometry can be exposed directly by Scan.
        const auto direct_data_scan =
            data_type == DataType::GEOMETRY
                ? IsArrowBinaryLikeType(arrow_type->id())
                : compatible;
        return {compatible, direct_data_scan};
    }

    if (ChunkedColumnInterface::IsChunkedArrayColumnDataType(data_type)) {
        return {IsArrowArrayLikeType(arrow_type->id()), false};
    }

    return {};
}

void
ValidateVortexArrowArray(FieldId field_id,
                         DataType data_type,
                         bool nullable,
                         const std::shared_ptr<arrow::Array>& array,
                         bool require_direct_data_scan) {
    if (array == nullptr) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "vortex field {} returned a null Arrow array",
                  field_id.get());
    }
    const auto compatibility =
        GetVortexArrowTypeCompatibility(data_type, array->type());
    if (!compatibility.compatible ||
        (require_direct_data_scan && !compatibility.direct_data_scan)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "vortex field {} type {} is incompatible with Arrow {}",
                  field_id.get(),
                  data_type,
                  array->type()->ToString());
    }
    if (!nullable && array->null_count() > 0) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "non-nullable vortex field {} returned {} null rows",
                  field_id.get(),
                  array->null_count());
    }
}

void
ResetScanBatchOutput(ChunkedColumnInterface::ScanBatch* out) {
    out->values = ChunkedColumnInterface::ValueView{};
    out->validity = nullptr;
    out->row_ids.clear();
    out->owner.reset();
    out->row_id_start = 0;
    out->size = 0;
}

void
ResetRowIdPayloadOutput(ChunkedColumnInterface::ScanBatch* out) {
    ResetScanBatchOutput(out);
}

struct VortexReaderRange {
    int64_t chunk_id;
    int64_t local_offset;
    int64_t length;
    int64_t chunk_start;
    int64_t range_start;
    int64_t range_end;
};

struct VortexDataScanSource {
    VortexReaderRange range;
    PinWrapper<std::shared_ptr<arrow::RecordBatchReader>> reader;
};

struct VortexRowIdScanSource {
    VortexReaderRange range;
    PinWrapper<std::shared_ptr<arrow::RecordBatchReader>> matched_reader;
    std::optional<PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
        validity_reader;
};

using VortexCellPin = std::shared_ptr<
    cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>;

struct PreparedVortexScanSource {
    VortexReaderRange range;
    std::vector<uint64_t> cell_ids;
    VortexCellPin pin;
};

class CallbackPreparedScan final : public ChunkedColumnInterface::PreparedScan {
 public:
    using OpenFunc = std::function<ChunkedColumnInterface::ScanResult(
        const ChunkedColumnInterface::ScanPlan&,
        ChunkedColumnInterface::ScanProjection)>;

    CallbackPreparedScan(int64_t start, int64_t end, OpenFunc open)
        : start_(start),
          end_(end),
          plan_(ChunkedColumnInterface::ScanPlan::Full(start, end - start)),
          open_(std::move(open)) {
    }

    int64_t
    Start() const override {
        return start_;
    }

    int64_t
    End() const override {
        return end_;
    }

    const ChunkedColumnInterface::ScanPlan&
    Plan() const override {
        return plan_;
    }

    ChunkedColumnInterface::ScanResult
    Open(const ChunkedColumnInterface::ScanPlan& plan,
         ChunkedColumnInterface::ScanProjection projection) const override {
        AssertInfo(plan.requested_range.start >= start_ &&
                       plan.requested_range.start <= plan.requested_range.end &&
                       plan.requested_range.end <= end_,
                   "vortex scan range [{}, {}) outside prepared range [{}, {})",
                   plan.requested_range.start,
                   plan.requested_range.end,
                   start_,
                   end_);
        AssertInfo(
            plan.skip_ranges.empty(),
            "vortex callback scan does not yet accept external skip ranges");
        return open_(plan, projection);
    }

 private:
    int64_t start_;
    int64_t end_;
    ChunkedColumnInterface::ScanPlan plan_;
    OpenFunc open_;
};

std::vector<uint64_t>
MergeCellIds(const std::vector<uint64_t>& left,
             const std::vector<uint64_t>& right) {
    std::vector<uint64_t> cell_ids;
    cell_ids.reserve(left.size() + right.size());
    cell_ids.insert(cell_ids.end(), left.begin(), left.end());
    cell_ids.insert(cell_ids.end(), right.begin(), right.end());
    std::sort(cell_ids.begin(), cell_ids.end());
    cell_ids.erase(std::unique(cell_ids.begin(), cell_ids.end()),
                   cell_ids.end());
    return cell_ids;
}

void
AssertCellsCovered(const std::vector<uint64_t>& planned,
                   const std::vector<uint64_t>& pinned) {
    AssertInfo(std::all_of(planned.begin(),
                           planned.end(),
                           [&pinned](uint64_t cell_id) {
                               return std::find(pinned.begin(),
                                                pinned.end(),
                                                cell_id) != pinned.end();
                           }),
               "vortex scan seek requires cells outside its prepared pin");
}

VortexReaderRange
TrimReaderRange(const VortexReaderRange& range, int64_t position) {
    const auto range_start = std::max(range.range_start, position);
    AssertInfo(range_start < range.range_end,
               "cannot trim vortex range [{}, {}) at {}",
               range.range_start,
               range.range_end,
               position);
    const auto skipped = range_start - range.range_start;
    auto trimmed = range;
    trimmed.local_offset += skipped;
    trimmed.length -= skipped;
    trimmed.range_start = range_start;
    return trimmed;
}

std::optional<VortexReaderRange>
NextVortexReaderRange(const VortexColumn* column,
                      int64_t* scan_pos,
                      int64_t scan_end) {
    AssertInfo(column != nullptr, "vortex scan column is null");
    AssertInfo(scan_pos != nullptr, "vortex scan position is null");
    while (*scan_pos < scan_end) {
        auto [chunk_id, local_offset] = column->GetChunkIDByOffset(*scan_pos);
        const auto chunk_start =
            static_cast<int64_t>(column->GetNumRowsUntilChunk(chunk_id));
        const auto chunk_rows =
            static_cast<int64_t>(column->chunk_row_nums(chunk_id));
        const auto chunk_end = chunk_start + chunk_rows;
        const auto local_end =
            std::min<int64_t>(chunk_end, scan_end) - chunk_start;
        const auto length = local_end - static_cast<int64_t>(local_offset);
        if (length == 0) {
            *scan_pos = chunk_end;
            continue;
        }
        AssertInfo(length > 0,
                   "invalid vortex scan chunk range, chunk {}, offset {}, "
                   "end {}",
                   chunk_id,
                   local_offset,
                   local_end);
        return VortexReaderRange{
            static_cast<int64_t>(chunk_id),
            static_cast<int64_t>(local_offset),
            length,
            chunk_start,
            *scan_pos,
            chunk_start + local_end,
        };
    }
    return std::nullopt;
}

void
AppendRowIdPayloadEntry(ChunkedColumnInterface::ScanBatch* out,
                        FixedVector<bool>* validity,
                        bool* has_invalid,
                        int64_t row_id,
                        bool valid) {
    AssertInfo(validity != nullptr, "row id payload validity owner is null");
    AssertInfo(has_invalid != nullptr, "row id payload invalid flag is null");
    if (!out->row_ids.empty()) {
        AssertInfo(out->row_ids.back() <= row_id,
                   "row id payload is not ordered: {} before {}",
                   out->row_ids.back(),
                   row_id);
    }
    out->row_ids.emplace_back(row_id);
    validity->push_back(valid);
    *has_invalid = *has_invalid || !valid;
}

void
FinalizeRowIdPayloadOutput(ChunkedColumnInterface::ScanBatch* out,
                           std::shared_ptr<FixedVector<bool>> validity,
                           bool has_invalid) {
    AssertInfo(validity != nullptr, "row id payload validity owner is null");
    AssertInfo(validity->size() == out->row_ids.size(),
               "row id payload validity size {} does not match row ids size {}",
               validity->size(),
               out->row_ids.size());
    out->row_id_start = out->row_ids.empty() ? 0 : out->row_ids.front();
    out->size = static_cast<int64_t>(out->row_ids.size());
    if (out->row_ids.empty() || !has_invalid) {
        return;
    }
    out->validity = validity->data();
    out->owner = std::move(validity);
}

void
FinalizeAllValidRowIdPayloadOutput(ChunkedColumnInterface::ScanBatch* out) {
    out->row_id_start = out->row_ids.empty() ? 0 : out->row_ids.front();
    out->size = static_cast<int64_t>(out->row_ids.size());
}

}  // namespace

struct VortexColumn::ArrowTakeResult {
    std::shared_ptr<arrow::Table> table;
};

struct VortexColumn::ArrowStringViewHolder {
    std::vector<std::shared_ptr<
        cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>>
        pins;
    std::vector<std::shared_ptr<arrow::Table>> tables;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

struct VortexColumn::OrderedTakeOwner {
    std::vector<std::shared_ptr<arrow::Table>> tables;
    std::vector<std::shared_ptr<arrow::Array>> normalized_arrays;

    std::vector<int8_t> int8_values;
    std::vector<int16_t> int16_values;
    std::vector<int32_t> int32_values;
    std::vector<int64_t> int64_values;
    std::vector<float> float_values;
    std::vector<double> double_values;
    FixedVector<bool> bool_values;
    FixedVector<bool> validity;

    std::vector<std::string_view> string_views;
    std::vector<Json> json_values;
    std::vector<Array> arrays;
    std::vector<ArrayView> array_views;
};

class VortexOrderedTakeCursor final
    : public ChunkedColumnInterface::TakeCursor {
 public:
    VortexOrderedTakeCursor(ChunkedColumnInterface::ValueView values,
                            const bool* validity,
                            std::shared_ptr<void> owner,
                            int64_t size)
        : values_(values),
          validity_(validity),
          owner_(std::move(owner)),
          size_(size) {
    }

    int64_t
    Position() const override {
        return position_;
    }

    bool
    Next(int64_t max_rows, ChunkedColumnInterface::TakeBatch* out) override {
        AssertInfo(out != nullptr, "vortex take output batch is null");
        AssertInfo(max_rows > 0,
                   "vortex take max rows must be positive, got {}",
                   max_rows);
        out->values = ChunkedColumnInterface::ValueView{};
        out->selection = nullptr;
        out->validity = nullptr;
        out->owner.reset();
        out->position = 0;
        out->size = 0;
        out->source_chunk_id = -1;
        if (position_ >= size_) {
            return false;
        }

        const auto rows = std::min(max_rows, size_ - position_);
        out->values = values_;
        out->values.offset += position_;
        out->values.size = rows;
        out->validity = validity_ == nullptr ? nullptr : validity_ + position_;
        out->owner = owner_;
        out->position = position_;
        out->size = rows;
        position_ += rows;
        return true;
    }

 private:
    ChunkedColumnInterface::ValueView values_;
    const bool* validity_;
    std::shared_ptr<void> owner_;
    int64_t size_;
    int64_t position_{0};
};

class VortexColumn::ArrowStringLikeColumn {
 public:
    explicit ArrowStringLikeColumn(const std::shared_ptr<arrow::Table>& table) {
        AssertInfo(table != nullptr, "vortex take table is null");
        AssertInfo(table->num_columns() == 1,
                   "vortex string-like take expects one column, got {}",
                   table->num_columns());
        Init(table->column(0)->chunks());
    }

    explicit ArrowStringLikeColumn(const std::shared_ptr<arrow::Array>& array) {
        AssertInfo(array != nullptr, "vortex string-like array is null");
        Init({array});
    }

    int64_t
    length() const {
        return prefix_.empty() ? 0 : prefix_.back();
    }

    bool
    IsValid(int64_t row) const {
        auto [array, offset] = ArrayAt(row);
        return array->IsValid(offset);
    }

    std::string_view
    ValueAt(int64_t row) const {
        auto [array, offset] = ArrayAt(row);
        if (!array->IsValid(offset)) {
            return {};
        }

        switch (array->type_id()) {
            case arrow::Type::BINARY: {
                auto typed =
                    std::static_pointer_cast<arrow::BinaryArray>(array);
                auto value = typed->GetView(offset);
                return {value.data(), static_cast<size_t>(value.size())};
            }
            case arrow::Type::STRING: {
                auto typed =
                    std::static_pointer_cast<arrow::StringArray>(array);
                auto value = typed->GetView(offset);
                return {value.data(), static_cast<size_t>(value.size())};
            }
            case arrow::Type::LARGE_BINARY: {
                auto typed =
                    std::static_pointer_cast<arrow::LargeBinaryArray>(array);
                auto value = typed->GetView(offset);
                return {value.data(), static_cast<size_t>(value.size())};
            }
            case arrow::Type::LARGE_STRING: {
                auto typed =
                    std::static_pointer_cast<arrow::LargeStringArray>(array);
                auto value = typed->GetView(offset);
                return {value.data(), static_cast<size_t>(value.size())};
            }
            case arrow::Type::BINARY_VIEW: {
                auto typed =
                    std::static_pointer_cast<arrow::BinaryViewArray>(array);
                auto value = typed->GetView(offset);
                return {value.data(), static_cast<size_t>(value.size())};
            }
            case arrow::Type::STRING_VIEW: {
                auto typed =
                    std::static_pointer_cast<arrow::StringViewArray>(array);
                auto value = typed->GetView(offset);
                return {value.data(), static_cast<size_t>(value.size())};
            }
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "VortexColumn string-like take got unsupported "
                          "Arrow type {}",
                          array->type()->ToString());
                return {};
        }
    }

 private:
    void
    Init(arrow::ArrayVector chunks) {
        chunks_ = std::move(chunks);
        prefix_.reserve(chunks_.size() + 1);
        prefix_.push_back(0);
        int64_t rows = 0;
        for (const auto& chunk : chunks_) {
            rows += chunk->length();
            prefix_.push_back(rows);
        }
    }

    std::pair<std::shared_ptr<arrow::Array>, int64_t>
    ArrayAt(int64_t row) const {
        AssertInfo(row >= 0 && row < length(),
                   "vortex string-like row {} out of range {}",
                   row,
                   length());
        auto it = std::upper_bound(prefix_.begin(), prefix_.end(), row);
        auto chunk_idx =
            static_cast<size_t>(std::distance(prefix_.begin(), it) - 1);
        return {chunks_[chunk_idx], row - prefix_[chunk_idx]};
    }

    arrow::ArrayVector chunks_;
    std::vector<int64_t> prefix_;
};

class VortexRowIdScanCursor final : public ChunkedColumnInterface::ScanCursor {
 public:
    VortexRowIdScanCursor(const VortexColumn* column,
                          int64_t start_offset,
                          int64_t length,
                          std::vector<VortexRowIdScanSource>&& sources)
        : scan_pos_(start_offset), sources_(std::move(sources)) {
        AssertInfo(start_offset >= 0 && length >= 0 &&
                       start_offset + length <=
                           static_cast<int64_t>(column->NumRows()),
                   "vortex row id scan range [{}, {}) out of rows {}",
                   start_offset,
                   start_offset + length,
                   column->NumRows());
    }

    int64_t
    Position() const override {
        return scan_pos_;
    }

    bool
    Next(int64_t max_rows, ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "vortex row id scan output batch is null");
        AssertInfo(max_rows > 0,
                   "vortex row id scan max rows must be positive, got {}",
                   max_rows);
        ResetRowIdPayloadOutput(out);
        std::shared_ptr<FixedVector<bool>> validity;
        bool has_invalid = false;
        while ((out->row_ids.empty() || HasBufferedEntries()) &&
               static_cast<int64_t>(out->row_ids.size()) < max_rows) {
            if (!EnsureActiveReader()) {
                break;
            }
            if (!reader_may_contain_invalids_) {
                if (!AppendNextMatchedEntries(out, max_rows)) {
                    CloseActiveReader();
                }
                continue;
            }
            if (validity == nullptr) {
                validity = std::make_shared<FixedVector<bool>>();
            }
            if (!AppendNextEntry(out, validity.get(), &has_invalid)) {
                CloseActiveReader();
                continue;
            }
        }

        if (out->row_ids.empty()) {
            return false;
        }
        if (validity == nullptr) {
            FinalizeAllValidRowIdPayloadOutput(out);
            return true;
        }
        FinalizeRowIdPayloadOutput(out, std::move(validity), has_invalid);
        return true;
    }

 private:
    bool
    HasBufferedEntries() const {
        return matched_pos_ < matched_row_ids_.size() ||
               invalid_pos_ < invalid_row_ids_.size();
    }

    bool
    EnsureActiveReader() {
        if (reader_active_) {
            return true;
        }
        if (source_index_ >= sources_.size()) {
            return false;
        }

        const auto& source = sources_[source_index_++];
        const auto& range = source.range;
        AssertInfo(range.range_start == scan_pos_,
                   "vortex row id source starts at {}, expected {}",
                   range.range_start,
                   scan_pos_);

        reader_active_ = true;
        reader_chunk_start_ = range.chunk_start;
        reader_range_start_ = range.range_start;
        reader_range_end_ = range.range_end;
        invalid_reader_next_row_id_ = reader_range_start_;
        matched_row_ids_.clear();
        invalid_row_ids_.clear();
        matched_pos_ = 0;
        invalid_pos_ = 0;
        reader_may_contain_invalids_ = source.validity_reader.has_value();
        matched_reader_ = source.matched_reader;
        invalid_reader_ = source.validity_reader;
        scan_pos_ = reader_range_end_;
        return true;
    }

    void
    CloseActiveReader() {
        reader_active_ = false;
        matched_reader_.reset();
        invalid_reader_.reset();
        matched_row_ids_.clear();
        invalid_row_ids_.clear();
        matched_pos_ = 0;
        invalid_pos_ = 0;
        reader_may_contain_invalids_ = false;
    }

    bool
    EnsureMatchedEntry() {
        while (matched_pos_ >= matched_row_ids_.size() &&
               matched_reader_.has_value()) {
            matched_row_ids_.clear();
            matched_pos_ = 0;

            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = matched_reader_->get()->ReadNext(&batch);
            if (!status.ok()) {
                ThrowVortexStatus(status,
                                  ErrorCode::DataFormatBroken,
                                  "failed to read vortex row id scan batch");
            }
            if (batch == nullptr) {
                matched_reader_.reset();
                break;
            }
            FillMatchedRowIdsFromBatch(batch);
        }
        return matched_pos_ < matched_row_ids_.size();
    }

    bool
    EnsureInvalidEntry(std::optional<int64_t> row_id_limit) {
        // Keep the validity side-stream only as far ahead as needed to merge
        // the next matched row id in order.
        while (invalid_pos_ >= invalid_row_ids_.size()) {
            if (!invalid_reader_.has_value()) {
                break;
            }
            if (row_id_limit.has_value() &&
                invalid_reader_next_row_id_ > row_id_limit.value()) {
                break;
            }
            invalid_row_ids_.clear();
            invalid_pos_ = 0;

            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = invalid_reader_->get()->ReadNext(&batch);
            if (!status.ok()) {
                ThrowVortexStatus(
                    status,
                    ErrorCode::DataFormatBroken,
                    "failed to read vortex row id validity batch");
            }
            if (batch == nullptr) {
                AssertInfo(invalid_reader_next_row_id_ == reader_range_end_,
                           "vortex row id validity scan ended after row {}, "
                           "expected {}",
                           invalid_reader_next_row_id_,
                           reader_range_end_);
                invalid_reader_.reset();
                break;
            }
            FillInvalidRowIdsFromBatch(batch);
        }
        return invalid_pos_ < invalid_row_ids_.size();
    }

    bool
    AppendNextMatchedEntries(ChunkedColumnInterface::ScanBatch* out,
                             int64_t max_rows) {
        if (!EnsureMatchedEntry()) {
            return false;
        }

        const auto remaining = static_cast<size_t>(
            max_rows - static_cast<int64_t>(out->row_ids.size()));
        const auto rows_to_append =
            std::min(matched_row_ids_.size() - matched_pos_, remaining);
        if (!out->row_ids.empty()) {
            AssertInfo(out->row_ids.back() <= matched_row_ids_[matched_pos_],
                       "row id payload is not ordered: {} before {}",
                       out->row_ids.back(),
                       matched_row_ids_[matched_pos_]);
        }
        out->row_ids.insert(
            out->row_ids.end(),
            matched_row_ids_.begin() + static_cast<int64_t>(matched_pos_),
            matched_row_ids_.begin() +
                static_cast<int64_t>(matched_pos_ + rows_to_append));
        matched_pos_ += rows_to_append;
        return true;
    }

    bool
    AppendNextEntry(ChunkedColumnInterface::ScanBatch* out,
                    FixedVector<bool>* validity,
                    bool* has_invalid) {
        const auto has_matched = EnsureMatchedEntry();
        const auto has_invalid_entry =
            has_matched ? EnsureInvalidEntry(matched_row_ids_[matched_pos_])
                        : EnsureInvalidEntry(std::nullopt);
        if (!has_matched && !has_invalid_entry) {
            return false;
        }

        if (has_matched &&
            (!has_invalid_entry ||
             matched_row_ids_[matched_pos_] < invalid_row_ids_[invalid_pos_])) {
            AppendRowIdPayloadEntry(out,
                                    validity,
                                    has_invalid,
                                    matched_row_ids_[matched_pos_++],
                                    true);
            return true;
        }

        if (has_matched &&
            matched_row_ids_[matched_pos_] == invalid_row_ids_[invalid_pos_]) {
            ++matched_pos_;
        }
        AppendRowIdPayloadEntry(out,
                                validity,
                                has_invalid,
                                invalid_row_ids_[invalid_pos_++],
                                false);
        return true;
    }

    void
    FillMatchedRowIdsFromBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch) {
        AssertInfo(batch != nullptr, "vortex row id scan batch is null");
        AssertInfo(batch->num_columns() == 1,
                   "vortex row id scan expects one column, got {}",
                   batch->num_columns());
        auto column = batch->column(0);
        AssertInfo(column->null_count() == 0,
                   "vortex row id scan returned nullable row id column");
        matched_row_ids_.reserve(batch->num_rows());
        if (auto uint64_column =
                std::dynamic_pointer_cast<arrow::UInt64Array>(column)) {
            for (int64_t i = 0; i < uint64_column->length(); ++i) {
                AppendMatchedRowId(
                    reader_chunk_start_,
                    static_cast<int64_t>(uint64_column->Value(i)),
                    reader_range_start_,
                    reader_range_end_,
                    &matched_row_ids_);
            }
            return;
        }
        if (auto int64_column =
                std::dynamic_pointer_cast<arrow::Int64Array>(column)) {
            for (int64_t i = 0; i < int64_column->length(); ++i) {
                AppendMatchedRowId(reader_chunk_start_,
                                   int64_column->Value(i),
                                   reader_range_start_,
                                   reader_range_end_,
                                   &matched_row_ids_);
            }
            return;
        }
        ThrowInfo(ErrorCode::UnexpectedError,
                  "vortex row id scan expects UInt64 or Int64 column, got {}",
                  column->type()->ToString());
    }

    void
    FillInvalidRowIdsFromBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch) {
        AssertInfo(batch != nullptr,
                   "vortex row id validity scan batch is null");
        AssertInfo(batch->num_columns() == 1,
                   "vortex row id validity scan expects one column, got {}",
                   batch->num_columns());
        auto array = batch->column(0);
        AssertInfo(array->length() > 0,
                   "vortex row id validity scan returned empty batch");
        AssertInfo(
            invalid_reader_next_row_id_ + array->length() <= reader_range_end_,
            "vortex row id validity scan returned too many rows");
        invalid_row_ids_.reserve(array->length());
        for (int64_t i = 0; i < array->length(); ++i) {
            if (!array->IsValid(i)) {
                invalid_row_ids_.emplace_back(invalid_reader_next_row_id_ + i);
            }
        }
        invalid_reader_next_row_id_ += array->length();
    }

    void
    AppendMatchedRowId(int64_t chunk_start,
                       int64_t chunk_local_row_id,
                       int64_t range_start,
                       int64_t range_end,
                       std::vector<int64_t>* row_ids) const {
        const auto row_id = chunk_start + chunk_local_row_id;
        AssertInfo(row_id >= range_start && row_id < range_end,
                   "vortex row id {} outside scan range [{}, {})",
                   row_id,
                   range_start,
                   range_end);
        if (!row_ids->empty()) {
            AssertInfo(row_ids->back() <= row_id,
                       "vortex row ids are not ordered: {} before {}",
                       row_ids->back(),
                       row_id);
        }
        row_ids->emplace_back(row_id);
    }

    int64_t scan_pos_;
    std::vector<VortexRowIdScanSource> sources_;
    size_t source_index_{0};
    bool reader_active_{false};
    bool reader_may_contain_invalids_{false};
    int64_t reader_chunk_start_{0};
    int64_t reader_range_start_{0};
    int64_t reader_range_end_{0};
    std::optional<PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
        matched_reader_;
    std::optional<PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
        invalid_reader_;
    int64_t invalid_reader_next_row_id_{0};
    std::vector<int64_t> matched_row_ids_;
    std::vector<int64_t> invalid_row_ids_;
    size_t matched_pos_{0};
    size_t invalid_pos_{0};
};

class VortexDataScanCursor final : public ChunkedColumnInterface::ScanCursor {
 public:
    VortexDataScanCursor(const VortexColumn* column,
                         int64_t start_offset,
                         int64_t length,
                         ChunkedColumnInterface::ScanProjection projection,
                         ChunkedColumnInterface::ScanValueKind value_kind,
                         std::vector<VortexDataScanSource>&& sources)
        : column_(column),
          projection_(projection),
          value_kind_(value_kind),
          scan_pos_(start_offset),
          scan_end_(start_offset + length),
          sources_(std::move(sources)) {
        AssertInfo(start_offset >= 0 && length >= 0 &&
                       start_offset + length <=
                           static_cast<int64_t>(column_->NumRows()),
                   "vortex data scan range [{}, {}) out of rows {}",
                   start_offset,
                   start_offset + length,
                   column_->NumRows());
    }

    int64_t
    Position() const override {
        return scan_pos_;
    }

    bool
    Next(int64_t max_rows, ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "vortex data scan output batch is null");
        AssertInfo(max_rows > 0,
                   "vortex data scan max rows must be positive, got {}",
                   max_rows);
        ResetScanBatchOutput(out);
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        while (scan_pos_ < scan_end_) {
            if (current_batch_ != nullptr &&
                current_batch_pos_ < current_batch_->num_rows()) {
                const auto rows_to_return = std::min<int64_t>(
                    {current_batch_->num_rows() - current_batch_pos_,
                     scan_end_ - scan_pos_,
                     max_rows});
                FillOutputFromCurrentBatch(rows_to_return, out);
                scan_pos_ += rows_to_return;
                current_batch_pos_ += rows_to_return;
                return true;
            }

            current_batch_.reset();
            current_batch_pos_ = 0;
            if (reader_.has_value()) {
                std::shared_ptr<arrow::RecordBatch> batch;
                auto status = reader_->get()->ReadNext(&batch);
                if (!status.ok()) {
                    ThrowVortexStatus(status,
                                      ErrorCode::DataFormatBroken,
                                      "failed to read vortex data scan batch");
                }
                if (batch != nullptr) {
                    AssertInfo(batch->num_columns() == 1,
                               "vortex data scan expects one column, got {}",
                               batch->num_columns());
                    AssertInfo(next_reader_row_id_ + batch->num_rows() <=
                                   reader_range_end_,
                               "vortex data scan returned rows through {}, "
                               "beyond planned end {}",
                               next_reader_row_id_ + batch->num_rows(),
                               reader_range_end_);
                    current_batch_ = std::move(batch);
                    current_batch_row_id_start_ = next_reader_row_id_;
                    next_reader_row_id_ += current_batch_->num_rows();
                    continue;
                }
                AssertInfo(next_reader_row_id_ == reader_range_end_,
                           "vortex data scan ended after row {}, expected {}",
                           next_reader_row_id_,
                           reader_range_end_);
                reader_.reset();
                continue;
            }

            if (!OpenNextReader()) {
                break;
            }
        }
        return false;
    }

 private:
    bool
    OpenNextReader() {
        if (source_index_ >= sources_.size()) {
            return false;
        }
        const auto& source = sources_[source_index_++];
        AssertInfo(source.range.range_start == scan_pos_,
                   "vortex data source starts at {}, expected {}",
                   source.range.range_start,
                   scan_pos_);
        reader_ = source.reader;
        next_reader_row_id_ = source.range.range_start;
        reader_range_end_ = source.range.range_end;
        return true;
    }

    template <typename ArrowArrayT>
    const void*
    RawPrimitiveValues(const std::shared_ptr<arrow::Array>& array) const {
        auto typed = std::dynamic_pointer_cast<ArrowArrayT>(array);
        if (typed == nullptr) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "vortex data scan field {} expected Arrow {}, got {}",
                      column_->field_id_.get(),
                      GetArrowDataType(column_->data_type_)->ToString(),
                      array == nullptr ? "<null>" : array->type()->ToString());
        }
        return typed->raw_values();
    }

    struct BatchOwner {
        std::optional<PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
            reader;
        std::shared_ptr<arrow::Array> array;
        std::shared_ptr<FixedVector<bool>> bool_values;
        std::shared_ptr<FixedVector<bool>> validity;
        std::vector<std::string_view> string_views;
        std::vector<Json> json_values;
    };

    bool
    IsStringLikeScan() const {
        return value_kind_ ==
                   ChunkedColumnInterface::ScanValueKind::StringView ||
               value_kind_ == ChunkedColumnInterface::ScanValueKind::JsonView;
    }

    void
    FillDataPointer(const std::shared_ptr<arrow::Array>& array,
                    const std::shared_ptr<BatchOwner>& owner,
                    ChunkedColumnInterface::ScanBatch* out) const {
        out->values.encoding =
            ChunkedColumnInterface::ValueEncoding::FixedWidth;
        out->values.kind =
            value_kind_ == ChunkedColumnInterface::ScanValueKind::Default
                ? ChunkedColumnInterface::ScanValueKind::FixedWidth
                : value_kind_;
        out->values.physical_type = column_->data_type_;
        out->values.logical_type = column_->data_type_;
        out->values.size = out->size;
        switch (column_->data_type_) {
            case DataType::INT8:
                out->values.data = RawPrimitiveValues<arrow::Int8Array>(array);
                out->values.byte_width = sizeof(int8_t);
                break;
            case DataType::INT16:
                out->values.data = RawPrimitiveValues<arrow::Int16Array>(array);
                out->values.byte_width = sizeof(int16_t);
                break;
            case DataType::INT32:
                out->values.data = RawPrimitiveValues<arrow::Int32Array>(array);
                out->values.byte_width = sizeof(int32_t);
                break;
            case DataType::INT64:
            case DataType::TIMESTAMPTZ:
                out->values.data = RawPrimitiveValues<arrow::Int64Array>(array);
                out->values.byte_width = sizeof(int64_t);
                break;
            case DataType::FLOAT:
                out->values.data = RawPrimitiveValues<arrow::FloatArray>(array);
                out->values.byte_width = sizeof(float);
                break;
            case DataType::DOUBLE:
                out->values.data =
                    RawPrimitiveValues<arrow::DoubleArray>(array);
                out->values.byte_width = sizeof(double);
                break;
            case DataType::BOOL: {
                auto typed =
                    std::dynamic_pointer_cast<arrow::BooleanArray>(array);
                if (typed == nullptr) {
                    ThrowInfo(
                        ErrorCode::DataFormatBroken,
                        "vortex data scan field {} expected Arrow bool, got {}",
                        column_->field_id_.get(),
                        array == nullptr ? "<null>"
                                         : array->type()->ToString());
                }
                owner->bool_values = std::make_shared<FixedVector<bool>>();
                owner->bool_values->resize(array->length());
                for (int64_t i = 0; i < array->length(); ++i) {
                    (*owner->bool_values)[i] = typed->Value(i);
                }
                out->values.data = owner->bool_values->data();
                out->values.byte_width = sizeof(bool);
                break;
            }
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "unsupported vortex data scan type {}",
                          column_->data_type_);
        }
    }

    void
    FillValidityPointer(const std::shared_ptr<arrow::Array>& array,
                        const std::shared_ptr<BatchOwner>& owner,
                        ChunkedColumnInterface::ScanBatch* out) const {
        if (!column_->IsNullable() || array->null_count() == 0) {
            return;
        }
        owner->validity = std::make_shared<FixedVector<bool>>();
        owner->validity->resize(out->size);
        bool has_invalid = false;
        for (int64_t i = 0; i < out->size; ++i) {
            const auto valid = array->IsValid(current_batch_pos_ + i);
            (*owner->validity)[i] = valid;
            has_invalid = has_invalid || !valid;
        }
        if (!has_invalid) {
            owner->validity.reset();
            return;
        }
        out->validity = owner->validity->data();
    }

    void
    FillStringLikeOutput(const std::shared_ptr<arrow::Array>& array,
                         const std::shared_ptr<BatchOwner>& owner,
                         ChunkedColumnInterface::ScanBatch* out) const {
        if (projection_ == ChunkedColumnInterface::ScanProjection::NoData) {
            FillValidityPointer(array, owner, out);
            return;
        }

        VortexColumn::ArrowStringLikeColumn string_column(array);
        const bool emit_valid =
            column_->field_meta_.is_nullable() && array->null_count() > 0;
        auto views = column_->BuildStringViewsFromArrow(
            string_column,
            std::make_pair(current_batch_pos_, out->size),
            emit_valid);

        out->values.physical_type = column_->data_type_;
        out->values.logical_type = column_->data_type_;
        out->values.offset = 0;
        out->values.size = out->size;

        if (value_kind_ == ChunkedColumnInterface::ScanValueKind::StringView) {
            owner->string_views = std::move(views.first);
            out->values.encoding =
                ChunkedColumnInterface::ValueEncoding::StringView;
            out->values.kind =
                ChunkedColumnInterface::ScanValueKind::StringView;
            out->values.data = owner->string_views.data();
            out->values.byte_width = sizeof(std::string_view);
        } else {
            owner->string_views = std::move(views.first);
            owner->json_values.reserve(owner->string_views.size());
            for (const auto& value : owner->string_views) {
                owner->json_values.emplace_back(Json(value));
            }
            out->values.encoding =
                ChunkedColumnInterface::ValueEncoding::JsonView;
            out->values.kind = ChunkedColumnInterface::ScanValueKind::JsonView;
            out->values.logical_type = DataType::JSON;
            out->values.data = owner->json_values.data();
            out->values.byte_width = sizeof(Json);
        }

        if (emit_valid) {
            owner->validity =
                std::make_shared<FixedVector<bool>>(std::move(views.second));
            if (std::any_of(owner->validity->begin(),
                            owner->validity->end(),
                            [](bool valid) { return !valid; })) {
                out->validity = owner->validity->data();
            } else {
                owner->validity.reset();
            }
        }
    }

    void
    FillOutputFromCurrentBatch(int64_t rows_to_return,
                               ChunkedColumnInterface::ScanBatch* out) const {
        auto array = current_batch_->column(0);
        ValidateVortexArrowArray(
            column_->field_id_,
            column_->data_type_,
            column_->IsNullable(),
            array,
            projection_ != ChunkedColumnInterface::ScanProjection::NoData);
        auto owner = std::make_shared<BatchOwner>();
        AssertInfo(reader_.has_value(),
                   "vortex data scan batch requires active reader pin");
        owner->reader = *reader_;
        owner->array = array;
        out->row_id_start = current_batch_row_id_start_ + current_batch_pos_;
        out->size = rows_to_return;
        if (IsStringLikeScan()) {
            FillStringLikeOutput(array, owner, out);
        } else {
            if (projection_ != ChunkedColumnInterface::ScanProjection::NoData) {
                FillDataPointer(array, owner, out);
                out->values.offset = current_batch_pos_;
            }
            FillValidityPointer(array, owner, out);
        }
        out->owner = std::move(owner);
    }

    const VortexColumn* column_;
    ChunkedColumnInterface::ScanProjection projection_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    int64_t scan_pos_;
    int64_t scan_end_;
    std::vector<VortexDataScanSource> sources_;
    size_t source_index_{0};
    std::optional<PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
        reader_;
    int64_t next_reader_row_id_{0};
    int64_t reader_range_end_{0};
    std::shared_ptr<arrow::RecordBatch> current_batch_;
    int64_t current_batch_pos_{0};
    int64_t current_batch_row_id_start_{0};
};

VortexColumn::VortexColumn(
    FieldId field_id,
    FieldMeta field_meta,
    std::shared_ptr<milvus_storage::api::Properties> properties,
    std::shared_ptr<VortexColumnGroup> column_group,
    std::optional<size_t> data_byte_size)
    : field_id_(field_id),
      field_meta_(std::move(field_meta)),
      data_type_(field_meta_.get_data_type()),
      field_name_(field_meta_.is_external_field()
                      ? field_meta_.get_external_field()
                      : std::to_string(field_id_.get())) {
    AssertInfo(!IsVectorDataType(data_type_),
               "vortex local_format does not support vector field {}",
               field_id_.get());
    AssertInfo(properties != nullptr, "vortex properties is null");
    AssertInfo(column_group != nullptr, "vortex column group is null");

    local_format_properties_ = MakeVortexReaderProperties(properties);
    column_group_ = std::move(column_group);
    data_byte_size_ = data_byte_size.value_or(column_group_->memory_size());

    const auto& group_files = column_group_->files();
    files_.reserve(group_files.size());
    for (size_t file_index = 0; file_index < group_files.size(); ++file_index) {
        files_.emplace_back(
            BuildFileState(file_index, group_files[file_index]));
    }

    num_rows_until_chunk_ = column_group_->num_rows_until_chunk();
    num_rows_ = column_group_->num_rows();
}

VortexColumn::~VortexColumn() = default;

void
VortexColumn::ManualEvictCache() const {
    if (!IsInMultiFieldColumnGroup()) {
        column_group_->ManualEvictCache();
    }
}

void
VortexColumn::CancelWarmup() {
    if (!IsInMultiFieldColumnGroup()) {
        column_group_->CancelWarmup();
    }
}

bool
VortexColumn::IsInMultiFieldColumnGroup() const {
    return column_group_->num_fields() > 1;
}

bool
VortexColumn::IsNullable() const {
    return field_meta_.is_nullable();
}

size_t
VortexColumn::NumRows() const {
    return num_rows_;
}

int64_t
VortexColumn::num_chunks() const {
    return static_cast<int64_t>(files_.size());
}

size_t
VortexColumn::DataByteSize() const {
    return data_byte_size_;
}

int64_t
VortexColumn::chunk_row_nums(int64_t chunk_id) const {
    CheckChunkId(chunk_id);
    return files_[chunk_id].rows;
}

PinWrapper<const char*>
VortexColumn::DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const {
    auto chunk = MaterializeChunk(op_ctx, chunk_id);
    return PinWrapper<const char*>(chunk, chunk->Data());
}

bool
VortexColumn::IsValid(milvus::OpContext* op_ctx, size_t offset) const {
    if (!field_meta_.is_nullable()) {
        return true;
    }
    auto [chunk_id, chunk_offset] =
        GetChunkIDByOffset(static_cast<int64_t>(offset));
    auto chunk = MaterializeChunk(op_ctx, chunk_id);
    return chunk->isValid(static_cast<int>(chunk_offset));
}

void
VortexColumn::BulkIsValid(milvus::OpContext* op_ctx,
                          std::function<void(bool, size_t)> fn,
                          const int64_t* offsets,
                          int64_t count) const {
    if (!field_meta_.is_nullable()) {
        if (offsets == nullptr) {
            for (int64_t i = 0; i < num_rows_; ++i) {
                fn(true, i);
            }
        } else {
            for (int64_t i = 0; i < count; ++i) {
                fn(true, i);
            }
        }
        return;
    }

    if (offsets == nullptr) {
        int64_t logical_offset = 0;
        for (int64_t chunk_id = 0; chunk_id < num_chunks(); ++chunk_id) {
            auto chunk = MaterializeChunk(op_ctx, chunk_id);
            for (int64_t i = 0; i < chunk->RowNums(); ++i) {
                fn(chunk->isValid(static_cast<int>(i)), logical_offset + i);
            }
            logical_offset += chunk->RowNums();
        }
        return;
    }

    auto [chunk_ids, offsets_in_chunk] = GetChunkIDsByOffsets(offsets, count);
    std::unordered_map<int64_t, std::vector<int64_t>> indices_by_chunk;
    indices_by_chunk.reserve(chunk_ids.size());
    for (int64_t i = 0; i < count; ++i) {
        indices_by_chunk[chunk_ids[i]].emplace_back(i);
    }

    for (const auto& [chunk_id, indices] : indices_by_chunk) {
        auto chunk = MaterializeChunk(op_ctx, chunk_id);
        for (const auto index : indices) {
            fn(chunk->isValid(static_cast<int>(offsets_in_chunk[index])),
               index);
        }
    }
}

void
VortexColumn::PrefetchChunks(milvus::OpContext* op_ctx,
                             const std::vector<int64_t>& chunk_ids) const {
    for (auto chunk_id : chunk_ids) {
        CheckChunkId(chunk_id);
        const auto& file = files_[chunk_id];
        std::vector<uint64_t> cell_ids;
        cell_ids.reserve(file.planner->num_cells());
        for (size_t cell_id = 0; cell_id < file.planner->num_cells();
             ++cell_id) {
            cell_ids.emplace_back(cell_id);
        }
        column_group_->PinCells(op_ctx, chunk_id, cell_ids);
    }
}

bool
VortexColumn::CellsLoaded(const int64_t* offsets, int64_t count) const {
    if (count == 0) {
        return true;
    }
    AssertInfo(offsets != nullptr,
               "vortex cache check requires explicit row offsets");

    std::unordered_map<int64_t, std::vector<int64_t>> offsets_by_chunk;
    for (int64_t i = 0; i < count; ++i) {
        auto [chunk_id, chunk_offset] = GetChunkIDByOffset(offsets[i]);
        offsets_by_chunk[static_cast<int64_t>(chunk_id)].emplace_back(
            static_cast<int64_t>(chunk_offset));
    }

    for (auto& [chunk_id, chunk_offsets] : offsets_by_chunk) {
        std::sort(chunk_offsets.begin(), chunk_offsets.end());
        chunk_offsets.erase(
            std::unique(chunk_offsets.begin(), chunk_offsets.end()),
            chunk_offsets.end());

        const auto& file = files_[chunk_id];
        auto plan = PlanOffsets(file, chunk_offsets);
        if (!column_group_->CellsLoaded(chunk_id, plan.cell_ids)) {
            return false;
        }
    }
    return true;
}

PinWrapper<SpanBase>
VortexColumn::Span(milvus::OpContext* op_ctx, int64_t chunk_id) const {
    if (!IsChunkedColumnDataType(data_type_)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::Span only supports fixed-width scalar "
                  "fields");
    }
    auto chunk = MaterializeChunk(op_ctx, chunk_id);
    auto span = static_cast<FixedWidthChunk*>(chunk.get())->Span();
    return PinWrapper<SpanBase>(chunk, span);
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
VortexColumn::StringViews(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    if (!IsChunkedVariableColumnDataType(data_type_)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::StringViews only supports variable fields");
    }
    if (!SupportsDirectDataScan()) {
        auto chunk = MaterializeChunk(op_ctx, chunk_id, offset_len);
        auto views =
            static_cast<StringChunk*>(chunk.get())->StringViews(std::nullopt);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            chunk, std::move(views));
    }
    auto [holder, views] =
        ScanStringLikeViewsFromFile(op_ctx, chunk_id, offset_len);
    return PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
        std::move(holder), std::move(views));
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
VortexColumn::ArrayViews(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    if (!IsChunkedArrayColumnDataType(data_type_)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::ArrayViews only supports array fields");
    }
    auto chunk = MaterializeChunk(op_ctx, chunk_id, offset_len);
    auto views = static_cast<ArrayChunk*>(chunk.get())->Views({});
    return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
        chunk, std::move(views));
}

PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
VortexColumn::VectorArrayViews(
    milvus::OpContext*,
    int64_t,
    std::optional<std::pair<int64_t, int64_t>>) const {
    ThrowInfo(ErrorCode::Unsupported,
              "VortexColumn does not support vector array fields");
}

PinWrapper<const size_t*>
VortexColumn::VectorArrayOffsets(milvus::OpContext*, int64_t) const {
    ThrowInfo(ErrorCode::Unsupported,
              "VortexColumn does not support vector array fields");
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
VortexColumn::StringViewsByOffsets(milvus::OpContext* op_ctx,
                                   int64_t chunk_id,
                                   const FixedVector<int32_t>& offsets) const {
    if (!IsChunkedVariableColumnDataType(data_type_)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::StringViewsByOffsets only supports "
                  "variable fields");
    }
    CheckChunkId(chunk_id);
    std::pair<std::vector<std::string_view>, FixedVector<bool>> views;
    views.first.resize(offsets.size());
    views.second.resize(offsets.size());
    if (offsets.empty()) {
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            std::move(views));
    }

    std::vector<int64_t> global_offsets(offsets.size());
    const auto chunk_start = GetNumRowsUntilChunk(chunk_id);
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto offset = offsets[i];
        AssertInfo(offset >= 0 && offset < files_[chunk_id].rows,
                   "vortex chunk-local offset {} out of chunk {} rows {}",
                   offset,
                   chunk_id,
                   files_[chunk_id].rows);
        global_offsets[i] = chunk_start + offset;
    }

    const auto value_kind = data_type_ == DataType::JSON
                                ? ScanValueKind::JsonView
                                : ScanValueKind::StringView;
    auto cursor = Take(op_ctx,
                       TakeOptions{OffsetView::From(global_offsets.data(),
                                                    global_offsets.size()),
                                   value_kind});
    AssertInfo(cursor != nullptr,
               "vortex string view take is unsupported for type {}",
               data_type_);
    TakeBatch batch;
    AssertInfo(cursor->Next(offsets.size(), &batch),
               "vortex string view take returned no data");
    AssertInfo(batch.size == static_cast<int64_t>(offsets.size()),
               "vortex string view take returned {} rows, expected {}",
               batch.size,
               offsets.size());
    if (data_type_ == DataType::JSON) {
        const auto* data = batch.values.data_as<Json>();
        for (size_t i = 0; i < offsets.size(); ++i) {
            views.first[i] = data[i];
            views.second[i] = batch.validity == nullptr || batch.validity[i];
        }
    } else {
        const auto* data = batch.values.data_as<std::string_view>();
        for (size_t i = 0; i < offsets.size(); ++i) {
            views.first[i] = data[i];
            views.second[i] = batch.validity == nullptr || batch.validity[i];
        }
    }
    return PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
        std::move(batch.owner), std::move(views));
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
VortexColumn::ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                                  int64_t chunk_id,
                                  const FixedVector<int32_t>& offsets) const {
    if (!IsChunkedArrayColumnDataType(data_type_)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::ArrayViewsByOffsets only supports array "
                  "fields");
    }
    CheckChunkId(chunk_id);
    std::pair<std::vector<ArrayView>, FixedVector<bool>> views;
    views.first.resize(offsets.size());
    views.second.resize(offsets.size());
    if (offsets.empty()) {
        return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
            std::move(views));
    }

    std::vector<int64_t> global_offsets(offsets.size());
    const auto chunk_start = GetNumRowsUntilChunk(chunk_id);
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto offset = offsets[i];
        AssertInfo(offset >= 0 && offset < files_[chunk_id].rows,
                   "vortex chunk-local offset {} out of chunk {} rows {}",
                   offset,
                   chunk_id,
                   files_[chunk_id].rows);
        global_offsets[i] = chunk_start + offset;
    }

    auto cursor = Take(op_ctx,
                       TakeOptions{OffsetView::From(global_offsets.data(),
                                                    global_offsets.size()),
                                   ScanValueKind::ArrayView});
    AssertInfo(cursor != nullptr, "vortex array view take is unsupported");
    TakeBatch batch;
    AssertInfo(cursor->Next(offsets.size(), &batch),
               "vortex array view take returned no data");
    AssertInfo(batch.size == static_cast<int64_t>(offsets.size()),
               "vortex array view take returned {} rows, expected {}",
               batch.size,
               offsets.size());
    const auto* data = batch.values.data_as<ArrayView>();
    for (size_t i = 0; i < offsets.size(); ++i) {
        views.first[i] = data[i];
        views.second[i] = batch.validity == nullptr || batch.validity[i];
    }
    return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
        std::move(batch.owner), std::move(views));
}

std::pair<size_t, size_t>
VortexColumn::GetChunkIDByOffset(int64_t offset) const {
    AssertInfo(offset >= 0 && offset < num_rows_,
               "offset {} is out of range, num_rows: {}",
               offset,
               num_rows_);
    return ::milvus::GetChunkIDByOffset(offset, num_rows_until_chunk_);
}

std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
VortexColumn::GetChunkIDsByOffsets(const int64_t* offsets,
                                   int64_t count) const {
    return ::milvus::GetChunkIDsByOffsets(
        offsets, count, num_rows_until_chunk_);
}

PinWrapper<Chunk*>
VortexColumn::GetChunk(milvus::OpContext*, int64_t) const {
    ThrowInfo(ErrorCode::Unsupported,
              "VortexColumn::GetChunk is disabled because it "
              "materializes Vortex data; use column view/bulk APIs instead");
}

std::vector<PinWrapper<Chunk*>>
VortexColumn::PinChunks(milvus::OpContext* op_ctx,
                        const std::vector<int64_t>& chunk_ids) const {
    std::vector<PinWrapper<Chunk*>> chunks;
    chunks.reserve(chunk_ids.size());
    for (auto chunk_id : chunk_ids) {
        auto chunk = MaterializeChunk(op_ctx, chunk_id);
        chunks.emplace_back(chunk, chunk.get());
    }
    return chunks;
}

std::vector<PinWrapper<Chunk*>>
VortexColumn::GetAllChunks(milvus::OpContext*) const {
    ThrowInfo(ErrorCode::Unsupported,
              "VortexColumn::GetAllChunks is disabled because it "
              "materializes Vortex data; use column view/bulk APIs instead");
}

void
VortexColumn::ApplyValidDataInChunk(milvus::OpContext* op_ctx,
                                    int64_t chunk_id,
                                    int64_t offset,
                                    int64_t size,
                                    TargetBitmapView valid_result) const {
    if (!IsNullable() || size == 0) {
        return;
    }

    CheckChunkId(chunk_id);
    const auto chunk_rows = files_[chunk_id].rows;
    AssertInfo(offset >= 0 && size >= 0 && offset + size <= chunk_rows,
               "vortex valid-data range [{}, {}) out of chunk rows {}",
               offset,
               offset + size,
               chunk_rows);

    const auto global_start = GetNumRowsUntilChunk(chunk_id) + offset;
    auto cursor = Scan(op_ctx, ScanOptions::ForNoData(global_start, size));
    AssertInfo(cursor != nullptr,
               "failed to create vortex validity scan for field {} chunk {}",
               field_id_.get(),
               chunk_id);

    ScanBatch batch;
    int64_t processed = 0;
    while (processed < size && cursor->Next(size - processed, &batch)) {
        AssertInfo(batch.row_id_start == global_start + processed,
                   "vortex validity scan returned row {} after processing {} "
                   "rows from {}",
                   batch.row_id_start,
                   processed,
                   global_start);
        AssertInfo(batch.size > 0 && processed + batch.size <= size,
                   "vortex validity scan returned invalid batch size {}, "
                   "processed {}, expected {}",
                   batch.size,
                   processed,
                   size);
        for (int64_t i = 0; i < batch.size; ++i) {
            if (batch.validity != nullptr && !batch.validity[i]) {
                valid_result[processed + i] = false;
            }
        }
        processed += batch.size;
    }
    AssertInfo(processed == size,
               "vortex validity scan returned {} rows, expected {}",
               processed,
               size);
}

int64_t
VortexColumn::GetNumRowsUntilChunk(int64_t chunk_id) const {
    AssertInfo(chunk_id >= 0 && chunk_id < static_cast<int64_t>(
                                               num_rows_until_chunk_.size()),
               "vortex chunk_id {} out of prefix range",
               chunk_id);
    return num_rows_until_chunk_[chunk_id];
}

const std::vector<int64_t>&
VortexColumn::GetNumRowsUntilChunk() const {
    return num_rows_until_chunk_;
}

void
VortexColumn::BulkValueAt(milvus::OpContext* op_ctx,
                          std::function<void(const char*, size_t)> fn,
                          const int64_t* offsets,
                          int64_t count) {
    if (count == 0) {
        return;
    }
    auto cursor = Take(op_ctx,
                       TakeOptions{OffsetView::From(offsets, count),
                                   ScanValueKind::FixedWidth});
    AssertInfo(cursor != nullptr,
               "vortex bulk value take is unsupported for type {}",
               data_type_);
    TakeBatch batch;
    int64_t output_index = 0;
    while (output_index < count && cursor->Next(count - output_index, &batch)) {
        AssertInfo(batch.values.encoding == ValueEncoding::FixedWidth,
                   "vortex bulk value expected fixed-width take, got {}",
                   static_cast<int>(batch.values.encoding));
        const auto* data = static_cast<const char*>(batch.values.data);
        for (int64_t i = 0; i < batch.size; ++i) {
            const auto value_index = batch.selection == nullptr
                                         ? batch.values.offset + i
                                         : batch.selection[i];
            fn(data + value_index * batch.values.byte_width, output_index++);
        }
    }
    AssertInfo(output_index == count,
               "vortex bulk value take returned {} rows, expected {}",
               output_index,
               count);
}

std::optional<DataType>
VortexColumn::GetDefaultScanDataType() const {
    return data_type_;
}

std::shared_ptr<arrow::Schema>
VortexColumn::MakeProjectedArrowSchema(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::string& field_name) {
    AssertInfo(schema != nullptr,
               "vortex projected schema requires a non-null file schema");
    auto field = schema->GetFieldByName(field_name);
    AssertInfo(field != nullptr,
               "vortex file schema does not contain field {}",
               field_name);
    return arrow::schema({field});
}

std::optional<std::string>
VortexColumn::VortexCompareOp(proto::plan::OpType op_type) {
    switch (op_type) {
        case proto::plan::OpType::GreaterThan:
            return ">";
        case proto::plan::OpType::GreaterEqual:
            return ">=";
        case proto::plan::OpType::LessThan:
            return "<";
        case proto::plan::OpType::LessEqual:
            return "<=";
        case proto::plan::OpType::Equal:
            return "=";
        case proto::plan::OpType::NotEqual:
            return "!=";
        default:
            return std::nullopt;
    }
}

std::string
VortexColumn::QuoteSqlIdentifier(std::string_view value) {
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('"');
    for (auto ch : value) {
        if (ch == '"') {
            out.push_back('"');
        }
        out.push_back(ch);
    }
    out.push_back('"');
    return out;
}

std::string
VortexColumn::QuoteSqlStringLiteral(std::string_view value) {
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('\'');
    for (auto ch : value) {
        if (ch == '\'') {
            out.push_back('\'');
        }
        out.push_back(ch);
    }
    out.push_back('\'');
    return out;
}

std::optional<std::string>
VortexColumn::VortexLiteral(DataType data_type,
                            const proto::plan::GenericValue& value) {
    if ((data_type != DataType::STRING && data_type != DataType::VARCHAR) ||
        value.val_case() != proto::plan::GenericValue::kStringVal) {
        return std::nullopt;
    }
    return QuoteSqlStringLiteral(value.string_val());
}

std::optional<std::string>
VortexColumn::BuildVortexPredicate(const ScanOptions& options) const {
    const auto field = QuoteSqlIdentifier(field_name_);
    switch (options.predicate) {
        case ScanPredicate::Unary: {
            auto op = VortexCompareOp(options.op_type);
            auto value = VortexLiteral(data_type_, options.value);
            if (!op.has_value() || !value.has_value()) {
                return std::nullopt;
            }
            return field + " " + *op + " " + *value;
        }
        case ScanPredicate::BinaryRange: {
            auto lower = VortexLiteral(data_type_, options.lower_value);
            auto upper = VortexLiteral(data_type_, options.upper_value);
            if (!lower.has_value() || !upper.has_value()) {
                return std::nullopt;
            }
            return field + (options.lower_inclusive ? " >= " : " > ") + *lower +
                   " AND " + field +
                   (options.upper_inclusive ? " <= " : " < ") + *upper;
        }
        default:
            return std::nullopt;
    }
}

bool
VortexColumn::SupportsScanPushdown(const ScanOptions& options) const {
    switch (data_type_) {
        case DataType::STRING:
        case DataType::VARCHAR:
            break;
        default:
            return false;
    }

    return options.output == ScanOutput::RowIds &&
           options.projection == ScanProjection::NoData &&
           options.predicate != ScanPredicate::None &&
           BuildVortexPredicate(options).has_value();
}

milvus_storage::api::Properties
VortexColumn::MakeVortexReaderProperties(
    const std::shared_ptr<milvus_storage::api::Properties>& properties) {
    auto reader_properties = *properties;
    reader_properties[PROPERTY_READER_VORTEX_SPLIT_ROW_INDICES] =
        std::string("true");
    return reader_properties;
}

std::shared_ptr<milvus_storage::vortex::VortexFormatReader>
VortexColumn::BuildFileReader(
    const VortexColumnGroup::FileState& group_file) const {
    auto projected_arrow_schema = MakeProjectedArrowSchema(
        group_file.footer_reader->file_schema(), field_name_);
    auto reader = std::make_shared<milvus_storage::vortex::VortexFormatReader>(
        group_file.sparse_fs,
        projected_arrow_schema,
        group_file.sparse_path,
        local_format_properties_,
        std::vector<std::string>{field_name_},
        group_file.footer_reader->file_size(),
        group_file.footer_reader->footer_size());
    auto status = reader->open();
    if (!status.ok()) {
        ThrowVortexStatus(
            status,
            ErrorCode::DataFormatBroken,
            fmt::format("failed to open vortex data reader for file {}",
                        group_file.path));
    }
    return reader;
}

void
VortexColumn::ValidateFileState(
    const FileState& state,
    const VortexColumnGroup::FileState& group_file) const {
    AssertInfo(state.rows == group_file.rows,
               "vortex field {} rows {} does not match column group rows {} "
               "for file {}",
               field_id_.get(),
               state.rows,
               group_file.rows,
               group_file.path);
    AssertInfo(state.planner->num_cells() == group_file.slot->num_cells(),
               "vortex field {} cells {} does not match column group cells {} "
               "for file {}",
               field_id_.get(),
               state.planner->num_cells(),
               group_file.slot->num_cells(),
               group_file.path);

    const auto& field_cells = state.planner->cell_metas();
    const auto& group_cells = group_file.planner->cell_metas();
    for (size_t cell_id = 0; cell_id < field_cells.size(); ++cell_id) {
        AssertInfo(
            field_cells[cell_id].row_offset ==
                    group_cells[cell_id].row_offset &&
                field_cells[cell_id].row_count ==
                    group_cells[cell_id].row_count,
            "vortex field {} cell {} row range [{}, {}) does not "
            "match column group [{}, {}) for file {}",
            field_id_.get(),
            cell_id,
            field_cells[cell_id].row_offset,
            field_cells[cell_id].row_offset + field_cells[cell_id].row_count,
            group_cells[cell_id].row_offset,
            group_cells[cell_id].row_offset + group_cells[cell_id].row_count,
            group_file.path);
    }
}

VortexColumn::FileState
VortexColumn::BuildFileState(
    size_t file_index, const VortexColumnGroup::FileState& group_file) const {
    FileState state;
    const auto file_schema = group_file.footer_reader->file_schema();
    if (file_schema == nullptr) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "vortex file {} has no Arrow schema",
                  group_file.path);
    }
    const auto arrow_field = file_schema->GetFieldByName(field_name_);
    if (arrow_field == nullptr) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "vortex file {} does not contain field {}",
                  group_file.path,
                  field_name_);
    }
    const auto compatibility =
        GetVortexArrowTypeCompatibility(data_type_, arrow_field->type());
    if (!compatibility.compatible) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "vortex file {} field {} type {} is incompatible with "
                  "Arrow {}",
                  group_file.path,
                  field_id_.get(),
                  data_type_,
                  arrow_field->type()->ToString());
    }
    state.direct_data_scan = compatibility.direct_data_scan;
    state.planner = column_group_->FieldPlanner(file_index, field_name_);
    state.rows = static_cast<int64_t>(state.planner->rows());
    ValidateFileState(state, group_file);
    return state;
}

bool
VortexColumn::SupportsDirectDataScan() const {
    return std::all_of(files_.begin(), files_.end(), [](const auto& file) {
        return file.direct_data_scan;
    });
}

std::pair<std::vector<std::string_view>, FixedVector<bool>>
VortexColumn::BuildStringViewsFromArrow(
    const ArrowStringLikeColumn& column,
    std::optional<std::pair<int64_t, int64_t>> offset_len,
    bool emit_valid) const {
    int64_t start = 0;
    int64_t length = column.length();
    if (offset_len.has_value()) {
        start = offset_len->first;
        length = offset_len->second;
        AssertInfo(
            start >= 0 && length >= 0 && start + length <= column.length(),
            "vortex string-like view range [{}, {}) out of rows {}",
            start,
            start + length,
            column.length());
    }

    std::pair<std::vector<std::string_view>, FixedVector<bool>> views;
    views.first.reserve(length);
    if (emit_valid) {
        views.second.reserve(length);
    }
    for (int64_t i = 0; i < length; ++i) {
        const auto row = start + i;
        views.first.emplace_back(column.ValueAt(row));
        if (emit_valid) {
            views.second.emplace_back(column.IsValid(row));
        }
    }
    return views;
}

void
VortexColumn::CheckChunkId(int64_t chunk_id) const {
    AssertInfo(chunk_id >= 0 && chunk_id < num_chunks(),
               "vortex chunk_id {} out of range {}",
               chunk_id,
               num_chunks());
}

std::shared_ptr<
    cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>
VortexColumn::PinPlanCells(milvus::OpContext* op_ctx,
                           size_t file_index,
                           const std::vector<uint64_t>& cell_ids) const {
    return column_group_->PinCells(op_ctx, file_index, cell_ids);
}

milvus_storage::vortex::VortexPlan
VortexColumn::PlanRowRange(const FileState& file,
                           uint64_t row_start,
                           uint64_t row_end,
                           const std::string& predicate) const {
    auto result = file.planner->PlanForRowRange(row_start, row_end, predicate);
    if (!result.ok()) {
        ThrowVortexStatus(
            result.status(),
            ErrorCode::UnexpectedError,
            fmt::format("failed to plan vortex read for row range [{}, {})",
                        row_start,
                        row_end));
    }
    return std::move(result).ValueOrDie();
}

milvus_storage::vortex::VortexPlan
VortexColumn::PlanOffsets(const FileState& file,
                          const std::vector<int64_t>& offsets) const {
    auto result = file.planner->PlanForOffsets(offsets);
    if (!result.ok()) {
        ThrowVortexStatus(result.status(),
                          ErrorCode::UnexpectedError,
                          "failed to plan vortex read for offsets");
    }
    return std::move(result).ValueOrDie();
}

std::shared_ptr<Chunk>
VortexColumn::ChunkFromTable(const std::shared_ptr<arrow::Table>& table) const {
    AssertInfo(table != nullptr, "vortex table is null");
    AssertInfo(table->num_columns() == 1,
               "vortex materialization expects one column, got {}",
               table->num_columns());
    auto arrays = table->column(0)->chunks();
    for (const auto& array : arrays) {
        ValidateVortexArrowArray(
            field_id_, data_type_, IsNullable(), array, false);
    }
    arrays = storage::NormalizeArrowForChunkWriter(arrays, field_meta_);
    return create_chunk(field_meta_, arrays);
}

std::shared_ptr<Chunk>
VortexColumn::MaterializeChunk(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    CheckChunkId(chunk_id);
    const auto& file = files_[chunk_id];
    int64_t start = 0;
    int64_t length = file.rows;
    if (offset_len.has_value()) {
        start = offset_len->first;
        length = offset_len->second;
        AssertInfo(start >= 0 && length >= 0 && start + length <= file.rows,
                   "vortex materialize range [{}, {}) out of chunk rows {}",
                   start,
                   start + length,
                   file.rows);
    }

    auto scan = OpenDataScanForFile(op_ctx, chunk_id, start, length);
    auto arrays = read_single_column_batches(scan.get());
    for (const auto& array : arrays) {
        ValidateVortexArrowArray(
            field_id_, data_type_, IsNullable(), array, false);
    }
    arrays = storage::NormalizeArrowForChunkWriter(arrays, field_meta_);
    return create_chunk(field_meta_, arrays);
}

PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>
VortexColumn::OpenDataScanForFile(milvus::OpContext* op_ctx,
                                  int64_t chunk_id,
                                  int64_t start_offset,
                                  int64_t length) const {
    CheckChunkId(chunk_id);
    const auto& file = files_[chunk_id];
    AssertInfo(
        start_offset >= 0 && length >= 0 && start_offset + length <= file.rows,
        "vortex data scan range [{}, {}) out of chunk rows {}",
        start_offset,
        start_offset + length,
        file.rows);
    auto plan = PlanRowRange(file,
                             static_cast<uint64_t>(start_offset),
                             static_cast<uint64_t>(start_offset + length));
    auto pin = PinPlanCells(op_ctx, chunk_id, plan.cell_ids);
    return OpenDataScanWithPlan(chunk_id, plan, pin);
}

PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>
VortexColumn::OpenDataScanWithPlan(
    int64_t chunk_id,
    const milvus_storage::vortex::VortexPlan& plan,
    const std::shared_ptr<
        cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>&
        pin) const {
    CheckChunkId(chunk_id);
    AssertInfo(pin != nullptr,
               "vortex data scan field {} chunk {} has no cell pin",
               field_id_.get(),
               chunk_id);
    auto vortex_reader = BuildFileReader(column_group_->files()[chunk_id]);
    auto stream_result = vortex_reader->read_with_plan(plan.read_plan);
    if (!stream_result.ok()) {
        ThrowVortexStatus(
            stream_result.status(),
            ErrorCode::DataFormatBroken,
            fmt::format("failed to open vortex data scan field {} chunk {}",
                        field_id_.get(),
                        chunk_id));
    }
    auto array_stream = std::move(stream_result).ValueOrDie();
    auto reader_result = arrow::ImportRecordBatchReader(&array_stream);
    if (!reader_result.ok()) {
        ThrowVortexStatus(
            reader_result.status(),
            ErrorCode::DataFormatBroken,
            fmt::format("failed to import vortex data scan field {} chunk {}",
                        field_id_.get(),
                        chunk_id));
    }
    return PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>(
        pin, std::move(reader_result).ValueOrDie());
}

PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>
VortexColumn::OpenRowIdScanForFile(milvus::OpContext* op_ctx,
                                   int64_t chunk_id,
                                   int64_t start_offset,
                                   int64_t length,
                                   const std::string& predicate) const {
    CheckChunkId(chunk_id);
    const auto& file = files_[chunk_id];
    AssertInfo(
        start_offset >= 0 && length >= 0 && start_offset + length <= file.rows,
        "vortex row id scan range [{}, {}) out of chunk rows {}",
        start_offset,
        start_offset + length,
        file.rows);
    auto plan = PlanRowRange(file,
                             static_cast<uint64_t>(start_offset),
                             static_cast<uint64_t>(start_offset + length),
                             predicate);
    auto pin = PinPlanCells(op_ctx, chunk_id, plan.cell_ids);
    return OpenRowIdScanWithPlan(chunk_id, plan, pin);
}

PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>
VortexColumn::OpenRowIdScanWithPlan(
    int64_t chunk_id,
    const milvus_storage::vortex::VortexPlan& plan,
    const std::shared_ptr<
        cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>&
        pin) const {
    CheckChunkId(chunk_id);
    AssertInfo(pin != nullptr,
               "vortex row id scan field {} chunk {} has no cell pin",
               field_id_.get(),
               chunk_id);
    auto vortex_reader = BuildFileReader(column_group_->files()[chunk_id]);
    auto stream_result = vortex_reader->read_row_ids_with_plan(plan.read_plan);
    if (!stream_result.ok()) {
        ThrowVortexStatus(
            stream_result.status(),
            ErrorCode::DataFormatBroken,
            fmt::format("failed to open vortex row id scan field {} chunk {}",
                        field_id_.get(),
                        chunk_id));
    }
    auto array_stream = std::move(stream_result).ValueOrDie();
    auto reader_result = arrow::ImportRecordBatchReader(&array_stream);
    if (!reader_result.ok()) {
        ThrowVortexStatus(
            reader_result.status(),
            ErrorCode::DataFormatBroken,
            fmt::format("failed to import vortex row id scan field {} chunk {}",
                        field_id_.get(),
                        chunk_id));
    }
    return PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>(
        pin, std::move(reader_result).ValueOrDie());
}

std::pair<std::shared_ptr<VortexColumn::ArrowStringViewHolder>,
          std::pair<std::vector<std::string_view>, FixedVector<bool>>>
VortexColumn::ScanStringLikeViewsFromFile(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    CheckChunkId(chunk_id);
    const auto& file = files_[chunk_id];
    int64_t start = 0;
    int64_t length = file.rows;
    if (offset_len.has_value()) {
        start = offset_len->first;
        length = offset_len->second;
        AssertInfo(
            start >= 0 && length >= 0 && start + length <= file.rows,
            "vortex string-like scan range [{}, {}) out of chunk rows {}",
            start,
            start + length,
            file.rows);
    }

    auto holder = std::make_shared<ArrowStringViewHolder>();
    std::pair<std::vector<std::string_view>, FixedVector<bool>> views;
    views.first.reserve(length);
    if (field_meta_.is_nullable()) {
        views.second.reserve(length);
    }
    if (length == 0) {
        return {std::move(holder), std::move(views)};
    }

    auto plan = PlanRowRange(file,
                             static_cast<uint64_t>(start),
                             static_cast<uint64_t>(start + length));
    auto pin = PinPlanCells(op_ctx, chunk_id, plan.cell_ids);
    auto vortex_reader = BuildFileReader(column_group_->files()[chunk_id]);
    auto stream_result = vortex_reader->read_with_plan(plan.read_plan);
    if (!stream_result.ok()) {
        ThrowVortexStatus(
            stream_result.status(),
            ErrorCode::DataFormatBroken,
            fmt::format(
                "failed to open vortex string-like scan field {} chunk {}",
                field_id_.get(),
                chunk_id));
    }
    auto array_stream = std::move(stream_result).ValueOrDie();
    auto reader_result = arrow::ImportRecordBatchReader(&array_stream);
    if (!reader_result.ok()) {
        ThrowVortexStatus(
            reader_result.status(),
            ErrorCode::DataFormatBroken,
            fmt::format(
                "failed to import vortex string-like scan field {} chunk {}",
                field_id_.get(),
                chunk_id));
    }

    auto reader = std::move(reader_result).ValueOrDie();
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = reader->ReadNext(&batch);
        if (!status.ok()) {
            ThrowVortexStatus(status,
                              ErrorCode::DataFormatBroken,
                              "failed to read vortex string-like scan batch");
        }
        if (batch == nullptr) {
            break;
        }
        AssertInfo(batch->num_columns() == 1,
                   "vortex string-like scan expects one column, got {}",
                   batch->num_columns());
        ValidateVortexArrowArray(
            field_id_, data_type_, IsNullable(), batch->column(0), false);
        ArrowStringLikeColumn column(batch->column(0));
        auto batch_views = BuildStringViewsFromArrow(
            column, std::nullopt, field_meta_.is_nullable());
        views.first.insert(views.first.end(),
                           batch_views.first.begin(),
                           batch_views.first.end());
        for (auto valid : batch_views.second) {
            views.second.emplace_back(valid);
        }
        holder->batches.emplace_back(std::move(batch));
    }
    holder->pins.emplace_back(std::move(pin));
    AssertInfo(static_cast<int64_t>(views.first.size()) == length,
               "vortex string-like scan returned {} rows, expected {}",
               views.first.size(),
               length);
    return {std::move(holder), std::move(views)};
}

VortexColumn::ArrowTakeResult
VortexColumn::TakeArrowFromFile(milvus::OpContext* op_ctx,
                                int64_t chunk_id,
                                const std::vector<int64_t>& offsets) const {
    const auto& file = files_[chunk_id];
    auto plan = PlanOffsets(file, offsets);
    // Take fully materializes decoded Arrow buffers. The cell pin only needs
    // to protect the reader through import and is released on return.
    auto pin = PinPlanCells(op_ctx, chunk_id, plan.cell_ids);
    auto vortex_reader = BuildFileReader(column_group_->files()[chunk_id]);
    auto stream_result = vortex_reader->read_with_plan(plan.read_plan);
    if (!stream_result.ok()) {
        ThrowVortexStatus(stream_result.status(),
                          ErrorCode::DataFormatBroken,
                          fmt::format("failed to take vortex field {} chunk {}",
                                      field_id_.get(),
                                      chunk_id));
    }
    auto array_stream = std::move(stream_result).ValueOrDie();
    auto chunked_array_result = arrow::ImportChunkedArray(&array_stream);
    if (!chunked_array_result.ok()) {
        ThrowVortexStatus(
            chunked_array_result.status(),
            ErrorCode::DataFormatBroken,
            fmt::format("failed to import vortex take field {} chunk {}",
                        field_id_.get(),
                        chunk_id));
    }
    auto chunked_array = chunked_array_result.ValueOrDie();
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(chunked_array->num_chunks());
    for (int i = 0; i < chunked_array->num_chunks(); ++i) {
        auto batch_result =
            arrow::RecordBatch::FromStructArray(chunked_array->chunk(i));
        if (!batch_result.ok()) {
            ThrowVortexStatus(
                batch_result.status(),
                ErrorCode::DataFormatBroken,
                fmt::format("failed to convert vortex take field {} chunk {}",
                            field_id_.get(),
                            chunk_id));
        }
        batches.emplace_back(batch_result.ValueOrDie());
    }
    auto table_result = arrow::Table::FromRecordBatches(batches);
    if (!table_result.ok()) {
        ThrowVortexStatus(
            table_result.status(),
            ErrorCode::DataFormatBroken,
            fmt::format("failed to build vortex take table field {} chunk {}",
                        field_id_.get(),
                        chunk_id));
    }
    ArrowTakeResult result;
    result.table = table_result.ValueOrDie();
    AssertInfo(result.table->num_columns() == 1,
               "vortex take field {} expected one column, got {}",
               field_id_.get(),
               result.table->num_columns());
    for (const auto& array : result.table->column(0)->chunks()) {
        ValidateVortexArrowArray(
            field_id_, data_type_, IsNullable(), array, false);
    }
    return result;
}

template <typename ArrowArrayT,
          typename SrcT,
          typename RawDstT,
          typename WidenDstT>
void
VortexColumn::CopyArrowPrimitiveValues(
    void* dst,
    const std::shared_ptr<arrow::Table>& table,
    const std::vector<int64_t>& original_positions,
    const std::vector<int64_t>& original_position_ends,
    bool small_int_raw_type) const {
    AssertInfo(table != nullptr, "vortex primitive take table is null");
    AssertInfo(table->num_columns() == 1,
               "vortex primitive take expects one column, got {}",
               table->num_columns());
    auto column = table->column(0);
    AssertInfo(column != nullptr, "vortex primitive take column is null");
    AssertInfo(
        static_cast<int64_t>(original_position_ends.size()) == column->length(),
        "vortex primitive take returned {} rows, expected {}",
        column->length(),
        original_position_ends.size());

    auto raw_dst = static_cast<RawDstT*>(dst);
    auto widen_dst = static_cast<WidenDstT*>(dst);
    int64_t table_offset = 0;
    for (const auto& chunk : column->chunks()) {
        auto array = std::dynamic_pointer_cast<ArrowArrayT>(chunk);
        AssertInfo(array != nullptr,
                   "vortex primitive take field {} expected Arrow array type "
                   "for {}, got {}",
                   field_id_.get(),
                   data_type_,
                   chunk ? chunk->type()->ToString() : "<null>");
        for (int64_t i = 0; i < array->length(); ++i) {
            const auto unique_index = table_offset + i;
            const auto position_start =
                unique_index == 0 ? 0
                                  : original_position_ends[unique_index - 1];
            const auto position_end = original_position_ends[unique_index];
            auto value = static_cast<SrcT>(array->Value(i));
            for (auto position = position_start; position < position_end;
                 ++position) {
                const auto output_index = original_positions[position];
                if (small_int_raw_type) {
                    raw_dst[output_index] = static_cast<RawDstT>(value);
                } else {
                    widen_dst[output_index] = static_cast<WidenDstT>(value);
                }
            }
        }
        table_offset += array->length();
    }
}

ChunkedColumnInterface::TakeResult
VortexColumn::Take(milvus::OpContext* op_ctx,
                   const TakeOptions& options) const {
    AssertInfo(options.offsets.size >= 0,
               "vortex take offset count must be non-negative, got {}",
               options.offsets.size);
    if (options.offsets.size > 0) {
        AssertInfo(options.offsets.data != nullptr,
                   "vortex take offsets are null with count {}",
                   options.offsets.size);
    }

    ScanValueKind expected_kind;
    if (IsPrimitiveDataType(data_type_)) {
        expected_kind = ScanValueKind::FixedWidth;
    } else if (data_type_ == DataType::JSON) {
        expected_kind = ScanValueKind::JsonView;
    } else if (IsChunkedVariableColumnDataType(data_type_)) {
        expected_kind = ScanValueKind::StringView;
    } else if (IsChunkedArrayColumnDataType(data_type_)) {
        expected_kind = ScanValueKind::ArrayView;
    } else {
        return nullptr;
    }
    const auto value_kind = options.value_kind == ScanValueKind::Default
                                ? expected_kind
                                : options.value_kind;
    AssertInfo(value_kind == expected_kind,
               "vortex take value kind {} does not match column type {}, "
               "expected {}",
               static_cast<int>(value_kind),
               data_type_,
               static_cast<int>(expected_kind));

    struct OffsetEntry {
        int64_t chunk_id;
        int64_t local_offset;
        int64_t original_position;
    };
    struct OffsetGroup {
        int64_t chunk_id;
        std::vector<int64_t> unique_offsets;
        std::vector<int64_t> original_positions;
        std::vector<int64_t> original_position_ends;
    };

    std::vector<OffsetEntry> entries;
    entries.reserve(options.offsets.size);
    for (int64_t i = 0; i < options.offsets.size; ++i) {
        auto [chunk_id, local_offset] = GetChunkIDByOffset(options.offsets[i]);
        entries.emplace_back(OffsetEntry{static_cast<int64_t>(chunk_id),
                                         static_cast<int64_t>(local_offset),
                                         i});
    }
    std::sort(entries.begin(),
              entries.end(),
              [](const OffsetEntry& left, const OffsetEntry& right) {
                  return std::tie(left.chunk_id,
                                  left.local_offset,
                                  left.original_position) <
                         std::tie(right.chunk_id,
                                  right.local_offset,
                                  right.original_position);
              });

    std::vector<OffsetGroup> groups;
    for (const auto& entry : entries) {
        if (groups.empty() || groups.back().chunk_id != entry.chunk_id) {
            groups.emplace_back(OffsetGroup{entry.chunk_id, {}, {}, {}});
        }
        auto& group = groups.back();
        if (group.unique_offsets.empty() ||
            group.unique_offsets.back() != entry.local_offset) {
            group.unique_offsets.emplace_back(entry.local_offset);
            group.original_position_ends.emplace_back();
        }
        group.original_positions.emplace_back(entry.original_position);
        group.original_position_ends.back() = group.original_positions.size();
    }

    auto for_each_original_position = [](const OffsetGroup& group,
                                         size_t unique_index,
                                         auto&& fn) {
        const auto position_start =
            unique_index == 0 ? 0
                              : group.original_position_ends[unique_index - 1];
        const auto position_end = group.original_position_ends[unique_index];
        for (auto position = position_start; position < position_end;
             ++position) {
            fn(group.original_positions[position]);
        }
    };

    auto owner = std::make_shared<OrderedTakeOwner>();
    if (IsNullable()) {
        owner->validity.resize(options.offsets.size);
        std::fill(owner->validity.begin(), owner->validity.end(), true);
    }

    auto normalize_table = [&](const std::shared_ptr<arrow::Table>& table) {
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(table->column(0)->num_chunks());
        for (const auto& array : table->column(0)->chunks()) {
            arrays.emplace_back(
                storage::NormalizeExternalArrow(array, field_meta_));
        }
        owner->normalized_arrays.insert(
            owner->normalized_arrays.end(), arrays.begin(), arrays.end());
        auto chunked = std::make_shared<arrow::ChunkedArray>(arrays);
        auto schema = arrow::schema(
            {arrow::field(field_name_, chunked->type(), IsNullable())});
        return arrow::Table::Make(schema, {std::move(chunked)});
    };

    auto retain_take = [&](ArrowTakeResult&& take,
                           const std::shared_ptr<arrow::Table>& table) {
        owner->tables.emplace_back(std::move(take.table));
        if (table != owner->tables.back()) {
            owner->tables.emplace_back(table);
        }
    };

    auto copy_validity = [&](const std::shared_ptr<arrow::Table>& table,
                             const OffsetGroup& group) {
        if (!IsNullable()) {
            return;
        }
        int64_t unique_index = 0;
        for (const auto& array : table->column(0)->chunks()) {
            for (int64_t i = 0; i < array->length(); ++i) {
                const auto valid = array->IsValid(i);
                for_each_original_position(
                    group, unique_index, [&](auto original_position) {
                        owner->validity[original_position] = valid;
                    });
                ++unique_index;
            }
        }
        AssertInfo(
            unique_index == static_cast<int64_t>(group.unique_offsets.size()),
            "vortex take validity returned {} rows, expected {}",
            unique_index,
            group.unique_offsets.size());
    };

    auto fill_primitive =
        [&]<typename ArrowArrayT, typename ValueT, typename ContainerT>(
            ContainerT& output, bool normalize) {
            output.resize(options.offsets.size);
            for (const auto& group : groups) {
                auto take = TakeArrowFromFile(
                    op_ctx, group.chunk_id, group.unique_offsets);
                auto table =
                    normalize ? normalize_table(take.table) : take.table;
                CopyArrowPrimitiveValues<ArrowArrayT, ValueT, ValueT, ValueT>(
                    output.data(),
                    table,
                    group.original_positions,
                    group.original_position_ends,
                    true);
                copy_validity(table, group);
                retain_take(std::move(take), table);
            }
        };

    ValueView values;
    values.physical_type = data_type_;
    values.logical_type = data_type_;
    values.offset = 0;
    values.size = options.offsets.size;

    switch (data_type_) {
        case DataType::INT8:
            fill_primitive.template operator()<arrow::Int8Array, int8_t>(
                owner->int8_values, false);
            values.data = owner->int8_values.data();
            values.byte_width = sizeof(int8_t);
            break;
        case DataType::INT16:
            fill_primitive.template operator()<arrow::Int16Array, int16_t>(
                owner->int16_values, false);
            values.data = owner->int16_values.data();
            values.byte_width = sizeof(int16_t);
            break;
        case DataType::INT32:
            fill_primitive.template operator()<arrow::Int32Array, int32_t>(
                owner->int32_values, false);
            values.data = owner->int32_values.data();
            values.byte_width = sizeof(int32_t);
            break;
        case DataType::INT64:
            fill_primitive.template operator()<arrow::Int64Array, int64_t>(
                owner->int64_values, false);
            values.data = owner->int64_values.data();
            values.byte_width = sizeof(int64_t);
            break;
        case DataType::TIMESTAMPTZ:
            fill_primitive.template operator()<arrow::Int64Array, int64_t>(
                owner->int64_values, !SupportsDirectDataScan());
            values.data = owner->int64_values.data();
            values.byte_width = sizeof(int64_t);
            break;
        case DataType::FLOAT:
            fill_primitive.template operator()<arrow::FloatArray, float>(
                owner->float_values, false);
            values.data = owner->float_values.data();
            values.byte_width = sizeof(float);
            break;
        case DataType::DOUBLE:
            fill_primitive.template operator()<arrow::DoubleArray, double>(
                owner->double_values, false);
            values.data = owner->double_values.data();
            values.byte_width = sizeof(double);
            break;
        case DataType::BOOL:
            fill_primitive.template operator()<arrow::BooleanArray, bool>(
                owner->bool_values, false);
            values.data = owner->bool_values.data();
            values.byte_width = sizeof(bool);
            break;
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::JSON:
        case DataType::GEOMETRY: {
            owner->string_views.resize(options.offsets.size);
            for (const auto& group : groups) {
                auto take = TakeArrowFromFile(
                    op_ctx, group.chunk_id, group.unique_offsets);
                auto table = !SupportsDirectDataScan()
                                 ? normalize_table(take.table)
                                 : take.table;
                ArrowStringLikeColumn column(table);
                auto unique_views = BuildStringViewsFromArrow(
                    column, std::nullopt, IsNullable());
                AssertInfo(
                    unique_views.first.size() == group.unique_offsets.size(),
                    "vortex take returned {} string-like rows, "
                    "expected {}",
                    unique_views.first.size(),
                    group.unique_offsets.size());
                for (size_t i = 0; i < group.unique_offsets.size(); ++i) {
                    const auto valid = !IsNullable() || unique_views.second[i];
                    for_each_original_position(
                        group, i, [&](auto original_position) {
                            owner->string_views[original_position] =
                                unique_views.first[i];
                            if (IsNullable()) {
                                owner->validity[original_position] = valid;
                            }
                        });
                }
                retain_take(std::move(take), table);
            }
            if (data_type_ == DataType::JSON) {
                owner->json_values.reserve(owner->string_views.size());
                for (const auto& value : owner->string_views) {
                    owner->json_values.emplace_back(value);
                }
                values.encoding = ValueEncoding::JsonView;
                values.kind = ScanValueKind::JsonView;
                values.logical_type = DataType::JSON;
                values.data = owner->json_values.data();
                values.byte_width = sizeof(Json);
            } else {
                values.encoding = ValueEncoding::StringView;
                values.kind = ScanValueKind::StringView;
                values.data = owner->string_views.data();
                values.byte_width = sizeof(std::string_view);
            }
            break;
        }
        case DataType::ARRAY: {
            size_t unique_count = 0;
            for (const auto& group : groups) {
                unique_count += group.unique_offsets.size();
            }
            owner->arrays.reserve(unique_count);
            std::vector<int64_t> ordered_array_indices(options.offsets.size,
                                                       -1);
            for (const auto& group : groups) {
                auto take = TakeArrowFromFile(
                    op_ctx, group.chunk_id, group.unique_offsets);
                auto table = normalize_table(take.table);
                ArrowStringLikeColumn column(table);
                auto serialized = BuildStringViewsFromArrow(
                    column, std::nullopt, IsNullable());
                AssertInfo(
                    serialized.first.size() == group.unique_offsets.size(),
                    "vortex take returned {} array rows, expected {}",
                    serialized.first.size(),
                    group.unique_offsets.size());
                for (size_t i = 0; i < group.unique_offsets.size(); ++i) {
                    const auto array_index =
                        static_cast<int64_t>(owner->arrays.size());
                    const auto valid = !IsNullable() || serialized.second[i];
                    if (valid) {
                        ScalarFieldProto proto;
                        const auto& value = serialized.first[i];
                        AssertInfo(
                            proto.ParseFromArray(
                                value.data(), static_cast<int>(value.size())),
                            "failed to parse vortex array take row");
                        owner->arrays.emplace_back(proto);
                    } else {
                        owner->arrays.emplace_back();
                    }
                    for_each_original_position(
                        group, i, [&](auto original_position) {
                            ordered_array_indices[original_position] =
                                array_index;
                            if (IsNullable()) {
                                owner->validity[original_position] = valid;
                            }
                        });
                }
                retain_take(std::move(take), table);
            }

            owner->array_views.resize(options.offsets.size);
            for (int64_t i = 0; i < options.offsets.size; ++i) {
                if (IsNullable() && !owner->validity[i]) {
                    continue;
                }
                const auto array_index = ordered_array_indices[i];
                AssertInfo(array_index >= 0 &&
                               array_index <
                                   static_cast<int64_t>(owner->arrays.size()),
                           "vortex array take mapping {} out of range {}",
                           array_index,
                           owner->arrays.size());
                auto& array = owner->arrays[array_index];
                if (array.length() == 0) {
                    continue;
                }
                owner->array_views[i] =
                    ArrayView(const_cast<char*>(array.data()),
                              array.length(),
                              array.byte_size(),
                              array.get_element_type(),
                              array.get_offsets_data());
            }
            values.encoding = ValueEncoding::ArrayView;
            values.kind = ScanValueKind::ArrayView;
            values.logical_type = DataType::ARRAY;
            values.data = owner->array_views.data();
            values.byte_width = sizeof(ArrayView);
            break;
        }
        default:
            return nullptr;
    }

    if (values.encoding == ValueEncoding::Empty) {
        values.encoding = ValueEncoding::FixedWidth;
        values.kind = ScanValueKind::FixedWidth;
    }
    const bool* validity =
        owner->validity.empty() ? nullptr : owner->validity.data();
    return std::make_unique<VortexOrderedTakeCursor>(
        values, validity, std::move(owner), options.offsets.size);
}

void
VortexColumn::BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                                   void* dst,
                                   const int64_t* offsets,
                                   int64_t count,
                                   bool small_int_raw_type) {
    if (count == 0) {
        return;
    }
    auto cursor = Take(op_ctx,
                       TakeOptions{OffsetView::From(offsets, count),
                                   ScanValueKind::FixedWidth});
    AssertInfo(cursor != nullptr,
               "vortex primitive take is unsupported for type {}",
               data_type_);

    auto copy_values = [&]<typename SrcT, typename DstT>() {
        TakeBatch batch;
        int64_t output_index = 0;
        while (output_index < count &&
               cursor->Next(count - output_index, &batch)) {
            AssertInfo(batch.values.encoding == ValueEncoding::FixedWidth,
                       "vortex primitive expected fixed-width take, got {}",
                       static_cast<int>(batch.values.encoding));
            const auto* values = static_cast<const SrcT*>(batch.values.data);
            auto* output = static_cast<DstT*>(dst);
            for (int64_t i = 0; i < batch.size; ++i) {
                const auto value_index = batch.selection == nullptr
                                             ? batch.values.offset + i
                                             : batch.selection[i];
                output[output_index++] = static_cast<DstT>(values[value_index]);
            }
        }
        AssertInfo(output_index == count,
                   "vortex primitive take returned {} rows, expected {}",
                   output_index,
                   count);
    };

    switch (data_type_) {
        case DataType::INT8:
            if (small_int_raw_type) {
                copy_values.template operator()<int8_t, int8_t>();
            } else {
                copy_values.template operator()<int8_t, int32_t>();
            }
            break;
        case DataType::INT16:
            if (small_int_raw_type) {
                copy_values.template operator()<int16_t, int16_t>();
            } else {
                copy_values.template operator()<int16_t, int32_t>();
            }
            break;
        case DataType::INT32:
            copy_values.template operator()<int32_t, int32_t>();
            break;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            copy_values.template operator()<int64_t, int64_t>();
            break;
        case DataType::FLOAT:
            copy_values.template operator()<float, float>();
            break;
        case DataType::DOUBLE:
            copy_values.template operator()<double, double>();
            break;
        case DataType::BOOL:
            copy_values.template operator()<bool, bool>();
            break;
        default:
            ThrowInfo(ErrorCode::Unsupported,
                      "VortexColumn::BulkPrimitiveValueAt unsupported data "
                      "type {}",
                      data_type_);
    }
}

void
VortexColumn::BulkVectorValueAt(
    milvus::OpContext*, void*, const int64_t*, int64_t, int64_t) {
    ThrowInfo(ErrorCode::Unsupported,
              "VortexColumn does not support vector fields");
}

void
VortexColumn::BulkRawStringAt(
    milvus::OpContext* op_ctx,
    std::function<void(std::string_view, size_t, bool)> fn,
    const int64_t* offsets,
    int64_t count) const {
    if (!IsChunkedVariableColumnDataType(data_type_) ||
        data_type_ == DataType::JSON) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::BulkRawStringAt only supports string fields");
    }
    BulkStringLikeAt(op_ctx, fn, offsets, count);
}

void
VortexColumn::BulkRawJsonAt(milvus::OpContext* op_ctx,
                            std::function<void(Json, size_t, bool)> fn,
                            const int64_t* offsets,
                            int64_t count) const {
    if (data_type_ != DataType::JSON) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::BulkRawJsonAt only supports JSON fields");
    }
    BulkStringLikeAt(
        op_ctx,
        [&](std::string_view value, size_t index, bool valid) {
            fn(Json(value.data(), value.size()), index, valid);
        },
        offsets,
        count);
}

void
VortexColumn::BulkRawBsonAt(
    milvus::OpContext* op_ctx,
    std::function<void(BsonView, uint32_t, uint32_t)> fn,
    const uint32_t* row_offsets,
    const uint32_t* value_offsets,
    int64_t count) const {
    if (data_type_ != DataType::STRING) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::BulkRawBsonAt only supports BSON fields");
    }
    if (count == 0) {
        return;
    }
    AssertInfo(row_offsets != nullptr && value_offsets != nullptr,
               "row_offsets and value_offsets must be provided");
    std::vector<int64_t> offsets(count);
    for (int64_t i = 0; i < count; ++i) {
        offsets[i] = row_offsets[i];
    }
    BulkStringLikeAt(
        op_ctx,
        [&](std::string_view value, size_t index, bool) {
            fn(BsonView(reinterpret_cast<const uint8_t*>(value.data()),
                        value.size()),
               row_offsets[index],
               value_offsets[index]);
        },
        offsets.data(),
        count);
}

void
VortexColumn::BulkArrayAt(milvus::OpContext* op_ctx,
                          std::function<void(const ArrayView&, size_t)> fn,
                          const int64_t* offsets,
                          int64_t count) const {
    if (!IsChunkedArrayColumnDataType(data_type_)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::BulkArrayAt only supports array fields");
    }
    if (count == 0) {
        return;
    }
    auto cursor = Take(op_ctx,
                       TakeOptions{OffsetView::From(offsets, count),
                                   ScanValueKind::ArrayView});
    AssertInfo(cursor != nullptr, "vortex array take is unsupported");
    TakeBatch batch;
    int64_t output_index = 0;
    while (output_index < count && cursor->Next(count - output_index, &batch)) {
        const auto* values = batch.values.data_as<ArrayView>();
        for (int64_t i = 0; i < batch.size; ++i) {
            fn(values[i], output_index++);
        }
    }
    AssertInfo(output_index == count,
               "vortex array take returned {} rows, expected {}",
               output_index,
               count);
}

void
VortexColumn::BulkVectorArrayAt(milvus::OpContext*,
                                std::function<void(VectorFieldProto&&, size_t)>,
                                const int64_t*,
                                int64_t) const {
    ThrowInfo(ErrorCode::Unsupported,
              "VortexColumn does not support vector array fields");
}

void
VortexColumn::BulkStringLikeAt(
    milvus::OpContext* op_ctx,
    const std::function<void(std::string_view, size_t, bool)>& fn,
    const int64_t* offsets,
    int64_t count) const {
    if (offsets == nullptr) {
        if (!SupportsDirectDataScan()) {
            int64_t global_offset = 0;
            for (int64_t chunk_id = 0; chunk_id < num_chunks(); ++chunk_id) {
                auto chunk = MaterializeChunk(op_ctx, chunk_id);
                auto views = static_cast<StringChunk*>(chunk.get())
                                 ->StringViews(std::nullopt);
                for (int64_t i = 0;
                     i < static_cast<int64_t>(views.first.size());
                     ++i) {
                    fn(views.first[i],
                       global_offset + i,
                       IsNullable() ? views.second[i] : true);
                }
                global_offset += files_[chunk_id].rows;
            }
            return;
        }
        int64_t global_offset = 0;
        for (int64_t chunk_id = 0; chunk_id < num_chunks(); ++chunk_id) {
            auto scan =
                OpenDataScanForFile(op_ctx, chunk_id, 0, files_[chunk_id].rows);
            int64_t row_offset = 0;
            while (true) {
                std::shared_ptr<arrow::RecordBatch> batch;
                auto status = scan.get()->ReadNext(&batch);
                if (!status.ok()) {
                    ThrowVortexStatus(
                        status,
                        ErrorCode::DataFormatBroken,
                        "failed to read vortex string-like scan batch");
                }
                if (batch == nullptr) {
                    break;
                }
                AssertInfo(batch->num_columns() == 1,
                           "vortex string-like scan expects one column, got {}",
                           batch->num_columns());
                ValidateVortexArrowArray(field_id_,
                                         data_type_,
                                         IsNullable(),
                                         batch->column(0),
                                         false);
                ArrowStringLikeColumn column(batch->column(0));
                for (int64_t i = 0; i < column.length(); ++i) {
                    fn(column.ValueAt(i),
                       global_offset + row_offset + i,
                       column.IsValid(i));
                }
                row_offset += column.length();
            }
            global_offset += files_[chunk_id].rows;
        }
        return;
    }

    if (count == 0) {
        return;
    }
    const auto value_kind = data_type_ == DataType::JSON
                                ? ScanValueKind::JsonView
                                : ScanValueKind::StringView;
    auto cursor =
        Take(op_ctx, TakeOptions{OffsetView::From(offsets, count), value_kind});
    AssertInfo(cursor != nullptr,
               "vortex string-like take is unsupported for type {}",
               data_type_);
    TakeBatch batch;
    int64_t output_index = 0;
    while (output_index < count && cursor->Next(count - output_index, &batch)) {
        if (data_type_ == DataType::JSON) {
            const auto* values = batch.values.data_as<Json>();
            for (int64_t i = 0; i < batch.size; ++i) {
                fn(values[i],
                   output_index,
                   batch.validity == nullptr || batch.validity[i]);
                ++output_index;
            }
        } else {
            const auto* values = batch.values.data_as<std::string_view>();
            for (int64_t i = 0; i < batch.size; ++i) {
                fn(values[i],
                   output_index,
                   batch.validity == nullptr || batch.validity[i]);
                ++output_index;
            }
        }
    }
    AssertInfo(output_index == count,
               "vortex string-like take returned {} rows, expected {}",
               output_index,
               count);
}

ChunkedColumnInterface::PreparedScanResult
VortexColumn::PrepareScan(milvus::OpContext* op_ctx,
                          const ScanOptions& options) const {
    AssertInfo(options.start_offset >= 0 && options.length >= 0 &&
                   options.start_offset + options.length <=
                       static_cast<int64_t>(NumRows()),
               "vortex scan range [{}, {}) out of rows {}",
               options.start_offset,
               options.start_offset + options.length,
               NumRows());
    AssertInfo(options.predicate == ScanPredicate::None ||
                   options.projection == ScanProjection::NoData,
               "vortex predicate scan must not return data");

    if (options.output == ScanOutput::Data) {
        // Dense data scans do not accept pushed-down predicates. Predicates
        // that must be evaluated by Milvus should use a plain data scan and
        // let the expression layer compare values from the returned batches.
        if (options.predicate != ScanPredicate::None) {
            return nullptr;
        }

        // A non-nullable validity-only scan has an empty cell plan. Reuse the
        // common cursor to emit the requested row ranges without opening a
        // Vortex reader.
        if (options.projection == ScanProjection::NoData && !IsNullable()) {
            return ChunkedColumnInterface::PrepareScan(op_ctx, options);
        }

        // Primitive and string-like fields can be exposed as ScanBatch views
        // directly from Arrow batches. Plan and pin every file range before
        // constructing the cursor; Next() only slices the prepared readers.
        if (options.projection == ScanProjection::NoData ||
            SupportsDirectDataScan()) {
            const auto value_kind = ResolveDataScanValueKind(
                data_type_, options.projection, options.value_kind);
            std::vector<PreparedVortexScanSource> prepared_sources;
            auto plan_pos = options.start_offset;
            const auto scan_end = options.start_offset + options.length;
            while (auto range =
                       NextVortexReaderRange(this, &plan_pos, scan_end)) {
                const auto& file = files_[range->chunk_id];
                auto plan = PlanRowRange(
                    file,
                    static_cast<uint64_t>(range->local_offset),
                    static_cast<uint64_t>(range->local_offset + range->length));
                auto pin = PinPlanCells(op_ctx, range->chunk_id, plan.cell_ids);
                prepared_sources.emplace_back(PreparedVortexScanSource{
                    *range, std::move(plan.cell_ids), std::move(pin)});
                plan_pos = range->range_end;
            }
            return std::make_shared<CallbackPreparedScan>(
                options.start_offset,
                scan_end,
                [this,
                 projection = options.projection,
                 value_kind,
                 prepared_sources = std::move(prepared_sources)](
                    const ChunkedColumnInterface::ScanPlan& scan_plan,
                    ChunkedColumnInterface::ScanProjection open_projection) {
                    AssertInfo(projection == ScanProjection::Data ||
                                   open_projection == ScanProjection::NoData,
                               "validity-only vortex scan cannot return data");
                    const auto position = scan_plan.requested_range.start;
                    const auto open_end = scan_plan.requested_range.end;
                    std::vector<VortexDataScanSource> sources;
                    for (const auto& prepared : prepared_sources) {
                        if (prepared.range.range_end <= position ||
                            prepared.range.range_start >= open_end) {
                            continue;
                        }
                        auto range = TrimReaderRange(prepared.range, position);
                        if (range.range_end > open_end) {
                            const auto trimmed = range.range_end - open_end;
                            range.length -= trimmed;
                            range.range_end = open_end;
                        }
                        const auto& file = files_[range.chunk_id];
                        auto plan = PlanRowRange(
                            file,
                            static_cast<uint64_t>(range.local_offset),
                            static_cast<uint64_t>(range.local_offset +
                                                  range.length));
                        AssertCellsCovered(plan.cell_ids, prepared.cell_ids);
                        auto reader = OpenDataScanWithPlan(
                            range.chunk_id, plan, prepared.pin);
                        sources.emplace_back(
                            VortexDataScanSource{range, std::move(reader)});
                    }
                    return std::make_unique<VortexDataScanCursor>(
                        this,
                        position,
                        open_end - position,
                        open_projection,
                        value_kind,
                        std::move(sources));
                });
        }

        // Complex types that cannot be represented by the Vortex scan view
        // reuse the common ChunkedColumn fallback path.
        return ChunkedColumnInterface::PrepareScan(op_ctx, options);
    }

    // RowId output is only meaningful for pushed-down predicates. Without a
    // predicate there is no sparse result to return, so let callers fall back.
    if (options.predicate == ScanPredicate::None) {
        return nullptr;
    }

    // Predicate pushdown is deliberately limited to expressions that can be
    // represented by the Vortex reader. Unsupported predicates fail here
    // instead of silently scanning data through the RowId path.
    auto predicate = BuildVortexPredicate(options);
    if (!predicate.has_value()) {
        ThrowInfo(ErrorCode::Unsupported,
                  "unsupported vortex row id scan predicate for field {} "
                  "type {}",
                  field_id_.get(),
                  static_cast<int>(data_type_));
    }

    // RowId scan and its optional validity side-stream share one unioned cell
    // pin per file range. All sources are prepared before cursor creation.
    std::vector<PreparedVortexScanSource> prepared_sources;
    auto plan_pos = options.start_offset;
    const auto scan_end = options.start_offset + options.length;
    while (auto range = NextVortexReaderRange(this, &plan_pos, scan_end)) {
        const auto& file = files_[range->chunk_id];
        const auto row_start = static_cast<uint64_t>(range->local_offset);
        const auto row_end =
            static_cast<uint64_t>(range->local_offset + range->length);
        auto matched_plan = PlanRowRange(file, row_start, row_end, *predicate);
        std::optional<milvus_storage::vortex::VortexPlan> validity_plan;
        auto cell_ids = matched_plan.cell_ids;
        if (IsNullable()) {
            validity_plan = PlanRowRange(file, row_start, row_end);
            cell_ids =
                MergeCellIds(matched_plan.cell_ids, validity_plan->cell_ids);
        }
        auto pin = PinPlanCells(op_ctx, range->chunk_id, cell_ids);
        prepared_sources.emplace_back(PreparedVortexScanSource{
            *range, std::move(cell_ids), std::move(pin)});
        plan_pos = range->range_end;
    }
    return std::make_shared<CallbackPreparedScan>(
        options.start_offset,
        scan_end,
        [this,
         predicate = std::move(*predicate),
         prepared_sources = std::move(prepared_sources)](
            const ChunkedColumnInterface::ScanPlan& scan_plan,
            ChunkedColumnInterface::ScanProjection open_projection) {
            AssertInfo(open_projection == ScanProjection::NoData,
                       "vortex row-id scan cannot return dense data");
            const auto position = scan_plan.requested_range.start;
            const auto open_end = scan_plan.requested_range.end;
            std::vector<VortexRowIdScanSource> sources;
            for (const auto& prepared : prepared_sources) {
                if (prepared.range.range_end <= position ||
                    prepared.range.range_start >= open_end) {
                    continue;
                }
                auto range = TrimReaderRange(prepared.range, position);
                if (range.range_end > open_end) {
                    const auto trimmed = range.range_end - open_end;
                    range.length -= trimmed;
                    range.range_end = open_end;
                }
                const auto& file = files_[range.chunk_id];
                const auto row_start =
                    static_cast<uint64_t>(range.local_offset);
                const auto row_end =
                    static_cast<uint64_t>(range.local_offset + range.length);
                auto matched_plan =
                    PlanRowRange(file, row_start, row_end, predicate);
                std::optional<milvus_storage::vortex::VortexPlan> validity_plan;
                auto cell_ids = matched_plan.cell_ids;
                if (IsNullable()) {
                    validity_plan = PlanRowRange(file, row_start, row_end);
                    cell_ids = MergeCellIds(matched_plan.cell_ids,
                                            validity_plan->cell_ids);
                }
                AssertCellsCovered(cell_ids, prepared.cell_ids);
                auto matched_reader = OpenRowIdScanWithPlan(
                    range.chunk_id, matched_plan, prepared.pin);
                std::optional<
                    PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
                    validity_reader;
                if (validity_plan.has_value()) {
                    validity_reader = OpenDataScanWithPlan(
                        range.chunk_id, *validity_plan, prepared.pin);
                }
                sources.emplace_back(
                    VortexRowIdScanSource{range,
                                          std::move(matched_reader),
                                          std::move(validity_reader)});
            }
            return std::make_unique<VortexRowIdScanCursor>(
                this, position, open_end - position, std::move(sources));
        });
}

}  // namespace milvus
