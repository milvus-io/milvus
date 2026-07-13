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
#include <iomanip>
#include <sstream>
#include <string>
#include <unordered_map>

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

void
ResetScanBatchOutput(ChunkedColumnInterface::ScanBatch* out) {
    out->values = ChunkedColumnInterface::ValueView{};
    out->validity = ChunkedColumnInterface::ValidityView{};
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
                           bool has_invalid,
                           bool nullable) {
    AssertInfo(validity != nullptr, "row id payload validity owner is null");
    AssertInfo(validity->size() == out->row_ids.size(),
               "row id payload validity size {} does not match row ids size {}",
               validity->size(),
               out->row_ids.size());
    out->row_id_start = out->row_ids.empty() ? 0 : out->row_ids.front();
    out->size = static_cast<int64_t>(out->row_ids.size());
    out->validity.size = out->size;
    out->validity.nullable = nullable;
    if (out->row_ids.empty() || !has_invalid) {
        out->validity.encoding =
            ChunkedColumnInterface::ValidityEncoding::AllValid;
        out->validity.all_valid = true;
        return;
    }
    out->validity.encoding =
        ChunkedColumnInterface::ValidityEncoding::BoolArray;
    out->validity.data = validity->data();
    out->validity.offset = 0;
    out->validity.all_valid = false;
    out->owner = std::move(validity);
}

void
FinalizeAllValidRowIdPayloadOutput(ChunkedColumnInterface::ScanBatch* out,
                                   bool nullable) {
    out->row_id_start = out->row_ids.empty() ? 0 : out->row_ids.front();
    out->size = static_cast<int64_t>(out->row_ids.size());
    out->validity.size = out->size;
    out->validity.nullable = nullable;
    out->validity.encoding = ChunkedColumnInterface::ValidityEncoding::AllValid;
    out->validity.all_valid = true;
}

}  // namespace

struct VortexColumn::ArrowTakeResult {
    std::shared_ptr<
        cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>
        pin;
    std::shared_ptr<arrow::Table> table;
};

struct VortexColumn::ArrowStringViewHolder {
    std::vector<std::shared_ptr<
        cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>>
        pins;
    std::vector<std::shared_ptr<arrow::Table>> tables;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
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
                          milvus::OpContext* op_ctx,
                          int64_t start_offset,
                          int64_t length,
                          std::string predicate)
        : column_(column),
          op_ctx_(op_ctx),
          predicate_(std::move(predicate)),
          scan_pos_(start_offset),
          scan_end_(start_offset + length) {
        AssertInfo(start_offset >= 0 && length >= 0 &&
                       start_offset + length <=
                           static_cast<int64_t>(column_->NumRows()),
                   "vortex row id scan range [{}, {}) out of rows {}",
                   start_offset,
                   start_offset + length,
                   column_->NumRows());
    }

    bool
    Next(ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "vortex row id scan output batch is null");
        ResetRowIdPayloadOutput(out);
        std::shared_ptr<FixedVector<bool>> validity;
        bool has_invalid = false;
        while (out->row_ids.empty() || HasBufferedEntries()) {
            if (!EnsureActiveReader()) {
                break;
            }
            if (!reader_may_contain_invalids_) {
                if (!AppendNextMatchedEntries(out)) {
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
            FinalizeAllValidRowIdPayloadOutput(out, column_->IsNullable());
            return true;
        }
        FinalizeRowIdPayloadOutput(
            out, std::move(validity), has_invalid, column_->IsNullable());
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
        auto range = NextVortexReaderRange(column_, &scan_pos_, scan_end_);
        if (!range.has_value()) {
            return false;
        }

        reader_active_ = true;
        reader_chunk_start_ = range->chunk_start;
        reader_range_start_ = range->range_start;
        reader_range_end_ = range->range_end;
        invalid_reader_next_row_id_ = reader_range_start_;
        matched_row_ids_.clear();
        invalid_row_ids_.clear();
        matched_pos_ = 0;
        invalid_pos_ = 0;
        reader_may_contain_invalids_ = ReaderNeedsValidityStream();
        matched_reader_ = column_->OpenRowIdScanForFile(op_ctx_,
                                                        range->chunk_id,
                                                        range->local_offset,
                                                        range->length,
                                                        predicate_);
        if (reader_may_contain_invalids_) {
            invalid_reader_ = column_->OpenDataScanForFile(
                op_ctx_, range->chunk_id, range->local_offset, range->length);
        }
        scan_pos_ = reader_range_end_;
        return true;
    }

    bool
    ReaderNeedsValidityStream() const {
        // VortexColumn does not build ChunkedColumnInterface's materialized
        // valid-count mapping. For nullable fields, read the Arrow validity
        // side-stream and merge it with pushed-down row ids.
        return column_->IsNullable();
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
            AssertInfo(status.ok(),
                       "failed to read vortex row id scan batch: {}",
                       status.ToString());
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
            AssertInfo(status.ok(),
                       "failed to read vortex row id validity batch: {}",
                       status.ToString());
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
    AppendNextMatchedEntries(ChunkedColumnInterface::ScanBatch* out) {
        if (!EnsureMatchedEntry()) {
            return false;
        }

        const auto rows_to_append = matched_row_ids_.size() - matched_pos_;
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

    const VortexColumn* column_;
    milvus::OpContext* op_ctx_;
    std::string predicate_;
    int64_t scan_pos_;
    int64_t scan_end_;
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
                         milvus::OpContext* op_ctx,
                         int64_t start_offset,
                         int64_t length,
                         ChunkedColumnInterface::ScanProjection projection,
                         ChunkedColumnInterface::ScanValueKind value_kind)
        : column_(column),
          op_ctx_(op_ctx),
          projection_(projection),
          value_kind_(value_kind),
          scan_pos_(start_offset),
          scan_end_(start_offset + length) {
        AssertInfo(start_offset >= 0 && length >= 0 &&
                       start_offset + length <=
                           static_cast<int64_t>(column_->NumRows()),
                   "vortex data scan range [{}, {}) out of rows {}",
                   start_offset,
                   start_offset + length,
                   column_->NumRows());
    }

    bool
    Next(ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "vortex data scan output batch is null");
        ResetScanBatchOutput(out);
        if (scan_pos_ >= scan_end_) {
            return false;
        }

        while (scan_pos_ < scan_end_) {
            if (current_batch_ != nullptr &&
                current_batch_pos_ < current_batch_->num_rows()) {
                const auto rows_to_return = std::min<int64_t>(
                    current_batch_->num_rows() - current_batch_pos_,
                    scan_end_ - scan_pos_);
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
                AssertInfo(status.ok(),
                           "failed to read vortex data scan batch: {}",
                           status.ToString());
                if (batch != nullptr) {
                    AssertInfo(batch->num_columns() == 1,
                               "vortex data scan expects one column, got {}",
                               batch->num_columns());
                    current_batch_ = std::move(batch);
                    current_batch_row_id_start_ = next_reader_row_id_;
                    next_reader_row_id_ += current_batch_->num_rows();
                    continue;
                }
                reader_.reset();
                continue;
            }

            if (!OpenReaderForScanPos()) {
                break;
            }
        }
        return false;
    }

 private:
    bool
    OpenReaderForScanPos() {
        auto range = NextVortexReaderRange(column_, &scan_pos_, scan_end_);
        if (!range.has_value()) {
            return false;
        }
        reader_ = column_->OpenDataScanForFile(
            op_ctx_, range->chunk_id, range->local_offset, range->length);
        next_reader_row_id_ = range->range_start;
        return true;
    }

    template <typename ArrowArrayT>
    const void*
    RawPrimitiveValues(const std::shared_ptr<arrow::Array>& array) const {
        auto typed = std::static_pointer_cast<ArrowArrayT>(array);
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
                    std::static_pointer_cast<arrow::BooleanArray>(array);
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
        out->validity.nullable = column_->IsNullable();
        out->validity.size = out->size;
        if (!column_->IsNullable() || array->null_count() == 0) {
            out->validity.encoding =
                ChunkedColumnInterface::ValidityEncoding::AllValid;
            out->validity.all_valid = true;
            return;
        }
        owner->validity = std::make_shared<FixedVector<bool>>();
        owner->validity->resize(array->length());
        for (int64_t i = 0; i < array->length(); ++i) {
            (*owner->validity)[i] = array->IsValid(i);
        }
        out->validity.encoding =
            ChunkedColumnInterface::ValidityEncoding::BoolArray;
        out->validity.data = owner->validity->data();
        out->validity.offset = current_batch_pos_;
        out->validity.all_valid = false;
    }

    void
    FillStringLikeOutput(const std::shared_ptr<arrow::Array>& array,
                         const std::shared_ptr<BatchOwner>& owner,
                         ChunkedColumnInterface::ScanBatch* out) const {
        out->validity.nullable = column_->IsNullable();
        out->validity.size = out->size;

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
            out->validity.encoding =
                ChunkedColumnInterface::ValidityEncoding::BoolArray;
            out->validity.data = owner->validity->data();
            out->validity.offset = 0;
            out->validity.all_valid = false;
        } else {
            out->validity.encoding =
                ChunkedColumnInterface::ValidityEncoding::AllValid;
            out->validity.all_valid = true;
        }
    }

    void
    FillOutputFromCurrentBatch(int64_t rows_to_return,
                               ChunkedColumnInterface::ScanBatch* out) const {
        auto array = current_batch_->column(0);
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
    milvus::OpContext* op_ctx_;
    ChunkedColumnInterface::ScanProjection projection_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    int64_t scan_pos_;
    int64_t scan_end_;
    std::optional<PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
        reader_;
    int64_t next_reader_row_id_{0};
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
    for (const auto& group_file : group_files) {
        files_.emplace_back(BuildFileState(group_file));
    }

    num_rows_until_chunk_ = column_group_->num_rows_until_chunk();
    num_rows_ = column_group_->num_rows();
}

VortexColumn::~VortexColumn() = default;

void
VortexColumn::ManualEvictCache() const {
    column_group_->ManualEvictCache();
}

void
VortexColumn::CancelWarmup() {
    column_group_->CancelWarmup();
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
        std::vector<cachinglayer::cid_t> cell_ids;
        cell_ids.reserve(file.planner->num_cells());
        for (size_t cell_id = 0; cell_id < file.planner->num_cells();
             ++cell_id) {
            cell_ids.emplace_back(static_cast<cachinglayer::cid_t>(cell_id));
        }
        cachinglayer::SemiInlineGet(file.slot->PinCells(op_ctx, cell_ids));
    }
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
    auto holder = std::make_shared<ArrowStringViewHolder>();
    std::pair<std::vector<std::string_view>, FixedVector<bool>> views;
    views.first.resize(offsets.size());
    views.second.resize(offsets.size());
    if (offsets.empty()) {
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            std::move(views));
    }

    std::vector<std::pair<int64_t, int64_t>> entries;
    entries.reserve(offsets.size());
    for (int64_t i = 0; i < static_cast<int64_t>(offsets.size()); ++i) {
        auto offset = offsets[i];
        AssertInfo(offset >= 0 && offset < files_[chunk_id].rows,
                   "vortex chunk-local offset {} out of chunk {} rows {}",
                   offset,
                   chunk_id,
                   files_[chunk_id].rows);
        entries.emplace_back(offset, i);
    }
    std::sort(entries.begin(), entries.end());
    std::vector<int64_t> unique_offsets;
    unique_offsets.reserve(entries.size());
    for (const auto& [offset, _] : entries) {
        if (unique_offsets.empty() || unique_offsets.back() != offset) {
            unique_offsets.emplace_back(offset);
        }
    }

    auto take = TakeArrowFromFile(op_ctx, chunk_id, unique_offsets);
    ArrowStringLikeColumn column(take.table);
    auto unique_views = BuildStringViewsFromArrow(
        column, std::nullopt, field_meta_.is_nullable());
    for (const auto& [offset, original_index] : entries) {
        auto it = std::lower_bound(
            unique_offsets.begin(), unique_offsets.end(), offset);
        auto take_offset = std::distance(unique_offsets.begin(), it);
        views.first[original_index] = unique_views.first[take_offset];
        views.second[original_index] =
            field_meta_.is_nullable() ? unique_views.second[take_offset] : true;
    }
    holder->pins.emplace_back(std::move(take.pin));
    holder->tables.emplace_back(std::move(take.table));
    return PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
        std::move(holder), std::move(views));
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

    std::vector<std::pair<int64_t, int64_t>> entries;
    entries.reserve(offsets.size());
    for (int64_t i = 0; i < static_cast<int64_t>(offsets.size()); ++i) {
        auto offset = offsets[i];
        AssertInfo(offset >= 0 && offset < files_[chunk_id].rows,
                   "vortex chunk-local offset {} out of chunk {} rows {}",
                   offset,
                   chunk_id,
                   files_[chunk_id].rows);
        entries.emplace_back(offset, i);
    }
    std::sort(entries.begin(), entries.end());
    std::vector<int64_t> unique_offsets;
    unique_offsets.reserve(entries.size());
    for (const auto& [offset, _] : entries) {
        if (unique_offsets.empty() || unique_offsets.back() != offset) {
            unique_offsets.emplace_back(offset);
        }
    }

    auto chunk = TakeFromFile(op_ctx, chunk_id, unique_offsets);
    auto array_chunk = static_cast<ArrayChunk*>(chunk.get());
    for (const auto& [offset, original_index] : entries) {
        auto it = std::lower_bound(
            unique_offsets.begin(), unique_offsets.end(), offset);
        auto take_offset =
            static_cast<int>(std::distance(unique_offsets.begin(), it));
        views.first[original_index] = array_chunk->View(take_offset);
        views.second[original_index] = array_chunk->isValid(take_offset);
    }
    return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
        chunk, std::move(views));
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
VortexColumn::GetAllChunks(milvus::OpContext*) const {
    ThrowInfo(ErrorCode::Unsupported,
              "VortexColumn::GetAllChunks is disabled because it "
              "materializes Vortex data; use column view/bulk APIs instead");
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
    auto result = TakeOwn(op_ctx, offsets, count);
    for (int64_t i = 0; i < count; ++i) {
        fn(result.chunks[i]->ValueAt(result.offsets[i]), i);
    }
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

bool
VortexColumn::CanUseVortexPredicateLiteral(
    DataType data_type, const proto::plan::GenericValue& value) {
    switch (data_type) {
        case DataType::BOOL:
            return value.val_case() == proto::plan::GenericValue::kBoolVal;
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
            return false;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return value.val_case() == proto::plan::GenericValue::kInt64Val;
        case DataType::FLOAT:
            return false;
        case DataType::DOUBLE:
            return value.val_case() == proto::plan::GenericValue::kFloatVal ||
                   value.val_case() == proto::plan::GenericValue::kInt64Val;
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::GEOMETRY:
            return value.val_case() == proto::plan::GenericValue::kStringVal;
        default:
            return false;
    }
}

std::string
VortexColumn::FormatVortexDoubleLiteral(double value) {
    std::ostringstream os;
    os << std::setprecision(17) << value;
    auto literal = os.str();
    if (literal.find_first_of(".eE") == std::string::npos) {
        literal += ".0";
    }
    return literal;
}

std::optional<std::string>
VortexColumn::VortexLiteral(DataType data_type,
                            const proto::plan::GenericValue& value) {
    if (!CanUseVortexPredicateLiteral(data_type, value)) {
        return std::nullopt;
    }
    switch (data_type) {
        case DataType::BOOL:
            return value.bool_val() ? "true" : "false";
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return std::to_string(value.int64_val());
        case DataType::FLOAT:
        case DataType::DOUBLE:
            if (value.val_case() == proto::plan::GenericValue::kFloatVal) {
                return FormatVortexDoubleLiteral(value.float_val());
            }
            return FormatVortexDoubleLiteral(
                static_cast<double>(value.int64_val()));
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::GEOMETRY:
            return QuoteSqlStringLiteral(value.string_val());
        default:
            return std::nullopt;
    }
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

std::shared_ptr<milvus_storage::vortex::VortexPlanner>
VortexColumn::MakeFilePlanner(
    const VortexColumnGroup::FileState& group_file) const {
    auto cell_metas_result = milvus_storage::vortex::BuildVortexCellMetas(
        group_file.footer_reader, field_name_);
    AssertInfo(cell_metas_result.ok(),
               "failed to build vortex cell metas for field {} file {}: {}",
               field_id_.get(),
               group_file.path,
               cell_metas_result.status().ToString());
    auto planner_result = milvus_storage::vortex::VortexPlanner::Make(
        group_file.footer_reader,
        field_name_,
        std::move(cell_metas_result).ValueOrDie());
    AssertInfo(planner_result.ok(),
               "failed to create vortex planner for field {} file {}: {}",
               field_id_.get(),
               group_file.path,
               planner_result.status().ToString());
    return std::move(planner_result).ValueOrDie();
}

std::shared_ptr<milvus_storage::vortex::VortexFormatReader>
VortexColumn::OpenFileReader(
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
    AssertInfo(status.ok(),
               "failed to open vortex data reader for file {}: {}",
               group_file.path,
               status.ToString());
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
    const VortexColumnGroup::FileState& group_file) const {
    FileState state;
    state.path = group_file.path;
    state.planner = MakeFilePlanner(group_file);
    state.slot = group_file.slot;
    state.rows = static_cast<int64_t>(state.planner->rows());
    state.memory_bytes = state.planner->memory_bytes();
    ValidateFileState(state, group_file);
    return state;
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
                           const FileState& file,
                           const std::vector<uint64_t>& cell_ids) const {
    std::vector<cachinglayer::cid_t> cids;
    cids.reserve(cell_ids.size());
    for (auto cell_id : cell_ids) {
        cids.emplace_back(static_cast<cachinglayer::cid_t>(cell_id));
    }
    return cachinglayer::SemiInlineGet(file.slot->PinCells(op_ctx, cids));
}

milvus_storage::vortex::VortexPlan
VortexColumn::PlanRowRange(const FileState& file,
                           uint64_t row_start,
                           uint64_t row_end,
                           const std::string& predicate) const {
    auto result = file.planner->PlanForRowRange(row_start, row_end, predicate);
    AssertInfo(result.ok(),
               "failed to plan vortex read for row range [{}, {}): {}",
               row_start,
               row_end,
               result.status().ToString());
    return std::move(result).ValueOrDie();
}

milvus_storage::vortex::VortexPlan
VortexColumn::PlanOffsets(const FileState& file,
                          const std::vector<int64_t>& offsets) const {
    auto result = file.planner->PlanForOffsets(offsets);
    AssertInfo(result.ok(),
               "failed to plan vortex read for offsets: {}",
               result.status().ToString());
    return std::move(result).ValueOrDie();
}

std::shared_ptr<Chunk>
VortexColumn::ChunkFromTable(const std::shared_ptr<arrow::Table>& table) const {
    AssertInfo(table != nullptr, "vortex table is null");
    AssertInfo(table->num_columns() == 1,
               "vortex materialization expects one column, got {}",
               table->num_columns());
    auto arrays = table->column(0)->chunks();
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
    auto pin = PinPlanCells(op_ctx, file, plan.cell_ids);
    auto vortex_reader = OpenFileReader(column_group_->files()[chunk_id]);
    auto stream_result = vortex_reader->read_with_plan(plan.read_plan);
    AssertInfo(stream_result.ok(),
               "failed to open vortex data scan field {} chunk {}: {}",
               field_id_.get(),
               chunk_id,
               stream_result.status().ToString());
    auto array_stream = std::move(stream_result).ValueOrDie();
    auto reader_result = arrow::ImportRecordBatchReader(&array_stream);
    AssertInfo(reader_result.ok(),
               "failed to open vortex data scan field {} chunk {}: {}",
               field_id_.get(),
               chunk_id,
               reader_result.status().ToString());
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
    auto pin = PinPlanCells(op_ctx, file, plan.cell_ids);
    auto vortex_reader = OpenFileReader(column_group_->files()[chunk_id]);
    auto stream_result = vortex_reader->read_row_ids_with_plan(plan.read_plan);
    AssertInfo(stream_result.ok(),
               "failed to open vortex row id scan field {} chunk {}: {}",
               field_id_.get(),
               chunk_id,
               stream_result.status().ToString());
    auto array_stream = std::move(stream_result).ValueOrDie();
    auto reader_result = arrow::ImportRecordBatchReader(&array_stream);
    AssertInfo(reader_result.ok(),
               "failed to open vortex row id scan field {} chunk {}: {}",
               field_id_.get(),
               chunk_id,
               reader_result.status().ToString());
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
    auto pin = PinPlanCells(op_ctx, file, plan.cell_ids);
    auto vortex_reader = OpenFileReader(column_group_->files()[chunk_id]);
    auto stream_result = vortex_reader->read_with_plan(plan.read_plan);
    AssertInfo(stream_result.ok(),
               "failed to open vortex string-like scan field {} chunk {}: {}",
               field_id_.get(),
               chunk_id,
               stream_result.status().ToString());
    auto array_stream = std::move(stream_result).ValueOrDie();
    auto reader_result = arrow::ImportRecordBatchReader(&array_stream);
    AssertInfo(reader_result.ok(),
               "failed to open vortex string-like scan field {} chunk {}: {}",
               field_id_.get(),
               chunk_id,
               reader_result.status().ToString());

    auto reader = std::move(reader_result).ValueOrDie();
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = reader->ReadNext(&batch);
        AssertInfo(status.ok(),
                   "failed to read vortex string-like scan batch: {}",
                   status.ToString());
        if (batch == nullptr) {
            break;
        }
        AssertInfo(batch->num_columns() == 1,
                   "vortex string-like scan expects one column, got {}",
                   batch->num_columns());
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

std::shared_ptr<Chunk>
VortexColumn::TakeFromFile(milvus::OpContext* op_ctx,
                           int64_t chunk_id,
                           const std::vector<int64_t>& offsets) const {
    auto take = TakeArrowFromFile(op_ctx, chunk_id, offsets);
    return ChunkFromTable(take.table);
}

VortexColumn::ArrowTakeResult
VortexColumn::TakeArrowFromFile(milvus::OpContext* op_ctx,
                                int64_t chunk_id,
                                const std::vector<int64_t>& offsets) const {
    const auto& file = files_[chunk_id];
    auto plan = PlanOffsets(file, offsets);
    auto pin = PinPlanCells(op_ctx, file, plan.cell_ids);
    auto vortex_reader = OpenFileReader(column_group_->files()[chunk_id]);
    auto stream_result = vortex_reader->read_with_plan(plan.read_plan);
    AssertInfo(stream_result.ok(),
               "failed to take vortex field {} chunk {}: {}",
               field_id_.get(),
               chunk_id,
               stream_result.status().ToString());
    auto array_stream = std::move(stream_result).ValueOrDie();
    auto chunked_array_result = arrow::ImportChunkedArray(&array_stream);
    AssertInfo(chunked_array_result.ok(),
               "failed to take vortex field {} chunk {}: {}",
               field_id_.get(),
               chunk_id,
               chunked_array_result.status().ToString());
    auto chunked_array = chunked_array_result.ValueOrDie();
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(chunked_array->num_chunks());
    for (int i = 0; i < chunked_array->num_chunks(); ++i) {
        auto batch_result =
            arrow::RecordBatch::FromStructArray(chunked_array->chunk(i));
        AssertInfo(batch_result.ok(),
                   "failed to take vortex field {} chunk {}: {}",
                   field_id_.get(),
                   chunk_id,
                   batch_result.status().ToString());
        batches.emplace_back(batch_result.ValueOrDie());
    }
    auto table_result = arrow::Table::FromRecordBatches(batches);
    AssertInfo(table_result.ok(),
               "failed to take vortex field {} chunk {}: {}",
               field_id_.get(),
               chunk_id,
               table_result.status().ToString());
    ArrowTakeResult result;
    result.pin = std::move(pin);
    result.table = table_result.ValueOrDie();
    return result;
}

std::pair<std::shared_ptr<VortexColumn::ArrowStringViewHolder>,
          std::pair<std::vector<std::string_view>, FixedVector<bool>>>
VortexColumn::TakeStringLikeViews(milvus::OpContext* op_ctx,
                                  const int64_t* offsets,
                                  int64_t count) const {
    auto holder = std::make_shared<ArrowStringViewHolder>();
    std::pair<std::vector<std::string_view>, FixedVector<bool>> views;
    views.first.resize(count);
    views.second.resize(count);
    if (count == 0) {
        return {std::move(holder), std::move(views)};
    }
    AssertInfo(offsets != nullptr,
               "vortex string-like take requires explicit row offsets");

    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>>
        chunk_entries;
    for (int64_t i = 0; i < count; ++i) {
        auto [chunk_id, chunk_offset] = GetChunkIDByOffset(offsets[i]);
        chunk_entries[static_cast<int64_t>(chunk_id)].push_back(
            {static_cast<int64_t>(chunk_offset), i});
    }

    holder->pins.reserve(chunk_entries.size());
    holder->tables.reserve(chunk_entries.size());
    for (auto& [chunk_id, entries] : chunk_entries) {
        std::sort(entries.begin(), entries.end());
        std::vector<int64_t> unique_offsets;
        unique_offsets.reserve(entries.size());
        for (const auto& [chunk_offset, _] : entries) {
            if (unique_offsets.empty() ||
                unique_offsets.back() != chunk_offset) {
                unique_offsets.emplace_back(chunk_offset);
            }
        }

        auto take = TakeArrowFromFile(op_ctx, chunk_id, unique_offsets);
        ArrowStringLikeColumn column(take.table);
        auto unique_views = BuildStringViewsFromArrow(
            column, std::nullopt, field_meta_.is_nullable());
        AssertInfo(static_cast<int64_t>(unique_views.first.size()) ==
                       static_cast<int64_t>(unique_offsets.size()),
                   "vortex string-like take returned {} rows, expected {}",
                   unique_views.first.size(),
                   unique_offsets.size());
        for (const auto& [chunk_offset, original_index] : entries) {
            auto it = std::lower_bound(
                unique_offsets.begin(), unique_offsets.end(), chunk_offset);
            auto take_offset = std::distance(unique_offsets.begin(), it);
            views.first[original_index] = unique_views.first[take_offset];
            views.second[original_index] =
                field_meta_.is_nullable() ? unique_views.second[take_offset]
                                          : true;
        }
        holder->pins.emplace_back(std::move(take.pin));
        holder->tables.emplace_back(std::move(take.table));
    }

    return {std::move(holder), std::move(views)};
}

template <typename ArrowArrayT,
          typename SrcT,
          typename RawDstT,
          typename WidenDstT>
void
VortexColumn::CopyArrowPrimitiveValues(
    void* dst,
    const std::shared_ptr<arrow::Table>& table,
    const std::vector<std::vector<int64_t>>& original_indices_by_unique,
    bool small_int_raw_type) const {
    AssertInfo(table != nullptr, "vortex primitive take table is null");
    AssertInfo(table->num_columns() == 1,
               "vortex primitive take expects one column, got {}",
               table->num_columns());
    auto column = table->column(0);
    AssertInfo(column != nullptr, "vortex primitive take column is null");
    AssertInfo(static_cast<int64_t>(original_indices_by_unique.size()) ==
                   column->length(),
               "vortex primitive take returned {} rows, expected {}",
               column->length(),
               original_indices_by_unique.size());

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
            const auto& output_indices =
                original_indices_by_unique[static_cast<size_t>(table_offset +
                                                               i)];
            auto value = static_cast<SrcT>(array->Value(i));
            for (auto output_index : output_indices) {
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

template <typename ArrowArrayT,
          typename SrcT,
          typename RawDstT,
          typename WidenDstT>
void
VortexColumn::BulkPrimitiveValueAtFromArrow(milvus::OpContext* op_ctx,
                                            void* dst,
                                            const int64_t* offsets,
                                            int64_t count,
                                            bool small_int_raw_type) const {
    if (count == 0) {
        return;
    }
    AssertInfo(offsets != nullptr,
               "vortex primitive take requires explicit row offsets");

    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>>
        chunk_entries;
    for (int64_t i = 0; i < count; ++i) {
        auto [chunk_id, chunk_offset] = GetChunkIDByOffset(offsets[i]);
        chunk_entries[static_cast<int64_t>(chunk_id)].push_back(
            {static_cast<int64_t>(chunk_offset), i});
    }

    for (auto& [chunk_id, entries] : chunk_entries) {
        std::sort(entries.begin(), entries.end());
        std::vector<int64_t> unique_offsets;
        std::vector<std::vector<int64_t>> original_indices_by_offset;
        unique_offsets.reserve(entries.size());
        original_indices_by_offset.reserve(entries.size());
        for (const auto& [chunk_offset, original_index] : entries) {
            if (unique_offsets.empty() ||
                unique_offsets.back() != chunk_offset) {
                unique_offsets.emplace_back(chunk_offset);
                original_indices_by_offset.emplace_back();
            }
            original_indices_by_offset.back().emplace_back(original_index);
        }

        auto take = TakeArrowFromFile(op_ctx, chunk_id, unique_offsets);
        CopyArrowPrimitiveValues<ArrowArrayT, SrcT, RawDstT, WidenDstT>(
            dst, take.table, original_indices_by_offset, small_int_raw_type);
    }
}

void
VortexColumn::BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                                   void* dst,
                                   const int64_t* offsets,
                                   int64_t count,
                                   bool small_int_raw_type) {
    switch (data_type_) {
        case DataType::INT8: {
            BulkPrimitiveValueAtFromArrow<arrow::Int8Array,
                                          int8_t,
                                          int8_t,
                                          int32_t>(
                op_ctx, dst, offsets, count, small_int_raw_type);
            break;
        }
        case DataType::INT16: {
            BulkPrimitiveValueAtFromArrow<arrow::Int16Array,
                                          int16_t,
                                          int16_t,
                                          int32_t>(
                op_ctx, dst, offsets, count, small_int_raw_type);
            break;
        }
        case DataType::INT32: {
            BulkPrimitiveValueAtFromArrow<arrow::Int32Array,
                                          int32_t,
                                          int32_t,
                                          int32_t>(
                op_ctx, dst, offsets, count, true);
            break;
        }
        case DataType::INT64:
        case DataType::TIMESTAMPTZ: {
            BulkPrimitiveValueAtFromArrow<arrow::Int64Array,
                                          int64_t,
                                          int64_t,
                                          int64_t>(
                op_ctx, dst, offsets, count, true);
            break;
        }
        case DataType::FLOAT: {
            BulkPrimitiveValueAtFromArrow<arrow::FloatArray,
                                          float,
                                          float,
                                          float>(
                op_ctx, dst, offsets, count, true);
            break;
        }
        case DataType::DOUBLE: {
            BulkPrimitiveValueAtFromArrow<arrow::DoubleArray,
                                          double,
                                          double,
                                          double>(
                op_ctx, dst, offsets, count, true);
            break;
        }
        case DataType::BOOL: {
            BulkPrimitiveValueAtFromArrow<arrow::BooleanArray,
                                          bool,
                                          bool,
                                          bool>(
                op_ctx, dst, offsets, count, true);
            break;
        }
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
    auto result = TakeOwn(op_ctx, offsets, count);
    for (int64_t i = 0; i < count; ++i) {
        auto view = static_cast<ArrayChunk*>(result.chunks[i].get())
                        ->View(result.offsets[i]);
        fn(view, i);
    }
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
        int64_t global_offset = 0;
        for (int64_t chunk_id = 0; chunk_id < num_chunks(); ++chunk_id) {
            auto scan =
                OpenDataScanForFile(op_ctx, chunk_id, 0, files_[chunk_id].rows);
            int64_t row_offset = 0;
            while (true) {
                std::shared_ptr<arrow::RecordBatch> batch;
                auto status = scan.get()->ReadNext(&batch);
                AssertInfo(status.ok(),
                           "failed to read vortex string-like scan batch: {}",
                           status.ToString());
                if (batch == nullptr) {
                    break;
                }
                AssertInfo(batch->num_columns() == 1,
                           "vortex string-like scan expects one column, got {}",
                           batch->num_columns());
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

    auto [holder, views] = TakeStringLikeViews(op_ctx, offsets, count);
    (void)holder;
    for (int64_t i = 0; i < count; ++i) {
        fn(views.first[i], i, views.second[i]);
    }
}

VortexColumn::TakeResult
VortexColumn::TakeOwn(milvus::OpContext* op_ctx,
                      const int64_t* offsets,
                      int64_t count) const {
    TakeResult result;
    result.chunks.resize(count);
    result.offsets.resize(count);
    if (count == 0) {
        return result;
    }
    AssertInfo(offsets != nullptr, "vortex take requires explicit row offsets");

    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>>
        chunk_entries;
    for (int64_t i = 0; i < count; ++i) {
        auto [chunk_id, chunk_offset] = GetChunkIDByOffset(offsets[i]);
        chunk_entries[static_cast<int64_t>(chunk_id)].push_back(
            {static_cast<int64_t>(chunk_offset), i});
    }

    for (auto& [chunk_id, entries] : chunk_entries) {
        std::sort(entries.begin(), entries.end());
        std::vector<int64_t> unique_offsets;
        unique_offsets.reserve(entries.size());
        for (const auto& [chunk_offset, _] : entries) {
            if (unique_offsets.empty() ||
                unique_offsets.back() != chunk_offset) {
                unique_offsets.emplace_back(chunk_offset);
            }
        }

        auto chunk = TakeFromFile(op_ctx, chunk_id, unique_offsets);
        for (const auto& [chunk_offset, original_index] : entries) {
            auto it = std::lower_bound(
                unique_offsets.begin(), unique_offsets.end(), chunk_offset);
            result.chunks[original_index] = chunk;
            result.offsets[original_index] =
                std::distance(unique_offsets.begin(), it);
        }
    }

    return result;
}

std::shared_ptr<VortexColumn::TakeResult>
VortexColumn::Take(milvus::OpContext* op_ctx,
                   const int64_t* offsets,
                   int64_t count) const {
    return std::make_shared<TakeResult>(TakeOwn(op_ctx, offsets, count));
}

ChunkedColumnInterface::ScanResult
VortexColumn::Scan(milvus::OpContext* op_ctx,
                   const ScanOptions& options) const {
    AssertInfo(options.predicate == ScanPredicate::None ||
                   options.projection == ScanProjection::NoData,
               "vortex predicate scan must not return data");

    // Scan() only builds a Milvus scan cursor. Planning, cell pinning, and
    // opening the Arrow reader are deferred to cursor::Next(), so a cursor
    // does not pin all cells in the requested range upfront.
    if (options.output == ScanOutput::Data) {
        // Dense data scans do not accept pushed-down predicates. Predicates
        // that must be evaluated by Milvus should use a plain data scan and
        // let the expression layer compare values from the returned batches.
        if (options.predicate != ScanPredicate::None) {
            return nullptr;
        }

        // Primitive and string-like fields can be exposed as ScanBatch views
        // directly from Arrow batches. NoData keeps the same dense row-range
        // cursor but omits values and returns only validity.
        if (options.projection == ScanProjection::NoData ||
            IsPrimitiveDataType(data_type_) ||
            IsChunkedVariableColumnDataType(data_type_)) {
            auto value_kind = options.value_kind;
            if (value_kind == ScanValueKind::Default &&
                IsChunkedVariableColumnDataType(data_type_)) {
                value_kind = data_type_ == DataType::JSON
                                 ? ScanValueKind::JsonView
                                 : ScanValueKind::StringView;
            }
            return std::make_unique<VortexDataScanCursor>(this,
                                                          op_ctx,
                                                          options.start_offset,
                                                          options.length,
                                                          options.projection,
                                                          value_kind);
        }

        // Complex types that cannot be represented by the Vortex scan view
        // reuse the common ChunkedColumn fallback path.
        return ChunkedColumnInterface::Scan(op_ctx, options);
    }

    // RowId output is only meaningful for pushed-down predicates. Without a
    // predicate there is no sparse result to return, so let callers fall back.
    if (options.predicate == ScanPredicate::None) {
        return nullptr;
    }

    // Predicate pushdown is deliberately limited to expressions that can be
    // represented by the Vortex reader. Unsupported predicates fail here
    // instead of silently scanning data through the RowId path.
    if (!SupportsScanPushdown(options)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "unsupported vortex row id scan predicate for field {} "
                  "type {}",
                  field_id_.get(),
                  static_cast<int>(data_type_));
    }

    auto predicate = BuildVortexPredicate(options);
    if (!predicate.has_value()) {
        ThrowInfo(ErrorCode::Unsupported,
                  "unsupported vortex row id scan predicate for field {} "
                  "type {}",
                  field_id_.get(),
                  static_cast<int>(data_type_));
    }

    // RowId scan returns sparse matched row ids (and nullable validity when
    // needed). The cursor opens per-file readers lazily and pins only the
    // cells required by each planned range.
    return std::make_unique<VortexRowIdScanCursor>(this,
                                                   op_ctx,
                                                   options.start_offset,
                                                   options.length,
                                                   std::move(*predicate));
}

}  // namespace milvus
