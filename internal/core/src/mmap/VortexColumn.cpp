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
#include "mmap/ChunkedColumnFilter.h"
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

TargetType
PrimitiveTargetType(DataType data_type) {
    switch (data_type) {
        case DataType::BOOL:
            return TargetType::Bool;
        case DataType::INT8:
            return TargetType::Int8;
        case DataType::INT16:
            return TargetType::Int16;
        case DataType::INT32:
            return TargetType::Int32;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return TargetType::Int64;
        case DataType::FLOAT:
            return TargetType::Float;
        case DataType::DOUBLE:
            return TargetType::Double;
        default:
            ThrowInfo(ErrorCode::Unsupported,
                      "data type {} has no primitive target type",
                      data_type);
    }
}

bool
CanReadVortexAsTargetType(const FieldMeta& field_meta, TargetType target_type) {
    if (!CanReadAsTargetType(field_meta.get_data_type(), target_type)) {
        return false;
    }
    if (field_meta.get_data_type() != DataType::ARRAY) {
        return true;
    }
    return field_meta.is_nested_array()
               ? target_type == TargetType::ArrayValueView
               : target_type == TargetType::ArrayView;
}

VortexArrowTypeCompatibility
GetVortexArrowTypeCompatibility(
    DataType data_type, const std::shared_ptr<arrow::DataType>& arrow_type) {
    if (arrow_type == nullptr) {
        return {};
    }

    switch (data_type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::TIMESTAMPTZ:
            if (arrow_type->Equals(*GetArrowDataType(data_type))) {
                return {true, true};
            }
            // The existing raw materialization path normalizes Arrow
            // timestamps to Milvus TIMESTAMPTZ's int64 microsecond
            // representation. Keep that compatibility, but do not expose an
            // unnormalized timestamp buffer through the zero-copy data scan.
            if (data_type == DataType::TIMESTAMPTZ &&
                arrow_type->id() == arrow::Type::TIMESTAMP) {
                return {true, false};
            }
            return {};
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::JSON:
        case DataType::GEOMETRY: {
            const auto compatible = IsArrowStringLikeType(arrow_type->id());
            // Geometry string encodings are WKT. The existing materialization
            // path converts them to Milvus's WKB representation, so only
            // binary geometry can be exposed directly by Scan.
            const auto direct_data_scan =
                data_type == DataType::GEOMETRY
                    ? IsArrowBinaryLikeType(arrow_type->id())
                    : compatible;
            return {compatible, direct_data_scan};
        }
        case DataType::ARRAY:
            return {IsArrowArrayLikeType(arrow_type->id()), false};
        default:
            return {};
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
    out->validity = {};
    out->row_ids.clear();
    out->owner.reset();
    out->row_id_start = 0;
    out->size = 0;
    out->data_skipped = false;
}

struct PackedValidityBuffer {
    void
    Resize(int64_t new_size, bool value = false) {
        AssertInfo(new_size >= 0, "negative validity size {}", new_size);
        size = new_size;
        bits.assign((size + 7) / 8, value ? uint8_t{0xff} : uint8_t{0});
    }

    void
    PushBack(bool valid) {
        const auto index = size++;
        if ((index & 0x07) == 0) {
            bits.emplace_back(0);
        }
        Set(index, valid);
    }

    void
    Set(int64_t index, bool valid) {
        AssertInfo(index >= 0 && index < size,
                   "validity index {} out of range {}",
                   index,
                   size);
        const auto mask = uint8_t{1} << (index & 0x07);
        if (valid) {
            bits[index >> 3] |= mask;
        } else {
            bits[index >> 3] &= ~mask;
        }
    }

    bool
    Get(int64_t index) const {
        AssertInfo(index >= 0 && index < size,
                   "validity index {} out of range {}",
                   index,
                   size);
        return (bits[index >> 3] >> (index & 0x07)) & 1;
    }

    ValidityView
    View() const {
        return size == 0 ? ValidityView{}
                         : ValidityView::FromPacked(bits.data());
    }

    std::vector<uint8_t> bits;
    int64_t size{0};
};

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

struct VortexRowIdScanSource {
    VortexReaderRange range;
    PinWrapper<std::shared_ptr<arrow::RecordBatchReader>> matched_reader;
    std::optional<PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
        validity_reader;
};

using VortexCellPin = std::shared_ptr<
    cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>;

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

class VortexColumnPlanner final : public ColumnPlanner {
 public:
    VortexColumnPlanner(const std::shared_ptr<VortexColumnGroup>& column_group,
                        std::string_view field_name)
        : ColumnPlanner(Boundaries(column_group)), column_group_(column_group) {
        const auto& files = column_group_->files();
        file_planner_delegates_.reserve(files.size());
        for (size_t file_id = 0; file_id < files.size(); ++file_id) {
            file_planner_delegates_.emplace_back(
                column_group_->FieldPlanner(file_id, field_name));
        }
        AssertInfo(
            static_cast<int64_t>(file_planner_delegates_.size()) == NumCells(),
            "vortex Column planner files {} do not match logical "
            "sources {}",
            file_planner_delegates_.size(),
            NumCells());
    }

    const milvus_storage::vortex::VortexPlanner&
    FileDelegate(int64_t file_id) const {
        AssertInfo(
            file_id >= 0 &&
                file_id < static_cast<int64_t>(file_planner_delegates_.size()),
            "vortex planner file {} out of range {}",
            file_id,
            file_planner_delegates_.size());
        return *file_planner_delegates_[file_id];
    }

    milvus_storage::vortex::VortexPlan
    PlanRowRange(int64_t file_id,
                 uint64_t row_start,
                 uint64_t row_end,
                 const std::string& predicate) const {
        auto result = FileDelegate(file_id).PlanForRowRange(
            row_start, row_end, predicate);
        if (!result.ok()) {
            ThrowVortexStatus(
                result.status(),
                ErrorCode::UnexpectedError,
                fmt::format("failed to plan vortex file {} row range [{}, {})",
                            file_id,
                            row_start,
                            row_end));
        }
        return std::move(result).ValueOrDie();
    }

    milvus_storage::vortex::VortexPlan
    PlanOffsets(int64_t file_id, const std::vector<int64_t>& offsets) const {
        auto result = FileDelegate(file_id).PlanForOffsets(offsets);
        if (!result.ok()) {
            ThrowVortexStatus(
                result.status(),
                ErrorCode::UnexpectedError,
                fmt::format("failed to plan vortex file {} offsets", file_id));
        }
        return std::move(result).ValueOrDie();
    }

 private:
    static const std::vector<int64_t>&
    Boundaries(const std::shared_ptr<VortexColumnGroup>& column_group) {
        AssertInfo(column_group != nullptr,
                   "vortex Column planner has no ColumnGroup");
        return column_group->num_rows_until_chunk();
    }

    // The base planner borrows the immutable file-boundary table from this
    // shared ColumnGroup generation.
    std::shared_ptr<VortexColumnGroup> column_group_;
    std::vector<std::shared_ptr<milvus_storage::vortex::VortexPlanner>>
        file_planner_delegates_;
};

const VortexColumnPlanner&
AsVortexPlanner(const ColumnPlanner& planner) {
    return static_cast<const VortexColumnPlanner&>(planner);
}

std::optional<VortexReaderRange>
NextVortexReaderRange(const ColumnPlanner& planner,
                      int64_t* scan_pos,
                      int64_t scan_end) {
    AssertInfo(scan_pos != nullptr, "vortex scan position is null");
    while (*scan_pos < scan_end) {
        const auto location = planner.Locate(*scan_pos);
        const auto chunk_id = location.cell_id;
        const auto local_offset = location.cell_offset;
        const auto chunk_start = planner.CellStart(chunk_id);
        const auto chunk_rows = planner.CellRows(chunk_id);
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
                        PackedValidityBuffer* validity,
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
    validity->PushBack(valid);
    *has_invalid = *has_invalid || !valid;
}

void
FinalizeRowIdPayloadOutput(ChunkedColumnInterface::ScanBatch* out,
                           std::shared_ptr<PackedValidityBuffer> validity,
                           bool has_invalid) {
    AssertInfo(validity != nullptr, "row id payload validity owner is null");
    AssertInfo(validity->size == static_cast<int64_t>(out->row_ids.size()),
               "row id payload validity size {} does not match row ids size {}",
               validity->size,
               out->row_ids.size());
    out->row_id_start = out->row_ids.empty() ? 0 : out->row_ids.front();
    out->size = static_cast<int64_t>(out->row_ids.size());
    if (out->row_ids.empty() || !has_invalid) {
        return;
    }
    out->validity = validity->View();
    out->owner = std::move(validity);
}

void
FinalizeAllValidRowIdPayloadOutput(ChunkedColumnInterface::ScanBatch* out) {
    out->row_id_start = out->row_ids.empty() ? 0 : out->row_ids.front();
    out->size = static_cast<int64_t>(out->row_ids.size());
}

}  // namespace

struct VortexColumn::ArrowStringViewHolder {
    std::vector<std::shared_ptr<
        cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>>
        pins;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    PackedValidityBuffer validity;
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
    PackedValidityBuffer validity;
    PackedValidityBuffer data_skipped;

    std::vector<std::string_view> string_views;
    std::vector<Json> json_values;
    std::vector<Array> arrays;
    std::vector<ArrayView> array_views;
    std::vector<ArrayValue> array_values;
    std::vector<ArrayValueView> array_value_views;
};

class VortexTakeResult final : public ChunkedColumnInterface::TakeResult {
 public:
    VortexTakeResult(ChunkedColumnInterface::ValueView values,
                     ValidityView validity,
                     ValidityView data_skipped,
                     std::shared_ptr<void> owner,
                     int64_t size,
                     DataType data_type)
        : values_(values),
          validity_(validity),
          data_skipped_(data_skipped),
          owner_(std::move(owner)),
          size_(size),
          data_type_(data_type) {
        AssertInfo(size_ >= 0, "vortex take result has invalid size {}", size_);
        AssertInfo(CanReadAsTargetType(data_type_, values_.target_type),
                   "vortex take type {} does not match target {}",
                   data_type_,
                   static_cast<int>(values_.target_type));
        AssertInfo(size_ == 0 || values_.data != nullptr,
                   "non-empty vortex take result has no values");
        AssertInfo(values_.offset >= 0,
                   "vortex take result has invalid value offset {}",
                   values_.offset);
        ValidateValueWidth();
    }

    int64_t
    Size() const override {
        return size_;
    }

    ChunkedColumnInterface::TargetType
    GetTargetType() const override {
        return values_.target_type;
    }

    DataType
    GetDataType() const override {
        return data_type_;
    }

    bool
    IsOwned() const override {
        return true;
    }

    ChunkedColumnInterface::OwnedTakeData
    GetOwn() const override {
        return ChunkedColumnInterface::OwnedTakeData{
            values_, validity_, owner_, size_, data_skipped_};
    }

 protected:
    ChunkedColumnInterface::TakeItemState
    PrepareItem(int64_t index, bool read_data) const override {
        return ChunkedColumnInterface::TakeItemState{
            !validity_ || validity_[index],
            read_data && data_skipped_ && data_skipped_[index]};
    }

    const void*
    FixedValueAt(int64_t index) const override {
        return static_cast<const char*>(values_.data) +
               (values_.offset + index) * values_.byte_width;
    }

    std::string_view
    StringViewAt(int64_t index) const override {
        return static_cast<const std::string_view*>(
            values_.data)[values_.offset + index];
    }

    Json
    JsonAt(int64_t index) const override {
        return static_cast<const Json*>(values_.data)[values_.offset + index];
    }

    ArrayView
    ArrayAt(int64_t index) const override {
        return static_cast<const ArrayView*>(
            values_.data)[values_.offset + index];
    }

    ArrayValueView
    ArrayValueAt(int64_t index) const override {
        return static_cast<const ArrayValueView*>(
            values_.data)[values_.offset + index];
    }

 private:
    void
    ValidateValueWidth() const {
        int32_t expected = 0;
        switch (values_.target_type) {
            case TargetType::Bool:
                expected = sizeof(bool);
                break;
            case TargetType::Int8:
                expected = sizeof(int8_t);
                break;
            case TargetType::Int16:
                expected = sizeof(int16_t);
                break;
            case TargetType::Int32:
                expected = sizeof(int32_t);
                break;
            case TargetType::Int64:
                expected = sizeof(int64_t);
                break;
            case TargetType::Float:
                expected = sizeof(float);
                break;
            case TargetType::Double:
                expected = sizeof(double);
                break;
            case TargetType::StringView:
                expected = sizeof(std::string_view);
                break;
            case TargetType::Json:
                expected = sizeof(Json);
                break;
            case TargetType::ArrayView:
                expected = sizeof(ArrayView);
                break;
            case TargetType::ArrayValueView:
                expected = sizeof(ArrayValueView);
                break;
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "unsupported vortex take target {}",
                          static_cast<int>(values_.target_type));
        }
        AssertInfo(values_.byte_width == expected,
                   "vortex take target {} has width {}, expected {}",
                   static_cast<int>(values_.target_type),
                   values_.byte_width,
                   expected);
    }

    ChunkedColumnInterface::ValueView values_;
    ValidityView validity_;
    ValidityView data_skipped_;
    std::shared_ptr<void> owner_;
    int64_t size_;
    DataType data_type_;
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

class VortexRowIdScanCursor final {
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
    Position() const {
        return scan_pos_;
    }

    bool
    NextBatch(int64_t max_rows, ChunkedColumnInterface::ScanBatch* out) {
        AssertInfo(out != nullptr, "vortex row id scan output batch is null");
        AssertInfo(max_rows > 0,
                   "vortex row id scan max rows must be positive, got {}",
                   max_rows);
        ResetRowIdPayloadOutput(out);
        std::shared_ptr<PackedValidityBuffer> validity;
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
                validity = std::make_shared<PackedValidityBuffer>();
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
                    PackedValidityBuffer* validity,
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

class VortexDataBatchReader final {
 public:
    VortexDataBatchReader(
        const VortexColumn* column,
        const VortexReaderRange& range,
        ChunkedColumnInterface::ScanReadMode mode,
        ChunkedColumnInterface::TargetType target_type,
        PinWrapper<std::shared_ptr<arrow::RecordBatchReader>> reader,
        bool result_owns_pin)
        : column_(column),
          mode_(mode),
          target_type_(target_type),
          file_id_(range.chunk_id),
          row_id_start_(range.range_start),
          range_end_(range.range_end),
          reader_(std::move(reader)),
          result_owns_pin_(result_owns_pin) {
        AssertInfo(row_id_start_ >= 0 && row_id_start_ < range_end_ &&
                       range_end_ <= static_cast<int64_t>(column_->NumRows()),
                   "vortex data scan range [{}, {}) out of rows {}",
                   row_id_start_,
                   range_end_,
                   column_->NumRows());
    }

    bool
    Read(ChunkedColumnInterface::ScanBatch* out) {
        AssertInfo(out != nullptr, "vortex data scan output batch is null");
        ResetScanBatchOutput(out);
        while (true) {
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = reader_.get()->ReadNext(&batch);
            if (!status.ok()) {
                ThrowVortexStatus(status,
                                  ErrorCode::DataFormatBroken,
                                  "failed to read vortex data scan batch");
            }
            if (batch == nullptr) {
                return false;
            }
            AssertInfo(batch->num_columns() == 1,
                       "vortex data scan expects one column, got {}",
                       batch->num_columns());
            if (batch->num_rows() == 0) {
                continue;
            }
            AssertInfo(row_id_start_ + batch->num_rows() <= range_end_,
                       "vortex data scan returned rows through {}, beyond "
                       "planned end {}",
                       row_id_start_ + batch->num_rows(),
                       range_end_);
            auto array = PrepareBatchArray(batch->column(0));
            FillOutput(array, batch->num_rows(), out);
            return true;
        }
    }

 private:
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
        std::vector<std::string_view> string_views;
        std::vector<Json> json_values;
        std::vector<Array> arrays;
        std::vector<ArrayView> array_views;
        std::vector<ArrayValue> array_values;
        std::vector<ArrayValueView> array_value_views;
    };

    bool
    IsStringLikeScan() const {
        return target_type_ == ChunkedColumnInterface::TargetType::StringView ||
               target_type_ == ChunkedColumnInterface::TargetType::Json;
    }

    bool
    IsArrayScan() const {
        return target_type_ == ChunkedColumnInterface::TargetType::ArrayView ||
               target_type_ ==
                   ChunkedColumnInterface::TargetType::ArrayValueView;
    }

    std::shared_ptr<arrow::Array>
    PrepareBatchArray(const std::shared_ptr<arrow::Array>& array) const {
        ValidateVortexArrowArray(column_->field_id_,
                                 column_->data_type_,
                                 column_->IsNullable(),
                                 array,
                                 false);
        if (mode_ == ChunkedColumnInterface::ScanReadMode::ValidityOnly) {
            return array;
        }
        if (column_->SupportsDirectDataScan(file_id_)) {
            ValidateVortexArrowArray(column_->field_id_,
                                     column_->data_type_,
                                     column_->IsNullable(),
                                     array,
                                     true);
            return array;
        }

        auto normalized =
            storage::NormalizeExternalArrow(array, column_->field_meta_);
        AssertInfo(
            normalized != nullptr && normalized->length() == array->length(),
            "vortex data scan normalization changed row count from {} "
            "to {}",
            array->length(),
            normalized == nullptr ? -1 : normalized->length());
        ValidateVortexArrowArray(column_->field_id_,
                                 column_->data_type_,
                                 column_->IsNullable(),
                                 normalized,
                                 !IsArrayScan());
        return normalized;
    }

    void
    FillDataPointer(const std::shared_ptr<arrow::Array>& array,
                    const std::shared_ptr<BatchOwner>& owner,
                    ChunkedColumnInterface::ScanBatch* out) const {
        AssertInfo(IsFixedWidthTargetType(target_type_),
                   "vortex primitive scan target {} is not fixed-width",
                   static_cast<int>(target_type_));
        out->values.target_type = target_type_;
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
                        ChunkedColumnInterface::ScanBatch* out) const {
        if (!column_->IsNullable() || array->null_count() == 0) {
            return;
        }
        const auto* bitmap = array->null_bitmap_data();
        AssertInfo(bitmap != nullptr,
                   "nullable vortex array with nulls has no validity bitmap");
        out->validity =
            ValidityView::FromPacked(bitmap).Subview(array->offset());
    }

    void
    FillStringLikeOutput(const std::shared_ptr<arrow::Array>& array,
                         const std::shared_ptr<BatchOwner>& owner,
                         ChunkedColumnInterface::ScanBatch* out) const {
        VortexColumn::ArrowStringLikeColumn string_column(array);
        auto views = column_->BuildStringViewsFromArrow(
            string_column, std::make_pair(0, out->size));

        out->values.offset = 0;

        if (target_type_ == ChunkedColumnInterface::TargetType::StringView) {
            owner->string_views = std::move(views);
            out->values.target_type =
                ChunkedColumnInterface::TargetType::StringView;
            out->values.data = owner->string_views.data();
            out->values.byte_width = sizeof(std::string_view);
        } else {
            owner->string_views = std::move(views);
            owner->json_values.reserve(owner->string_views.size());
            for (const auto& value : owner->string_views) {
                owner->json_values.emplace_back(Json(value));
            }
            out->values.target_type = ChunkedColumnInterface::TargetType::Json;
            out->values.data = owner->json_values.data();
            out->values.byte_width = sizeof(Json);
        }

        FillValidityPointer(array, out);
    }

    void
    FillArrayOutput(const std::shared_ptr<arrow::Array>& array,
                    const std::shared_ptr<BatchOwner>& owner,
                    ChunkedColumnInterface::ScanBatch* out) const {
        AssertInfo(array->type_id() == arrow::Type::BINARY,
                   "vortex array scan field {} expected normalized Arrow "
                   "binary, got {}",
                   column_->field_id_.get(),
                   array->type()->ToString());
        VortexColumn::ArrowStringLikeColumn serialized(array);
        const auto validity =
            column_->field_meta_.is_nullable() && array->null_count() > 0
                ? ValidityView::FromPacked(array->null_bitmap_data())
                      .Subview(array->offset())
                : ValidityView{};
        auto views = column_->BuildStringViewsFromArrow(
            serialized, std::make_pair(0, out->size));

        if (target_type_ ==
            ChunkedColumnInterface::TargetType::ArrayValueView) {
            owner->array_values.resize(out->size);
            owner->array_value_views.resize(out->size);
            for (int64_t i = 0; i < out->size; ++i) {
                if (validity && !validity[i]) {
                    continue;
                }
                ScalarFieldProto proto;
                const auto& value = views[i];
                AssertInfo(proto.ParseFromArray(value.data(),
                                                static_cast<int>(value.size())),
                           "failed to parse vortex recursive array scan row {}",
                           out->row_id_start + i);
                owner->array_values[i] = ArrayValue(
                    proto, column_->field_meta_.get_array_type_schema());
                owner->array_value_views[i] = owner->array_values[i].View();
            }
            out->values.target_type =
                ChunkedColumnInterface::TargetType::ArrayValueView;
            out->values.data = owner->array_value_views.data();
            out->values.byte_width = sizeof(ArrayValueView);
        } else {
            owner->arrays.reserve(out->size);
            for (int64_t i = 0; i < out->size; ++i) {
                const auto valid = !validity || validity[i];
                if (!valid) {
                    owner->arrays.emplace_back();
                    continue;
                }
                ScalarFieldProto proto;
                const auto& value = views[i];
                AssertInfo(proto.ParseFromArray(value.data(),
                                                static_cast<int>(value.size())),
                           "failed to parse vortex array scan row {}",
                           out->row_id_start + i);
                owner->arrays.emplace_back(proto);
            }

            owner->array_views.resize(out->size);
            for (int64_t i = 0; i < out->size; ++i) {
                if (validity && !validity[i]) {
                    continue;
                }
                auto& value = owner->arrays[i];
                owner->array_views[i] =
                    ArrayView(const_cast<char*>(value.data()),
                              value.length(),
                              value.byte_size(),
                              value.get_element_type(),
                              value.get_offsets_data());
            }

            out->values.target_type =
                ChunkedColumnInterface::TargetType::ArrayView;
            out->values.data = owner->array_views.data();
            out->values.byte_width = sizeof(ArrayView);
        }
        out->values.offset = 0;

        FillValidityPointer(array, out);
    }

    void
    FillOutput(const std::shared_ptr<arrow::Array>& array,
               int64_t rows_to_return,
               ChunkedColumnInterface::ScanBatch* out) const {
        AssertInfo(array != nullptr,
                   "vortex data scan batch has no prepared Arrow array");
        auto owner = std::make_shared<BatchOwner>();
        if (result_owns_pin_) {
            owner->reader = reader_;
        }
        owner->array = array;
        out->row_id_start = row_id_start_;
        out->size = rows_to_return;
        if (mode_ == ChunkedColumnInterface::ScanReadMode::ValidityOnly) {
            FillValidityPointer(array, out);
        } else if (IsStringLikeScan()) {
            FillStringLikeOutput(array, owner, out);
        } else if (IsArrayScan()) {
            FillArrayOutput(array, owner, out);
        } else {
            FillDataPointer(array, owner, out);
            out->values.offset = 0;
            FillValidityPointer(array, out);
        }
        out->owner = std::move(owner);
    }

    const VortexColumn* column_;
    ChunkedColumnInterface::ScanReadMode mode_;
    ChunkedColumnInterface::TargetType target_type_;
    int64_t file_id_;
    int64_t row_id_start_;
    int64_t range_end_;
    PinWrapper<std::shared_ptr<arrow::RecordBatchReader>> reader_;
    bool result_owns_pin_;
};

class VortexScanCursor final : public ChunkedColumnInterface::ScanCursor {
 public:
    VortexScanCursor(const VortexColumn* column,
                     milvus::OpContext* op_ctx,
                     const ChunkedColumnInterface::ScanOptions& options)
        : column_(column),
          op_ctx_(op_ctx),
          output_(options.output),
          pin_policy_(options.pin_policy),
          filter_(options.filter),
          scan_pos_(options.start_offset) {
        AssertInfo(column_ != nullptr, "vortex scan column is null");
        AssertInfo(scan_pos_ >= 0 &&
                       scan_pos_ <= static_cast<int64_t>(column_->NumRows()),
                   "vortex scan start {} out of rows {}",
                   scan_pos_,
                   column_->NumRows());
        if (output_ == ChunkedColumnInterface::ScanOutput::Data) {
            AssertInfo(options.predicate ==
                           ChunkedColumnInterface::ScanPredicate::None,
                       "vortex data scan does not accept a predicate");
            AssertInfo(options.target_type ==
                               ChunkedColumnInterface::TargetType::None ||
                           CanReadVortexAsTargetType(column_->field_meta_,
                                                     options.target_type),
                       "vortex scan target {} does not match column type {}",
                       static_cast<int>(options.target_type),
                       column_->data_type_);
            target_type_ = options.target_type;
            AssertInfo(
                filter_ == nullptr || filter_->Source() ==
                                          detail::ColumnFilter::MetricsSource::
                                              PreloadedStatistics,
                "vortex scan only supports filters backed by "
                "preloaded statistics");
        } else {
            AssertInfo(filter_ == nullptr,
                       "vortex row id scan does not accept a data filter");
            predicate_ = column_->BuildVortexPredicate(options);
            if (!predicate_.has_value()) {
                ThrowInfo(ErrorCode::Unsupported,
                          "unsupported vortex row id scan predicate for field "
                          "{} type {}",
                          column_->field_id_.get(),
                          static_cast<int>(column_->data_type_));
            }
        }
    }

    int64_t
    Position() const override {
        return scan_pos_;
    }

    void
    Seek(int64_t position) override {
        AssertInfo(position >= scan_pos_,
                   "vortex scan cannot seek backward from {} to {}",
                   scan_pos_,
                   position);
        AssertInfo(position <= static_cast<int64_t>(column_->NumRows()),
                   "vortex scan seek {} out of rows {}",
                   position,
                   column_->NumRows());
        if (position == scan_pos_) {
            return;
        }
        scan_pos_ = position;
        ResetCursorPin();
    }

    bool
    Next(int64_t length,
         ChunkedColumnInterface::ScanReadMode read_mode,
         ChunkedColumnInterface::ScanBatch* out) override {
        AssertInfo(out != nullptr, "vortex scan output batch is null");
        ResetScanBatchOutput(out);
        AssertInfo(
            length >= 0, "vortex scan length {} must be non-negative", length);
        if (length == 0 ||
            scan_pos_ == static_cast<int64_t>(column_->NumRows())) {
            ResetCursorPin();
            return false;
        }
        length = std::min(length,
                          static_cast<int64_t>(column_->NumRows()) - scan_pos_);

        const auto position = scan_pos_;
        if (output_ == ChunkedColumnInterface::ScanOutput::RowIds) {
            AssertInfo(
                read_mode ==
                    ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                "vortex row id scan does not support validity-only mode");
            ReadRowIds(position, length, out);
            scan_pos_ = position + length;
            ResetCursorPin();
            return true;
        }

        AssertInfo(
            read_mode != ChunkedColumnInterface::ScanReadMode::ValidityOnly ||
                column_->IsNullable(),
            "validity-only scan requested for non-nullable column");
        return ReadData(position, length, read_mode, out);
    }

 private:
    VortexCellPin
    PinCells(int64_t chunk_id, const std::vector<uint64_t>& cell_ids) {
        if (cursor_pin_ != nullptr && cursor_chunk_id_ == chunk_id &&
            cursor_cell_ids_ == cell_ids) {
            return cursor_pin_;
        }
        if (cursor_pin_ != nullptr) {
            ResetCursorPin();
        }
        return column_->PinPlanCells(op_ctx_, chunk_id, cell_ids);
    }

    bool
    ReadData(int64_t position,
             int64_t length,
             ChunkedColumnInterface::ScanReadMode mode,
             ChunkedColumnInterface::ScanBatch* out) {
        auto range_pos = position;
        auto range = NextVortexReaderRange(
            column_->Planner(), &range_pos, position + length);
        AssertInfo(range.has_value(),
                   "vortex data scan has no reader range at {}",
                   position);
        const auto data_skipped =
            mode == ChunkedColumnInterface::ScanReadMode::DataAndValidity &&
            filter_ != nullptr && ShouldSkipCell(range->chunk_id);
        if (data_skipped && !column_->IsNullable()) {
            out->row_id_start = position;
            out->size = range->length;
            out->data_skipped = true;
            scan_pos_ = position + out->size;
            ResetCursorPin();
            return true;
        }

        auto plan = column_->PlanRowRange(
            range->chunk_id,
            static_cast<uint64_t>(range->local_offset),
            static_cast<uint64_t>(range->local_offset + range->length));
        auto pin = PinCells(range->chunk_id, plan.cell_ids);
        auto reader = column_->OpenDataScanWithPlan(range->chunk_id, plan, pin);
        VortexDataBatchReader data_reader(
            column_,
            *range,
            data_skipped ? ChunkedColumnInterface::ScanReadMode::ValidityOnly
                         : mode,
            target_type_,
            std::move(reader),
            pin_policy_ == ChunkedColumnInterface::ScanPinPolicy::ResultOwned);
        const auto returned = data_reader.Read(out);
        AssertInfo(returned && out->row_id_start == position && out->size > 0 &&
                       out->size <= length,
                   "invalid vortex data scan batch [{}, {}) for request "
                   "[{}, {})",
                   out->row_id_start,
                   out->row_id_start + out->size,
                   position,
                   position + length);
        out->data_skipped = data_skipped;
        scan_pos_ = position + out->size;
        RememberCursorPin(range->chunk_id, plan.cell_ids, pin);
        return true;
    }

    void
    ReadRowIds(int64_t position,
               int64_t length,
               ChunkedColumnInterface::ScanBatch* out) {
        AssertInfo(predicate_.has_value(),
                   "vortex row id scan has no predicate");
        std::vector<VortexRowIdScanSource> sources;
        auto range_pos = position;
        const auto scan_end = position + length;
        while (auto range = NextVortexReaderRange(
                   column_->Planner(), &range_pos, scan_end)) {
            const auto row_start = static_cast<uint64_t>(range->local_offset);
            const auto row_end =
                static_cast<uint64_t>(range->local_offset + range->length);
            auto matched_plan = column_->PlanRowRange(
                range->chunk_id, row_start, row_end, *predicate_);
            std::optional<milvus_storage::vortex::VortexPlan> validity_plan;
            auto cell_ids = matched_plan.cell_ids;
            if (column_->IsNullable()) {
                validity_plan =
                    column_->PlanRowRange(range->chunk_id, row_start, row_end);
                cell_ids = MergeCellIds(matched_plan.cell_ids,
                                        validity_plan->cell_ids);
            }
            auto pin =
                column_->PinPlanCells(op_ctx_, range->chunk_id, cell_ids);
            auto matched_reader = column_->OpenRowIdScanWithPlan(
                range->chunk_id, matched_plan, pin);
            std::optional<PinWrapper<std::shared_ptr<arrow::RecordBatchReader>>>
                validity_reader;
            if (validity_plan.has_value()) {
                validity_reader = column_->OpenDataScanWithPlan(
                    range->chunk_id, *validity_plan, pin);
            }
            sources.emplace_back(VortexRowIdScanSource{
                *range, std::move(matched_reader), std::move(validity_reader)});
            range_pos = range->range_end;
        }
        VortexRowIdScanCursor reader(
            column_, position, length, std::move(sources));
        auto validity = std::make_shared<PackedValidityBuffer>();
        bool has_invalid = false;
        ChunkedColumnInterface::ScanBatch batch;
        while (reader.NextBatch(std::max<int64_t>(length, 1), &batch)) {
            for (int64_t i = 0; i < batch.size; ++i) {
                AppendRowIdPayloadEntry(out,
                                        validity.get(),
                                        &has_invalid,
                                        batch.row_ids[i],
                                        !batch.validity || batch.validity[i]);
            }
        }
        AssertInfo(reader.Position() == scan_end,
                   "vortex row id scan ended at {}, expected {}",
                   reader.Position(),
                   scan_end);
        FinalizeRowIdPayloadOutput(out, std::move(validity), has_invalid);
    }

    void
    RememberCursorPin(int64_t chunk_id,
                      const std::vector<uint64_t>& cell_ids,
                      const VortexCellPin& pin) {
        if (pin_policy_ != ChunkedColumnInterface::ScanPinPolicy::CursorOwned) {
            ResetCursorPin();
            return;
        }
        cursor_chunk_id_ = chunk_id;
        cursor_cell_ids_ = cell_ids;
        cursor_pin_ = pin;
    }

    void
    ResetCursorPin() {
        cursor_pin_.reset();
        cursor_chunk_id_ = -1;
        cursor_cell_ids_.clear();
    }

    bool
    ShouldSkipCell(int64_t cell_id) {
        AssertInfo(filter_ != nullptr,
                   "vortex scan has no filter for Cell {}",
                   cell_id);
        if (filter_cell_id_ != cell_id) {
            filter_cell_id_ = cell_id;
            filter_cell_skipped_ = filter_->CanSkipPhysicalCell(cell_id);
        }
        return filter_cell_skipped_;
    }

    const VortexColumn* column_;
    milvus::OpContext* op_ctx_;
    ChunkedColumnInterface::ScanOutput output_;
    ChunkedColumnInterface::TargetType target_type_{
        ChunkedColumnInterface::TargetType::None};
    ChunkedColumnInterface::ScanPinPolicy pin_policy_;
    detail::ColumnFilterPtr filter_;
    std::optional<std::string> predicate_;
    int64_t scan_pos_{0};
    VortexCellPin cursor_pin_;
    int64_t cursor_chunk_id_{-1};
    std::vector<uint64_t> cursor_cell_ids_;
    int64_t filter_cell_id_{-1};
    bool filter_cell_skipped_{false};
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
}

VortexColumn::~VortexColumn() = default;

std::unique_ptr<ColumnPlanner>
VortexColumn::BuildPlanner() const {
    return std::make_unique<VortexColumnPlanner>(column_group_, field_name_);
}

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
    return Planner().NumRows();
}

int64_t
VortexColumn::num_chunks() const {
    return Planner().NumCells();
}

size_t
VortexColumn::DataByteSize() const {
    return data_byte_size_;
}

int64_t
VortexColumn::chunk_row_nums(int64_t chunk_id) const {
    CheckChunkId(chunk_id);
    return Planner().CellRows(chunk_id);
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
            const auto num_rows = Planner().NumRows();
            for (int64_t i = 0; i < num_rows; ++i) {
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
        std::vector<uint64_t> cell_ids;
        const auto num_cells =
            AsVortexPlanner(Planner()).FileDelegate(chunk_id).num_cells();
        cell_ids.reserve(num_cells);
        for (size_t cell_id = 0; cell_id < num_cells; ++cell_id) {
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

        auto plan = PlanOffsets(chunk_id, chunk_offsets);
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

PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
VortexColumn::StringViews(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    if (!IsChunkedVariableColumnDataType(data_type_)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::StringViews only supports variable fields");
    }
    if (!SupportsDirectDataScan(chunk_id)) {
        auto chunk = MaterializeChunk(op_ctx, chunk_id, offset_len);
        auto views =
            static_cast<StringChunk*>(chunk.get())->StringViews(std::nullopt);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, ValidityView>>(
            chunk, std::move(views));
    }
    auto [holder, views] =
        ScanStringLikeViewsFromFile(op_ctx, chunk_id, offset_len);
    return PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>(
        std::move(holder), std::move(views));
}

PinWrapper<std::pair<std::vector<ArrayView>, ValidityView>>
VortexColumn::ArrayViews(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    if (!IsChunkedArrayColumnDataType(data_type_)) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::ArrayViews only supports array fields");
    }
    if (field_meta_.is_nested_array()) {
        ThrowInfo(ErrorCode::Unsupported,
                  "legacy ArrayViews API does not support nested ARRAY");
    }
    auto chunk = MaterializeChunk(op_ctx, chunk_id, offset_len);
    auto views = static_cast<ArrayChunk*>(chunk.get())->Views({});
    return PinWrapper<std::pair<std::vector<ArrayView>, ValidityView>>(
        chunk, std::move(views));
}

PinWrapper<std::pair<std::vector<ArrayValueView>, ValidityView>>
VortexColumn::ArrayValueViews(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    if (!IsChunkedArrayColumnDataType(data_type_) ||
        !field_meta_.is_nested_array()) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::ArrayValueViews only supports recursive "
                  "ARRAY fields");
    }
    auto chunk = MaterializeChunk(op_ctx, chunk_id, offset_len);
    auto* array_chunk = dynamic_cast<ColumnarArrayChunk*>(chunk.get());
    AssertInfo(array_chunk != nullptr,
               "vortex recursive ARRAY chunk {} must use ColumnarArrayChunk",
               chunk_id);
    auto views = array_chunk->Views();
    return PinWrapper<std::pair<std::vector<ArrayValueView>, ValidityView>>(
        chunk, std::move(views));
}

PinWrapper<std::pair<std::vector<VectorArrayView>, ValidityView>>
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
    const auto chunk_start = Planner().CellStart(chunk_id);
    const auto chunk_rows = Planner().CellRows(chunk_id);
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto offset = offsets[i];
        AssertInfo(offset >= 0 && offset < chunk_rows,
                   "vortex chunk-local offset {} out of chunk {} rows {}",
                   offset,
                   chunk_id,
                   chunk_rows);
        global_offsets[i] = chunk_start + offset;
    }

    const auto target_type = data_type_ == DataType::JSON
                                 ? TargetType::Json
                                 : TargetType::StringView;
    auto take = Take(op_ctx,
                     TakeOptions{OffsetView::From(global_offsets.data(),
                                                  global_offsets.size()),
                                 target_type});
    AssertInfo(take != nullptr,
               "vortex string view take is unsupported for type {}",
               data_type_);
    AssertInfo(take->Size() == static_cast<int64_t>(offsets.size()),
               "vortex string view take returned {} rows, expected {}",
               take->Size(),
               offsets.size());
    if (data_type_ == DataType::JSON) {
        const auto items = take->Access<Json>();
        for (size_t i = 0; i < offsets.size(); ++i) {
            const auto item = items[i];
            if (item.value.has_value()) {
                views.first[i] = std::string_view(*item.value);
            }
            views.second[i] = item.is_valid;
        }
    } else {
        const auto items = take->Access<std::string_view>();
        for (size_t i = 0; i < offsets.size(); ++i) {
            const auto item = items[i];
            if (item.value.has_value()) {
                views.first[i] = *item.value;
            }
            views.second[i] = item.is_valid;
        }
    }
    auto owned = take->GetOwn();
    return PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
        std::move(owned.owner), std::move(views));
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
    if (field_meta_.is_nested_array()) {
        ThrowInfo(
            ErrorCode::Unsupported,
            "legacy ArrayViewsByOffsets API does not support nested ARRAY");
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
    const auto chunk_start = Planner().CellStart(chunk_id);
    const auto chunk_rows = Planner().CellRows(chunk_id);
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto offset = offsets[i];
        AssertInfo(offset >= 0 && offset < chunk_rows,
                   "vortex chunk-local offset {} out of chunk {} rows {}",
                   offset,
                   chunk_id,
                   chunk_rows);
        global_offsets[i] = chunk_start + offset;
    }

    auto take = Take(op_ctx,
                     TakeOptions{OffsetView::From(global_offsets.data(),
                                                  global_offsets.size()),
                                 TargetType::ArrayView});
    AssertInfo(take != nullptr, "vortex array view take is unsupported");
    AssertInfo(take->Size() == static_cast<int64_t>(offsets.size()),
               "vortex array view take returned {} rows, expected {}",
               take->Size(),
               offsets.size());
    const auto items = take->Access<ArrayView>();
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto item = items[i];
        if (item.value.has_value()) {
            views.first[i] = *item.value;
        }
        views.second[i] = item.is_valid;
    }
    auto owned = take->GetOwn();
    return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
        std::move(owned.owner), std::move(views));
}

PinWrapper<std::pair<std::vector<ArrayValueView>, FixedVector<bool>>>
VortexColumn::ArrayValueViewsByOffsets(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    if (!IsChunkedArrayColumnDataType(data_type_) ||
        !field_meta_.is_nested_array()) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::ArrayValueViewsByOffsets only supports "
                  "recursive ARRAY fields");
    }
    CheckChunkId(chunk_id);
    std::pair<std::vector<ArrayValueView>, FixedVector<bool>> views;
    views.first.resize(offsets.size());
    views.second.resize(offsets.size());
    if (offsets.empty()) {
        return PinWrapper<
            std::pair<std::vector<ArrayValueView>, FixedVector<bool>>>(
            std::move(views));
    }

    std::vector<int64_t> global_offsets(offsets.size());
    const auto chunk_start = Planner().CellStart(chunk_id);
    const auto chunk_rows = Planner().CellRows(chunk_id);
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto offset = offsets[i];
        AssertInfo(offset >= 0 && offset < chunk_rows,
                   "vortex chunk-local offset {} out of chunk {} rows {}",
                   offset,
                   chunk_id,
                   chunk_rows);
        global_offsets[i] = chunk_start + offset;
    }

    auto take = Take(op_ctx,
                     TakeOptions{OffsetView::From(global_offsets.data(),
                                                  global_offsets.size()),
                                 TargetType::ArrayValueView});
    AssertInfo(take != nullptr,
               "vortex recursive array view take is unsupported");
    AssertInfo(take->Size() == static_cast<int64_t>(offsets.size()),
               "vortex recursive array view take returned {} rows, expected "
               "{}",
               take->Size(),
               offsets.size());
    const auto items = take->Access<ArrayValueView>();
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto item = items[i];
        if (item.value.has_value()) {
            views.first[i] = *item.value;
        }
        views.second[i] = item.is_valid;
    }
    auto owned = take->GetOwn();
    return PinWrapper<
        std::pair<std::vector<ArrayValueView>, FixedVector<bool>>>(
        std::move(owned.owner), std::move(views));
}

std::pair<size_t, size_t>
VortexColumn::GetChunkIDByOffset(int64_t offset) const {
    const auto location = Planner().Locate(offset);
    return {static_cast<size_t>(location.cell_id),
            static_cast<size_t>(location.cell_offset)};
}

std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
VortexColumn::GetChunkIDsByOffsets(const int64_t* offsets,
                                   int64_t count) const {
    AssertInfo(count >= 0, "offset count must be non-negative, got {}", count);
    AssertInfo(count == 0 || offsets != nullptr,
               "offsets are null with count {}",
               count);
    std::vector<milvus::cachinglayer::cid_t> cell_ids;
    std::vector<int64_t> cell_offsets;
    cell_ids.reserve(count);
    cell_offsets.reserve(count);
    const auto& planner = Planner();
    for (int64_t i = 0; i < count; ++i) {
        const auto location = planner.Locate(offsets[i]);
        cell_ids.emplace_back(
            static_cast<milvus::cachinglayer::cid_t>(location.cell_id));
        cell_offsets.emplace_back(location.cell_offset);
    }
    return {std::move(cell_ids), std::move(cell_offsets)};
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
    const auto chunk_rows = Planner().CellRows(chunk_id);
    AssertInfo(offset >= 0 && size >= 0 && offset + size <= chunk_rows,
               "vortex valid-data range [{}, {}) out of chunk rows {}",
               offset,
               offset + size,
               chunk_rows);

    const auto global_start = Planner().CellStart(chunk_id) + offset;
    auto cursor =
        Scan(op_ctx, ScanOptions::ForData(global_start, TargetType::None));
    AssertInfo(cursor != nullptr,
               "failed to create vortex validity scan for field {} chunk {}",
               field_id_.get(),
               chunk_id);

    int64_t processed = 0;
    while (processed < size) {
        ScanBatch batch;
        const auto returned =
            cursor->Next(size - processed, ScanReadMode::ValidityOnly, &batch);
        AssertInfo(returned,
                   "vortex validity scan ended after {} of {} rows",
                   processed,
                   size);
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
            if (batch.validity && !batch.validity[i]) {
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
    return Planner().CellStart(chunk_id);
}

const std::vector<int64_t>&
VortexColumn::GetNumRowsUntilChunk() const {
    return Planner().CellBoundaries();
}

void
VortexColumn::BulkValueAt(milvus::OpContext* op_ctx,
                          std::function<void(const char*, size_t)> fn,
                          const int64_t* offsets,
                          int64_t count) {
    if (count == 0) {
        return;
    }
    const auto target_type = PrimitiveTargetType(data_type_);
    auto take = Take(
        op_ctx, TakeOptions{OffsetView::From(offsets, count), target_type});
    AssertInfo(take != nullptr,
               "vortex bulk value take is unsupported for type {}",
               data_type_);
    AssertInfo(take->Size() == count,
               "vortex bulk value take returned {} rows, expected {}",
               take->Size(),
               count);
    auto owned = take->GetOwn();
    AssertInfo(owned.values.target_type == target_type,
               "vortex bulk value expected target {}, got {}",
               static_cast<int>(target_type),
               static_cast<int>(owned.values.target_type));
    const auto* data = static_cast<const char*>(owned.values.data);
    for (int64_t i = 0; i < count; ++i) {
        fn(data + (owned.values.offset + i) * owned.values.byte_width, i);
    }
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

VortexColumn::FileState
VortexColumn::BuildFileState(
    const VortexColumnGroup::FileState& group_file) const {
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
    return state;
}

bool
VortexColumn::SupportsDirectDataScan(int64_t file_id) const {
    AssertInfo(file_id >= 0 && file_id < static_cast<int64_t>(files_.size()),
               "vortex file {} out of range {}",
               file_id,
               files_.size());
    return files_[file_id].direct_data_scan;
}

std::vector<std::string_view>
VortexColumn::BuildStringViewsFromArrow(
    const ArrowStringLikeColumn& column,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
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

    std::vector<std::string_view> views;
    views.reserve(length);
    for (int64_t i = 0; i < length; ++i) {
        const auto row = start + i;
        views.emplace_back(column.ValueAt(row));
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
VortexColumn::PlanRowRange(int64_t file_id,
                           uint64_t row_start,
                           uint64_t row_end,
                           const std::string& predicate) const {
    return AsVortexPlanner(Planner()).PlanRowRange(
        file_id, row_start, row_end, predicate);
}

milvus_storage::vortex::VortexPlan
VortexColumn::PlanOffsets(int64_t file_id,
                          const std::vector<int64_t>& offsets) const {
    return AsVortexPlanner(Planner()).PlanOffsets(file_id, offsets);
}

std::shared_ptr<Chunk>
VortexColumn::MaterializeChunk(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    CheckChunkId(chunk_id);
    const auto chunk_rows = Planner().CellRows(chunk_id);
    int64_t start = 0;
    int64_t length = chunk_rows;
    if (offset_len.has_value()) {
        start = offset_len->first;
        length = offset_len->second;
        AssertInfo(start >= 0 && length >= 0 && start + length <= chunk_rows,
                   "vortex materialize range [{}, {}) out of chunk rows {}",
                   start,
                   start + length,
                   chunk_rows);
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
    const auto file_rows = Planner().CellRows(chunk_id);
    AssertInfo(
        start_offset >= 0 && length >= 0 && start_offset + length <= file_rows,
        "vortex data scan range [{}, {}) out of chunk rows {}",
        start_offset,
        start_offset + length,
        file_rows);
    auto plan = PlanRowRange(chunk_id,
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
          std::pair<std::vector<std::string_view>, ValidityView>>
VortexColumn::ScanStringLikeViewsFromFile(
    milvus::OpContext* op_ctx,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    CheckChunkId(chunk_id);
    const auto chunk_rows = Planner().CellRows(chunk_id);
    int64_t start = 0;
    int64_t length = chunk_rows;
    if (offset_len.has_value()) {
        start = offset_len->first;
        length = offset_len->second;
        AssertInfo(
            start >= 0 && length >= 0 && start + length <= chunk_rows,
            "vortex string-like scan range [{}, {}) out of chunk rows {}",
            start,
            start + length,
            chunk_rows);
    }

    auto holder = std::make_shared<ArrowStringViewHolder>();
    std::pair<std::vector<std::string_view>, ValidityView> views;
    views.first.reserve(length);
    if (length == 0) {
        return {std::move(holder), std::move(views)};
    }

    auto plan = PlanRowRange(chunk_id,
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
        auto batch_views = BuildStringViewsFromArrow(column, std::nullopt);
        views.first.insert(
            views.first.end(), batch_views.begin(), batch_views.end());
        if (field_meta_.is_nullable()) {
            for (int64_t i = 0; i < column.length(); ++i) {
                holder->validity.PushBack(column.IsValid(i));
            }
        }
        holder->batches.emplace_back(std::move(batch));
    }
    holder->pins.emplace_back(std::move(pin));
    AssertInfo(static_cast<int64_t>(views.first.size()) == length,
               "vortex string-like scan returned {} rows, expected {}",
               views.first.size(),
               length);
    if (field_meta_.is_nullable()) {
        views.second = holder->validity.View();
    }
    return {std::move(holder), std::move(views)};
}

std::shared_ptr<arrow::Table>
VortexColumn::TakeArrowFromFile(milvus::OpContext* op_ctx,
                                int64_t file_id,
                                const std::vector<int64_t>& offsets) const {
    CheckChunkId(file_id);
    const auto& file = column_group_->files()[file_id];
    std::vector<int64_t> file_offsets;
    file_offsets.reserve(offsets.size());
    for (auto offset : offsets) {
        AssertInfo(offset >= file.start_index && offset < file.end_index,
                   "vortex take offset {} is outside file {} range [{}, {})",
                   offset,
                   file_id,
                   file.start_index,
                   file.end_index);
        file_offsets.emplace_back(offset - file.start_index);
    }

    auto plan = PlanOffsets(file_id, file_offsets);
    // Take fully materializes decoded Arrow buffers. The cell pin only needs
    // to protect the reader through import and is released on return.
    auto pin = PinPlanCells(op_ctx, file_id, plan.cell_ids);
    auto vortex_reader = BuildFileReader(file);
    auto table_result = vortex_reader->take(file_offsets);
    if (!table_result.ok()) {
        ThrowVortexStatus(table_result.status(),
                          ErrorCode::DataFormatBroken,
                          fmt::format("failed to take vortex field {} chunk {}",
                                      field_id_.get(),
                                      file_id));
    }
    auto table = std::move(table_result).ValueOrDie();
    AssertInfo(table->num_columns() == 1,
               "vortex take field {} expected one column, got {}",
               field_id_.get(),
               table->num_columns());
    for (const auto& array : table->column(0)->chunks()) {
        ValidateVortexArrowArray(
            field_id_, data_type_, IsNullable(), array, false);
    }
    return table;
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

ChunkedColumnInterface::TakeResultPtr
VortexColumn::Take(milvus::OpContext* op_ctx, TakeOptions options) const {
    const auto& offsets = options.offsets;
    AssertInfo(offsets.size >= 0,
               "take offset count must be non-negative, got {}",
               offsets.size);
    if (offsets.size > 0) {
        AssertInfo(offsets.data != nullptr,
                   "take offsets are null with count {}",
                   offsets.size);
    }
    const auto target_type = options.target_type;
    AssertInfo(options.filter == nullptr ||
                   options.filter->Source() ==
                       detail::ColumnFilter::MetricsSource::PreloadedStatistics,
               "vortex take only supports filters backed by preloaded "
               "statistics");
    AssertInfo(target_type != TargetType::None,
               "vortex take target type must be specified");
    AssertInfo(CanReadVortexAsTargetType(field_meta_, target_type),
               "vortex take target {} does not match column type {}",
               static_cast<int>(target_type),
               data_type_);

    struct OffsetEntry {
        int64_t segment_offset;
        int64_t original_position;
    };
    struct OffsetGroup {
        int64_t file_id;
        std::vector<int64_t> unique_offsets;
        std::vector<int64_t> original_positions;
        std::vector<int64_t> original_position_ends;
    };

    auto owner = std::make_shared<OrderedTakeOwner>();
    if (IsNullable()) {
        owner->validity.Resize(offsets.size, true);
    }
    const auto has_filter = options.filter != nullptr;
    if (has_filter) {
        owner->data_skipped.Resize(offsets.size, false);
    }
    bool has_data_skipped = false;

    std::vector<OffsetEntry> entries;
    entries.reserve(offsets.size);
    for (int64_t i = 0; i < offsets.size; ++i) {
        entries.emplace_back(OffsetEntry{offsets[i], i});
    }
    std::sort(entries.begin(),
              entries.end(),
              [](const OffsetEntry& left, const OffsetEntry& right) {
                  return std::tie(left.segment_offset, left.original_position) <
                         std::tie(right.segment_offset,
                                  right.original_position);
              });

    std::vector<OffsetGroup> groups;
    const auto& files = column_group_->files();
    size_t file_id = 0;
    int64_t planned_cell_id = -1;
    bool planned_cell_skipped = false;
    for (const auto& entry : entries) {
        if (has_filter) {
            const auto cell_id = Planner().Locate(entry.segment_offset).cell_id;
            if (cell_id != planned_cell_id) {
                planned_cell_skipped =
                    options.filter->CanSkipPhysicalCell(cell_id);
                planned_cell_id = cell_id;
            }
            owner->data_skipped.Set(entry.original_position,
                                    planned_cell_skipped);
            has_data_skipped = has_data_skipped || planned_cell_skipped;
        }
        while (file_id < files.size() &&
               entry.segment_offset >= files[file_id].end_index) {
            ++file_id;
        }
        AssertInfo(file_id < files.size() &&
                       entry.segment_offset >= files[file_id].start_index,
                   "vortex take offset {} is outside column rows {}",
                   entry.segment_offset,
                   NumRows());
        if (groups.empty() ||
            groups.back().file_id != static_cast<int64_t>(file_id)) {
            groups.emplace_back(
                OffsetGroup{static_cast<int64_t>(file_id), {}, {}, {}});
        }
        auto& group = groups.back();
        if (group.unique_offsets.empty() ||
            group.unique_offsets.back() != entry.segment_offset) {
            group.unique_offsets.emplace_back(entry.segment_offset);
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

    auto retain_take = [&](const std::shared_ptr<arrow::Table>& take,
                           const std::shared_ptr<arrow::Table>& table) {
        owner->tables.emplace_back(take);
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
                        owner->validity.Set(original_position, valid);
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
            output.resize(offsets.size);
            for (const auto& group : groups) {
                auto take = TakeArrowFromFile(
                    op_ctx, group.file_id, group.unique_offsets);
                auto table = normalize && !SupportsDirectDataScan(group.file_id)
                                 ? normalize_table(take)
                                 : take;
                CopyArrowPrimitiveValues<ArrowArrayT, ValueT, ValueT, ValueT>(
                    output.data(),
                    table,
                    group.original_positions,
                    group.original_position_ends,
                    true);
                copy_validity(table, group);
                retain_take(take, table);
            }
        };

    ValueView values;
    values.offset = 0;

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
                owner->int64_values, true);
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
            owner->string_views.resize(offsets.size);
            for (const auto& group : groups) {
                auto take = TakeArrowFromFile(
                    op_ctx, group.file_id, group.unique_offsets);
                auto table = !SupportsDirectDataScan(group.file_id)
                                 ? normalize_table(take)
                                 : take;
                ArrowStringLikeColumn column(table);
                auto unique_views =
                    BuildStringViewsFromArrow(column, std::nullopt);
                AssertInfo(unique_views.size() == group.unique_offsets.size(),
                           "vortex take returned {} string-like rows, "
                           "expected {}",
                           unique_views.size(),
                           group.unique_offsets.size());
                for (size_t i = 0; i < group.unique_offsets.size(); ++i) {
                    const auto valid = !IsNullable() || column.IsValid(i);
                    for_each_original_position(
                        group, i, [&](auto original_position) {
                            owner->string_views[original_position] =
                                unique_views[i];
                            if (IsNullable()) {
                                owner->validity.Set(original_position, valid);
                            }
                        });
                }
                retain_take(take, table);
            }
            if (data_type_ == DataType::JSON) {
                owner->json_values.reserve(owner->string_views.size());
                for (const auto& value : owner->string_views) {
                    owner->json_values.emplace_back(value);
                }
                values.target_type = TargetType::Json;
                values.data = owner->json_values.data();
                values.byte_width = sizeof(Json);
            } else {
                values.target_type = TargetType::StringView;
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
            if (target_type == TargetType::ArrayValueView) {
                owner->array_values.reserve(unique_count);
            } else {
                owner->arrays.reserve(unique_count);
            }
            std::vector<int64_t> ordered_array_indices(offsets.size, -1);
            for (const auto& group : groups) {
                auto take = TakeArrowFromFile(
                    op_ctx, group.file_id, group.unique_offsets);
                auto table = normalize_table(take);
                ArrowStringLikeColumn column(table);
                auto serialized =
                    BuildStringViewsFromArrow(column, std::nullopt);
                AssertInfo(serialized.size() == group.unique_offsets.size(),
                           "vortex take returned {} array rows, expected {}",
                           serialized.size(),
                           group.unique_offsets.size());
                for (size_t i = 0; i < group.unique_offsets.size(); ++i) {
                    const auto array_index = static_cast<int64_t>(
                        target_type == TargetType::ArrayValueView
                            ? owner->array_values.size()
                            : owner->arrays.size());
                    const auto valid = !IsNullable() || column.IsValid(i);
                    if (valid) {
                        ScalarFieldProto proto;
                        const auto& value = serialized[i];
                        AssertInfo(
                            proto.ParseFromArray(
                                value.data(), static_cast<int>(value.size())),
                            "failed to parse vortex array take row");
                        if (target_type == TargetType::ArrayValueView) {
                            owner->array_values.emplace_back(
                                proto, field_meta_.get_array_type_schema());
                        } else {
                            owner->arrays.emplace_back(proto);
                        }
                    } else if (target_type == TargetType::ArrayValueView) {
                        owner->array_values.emplace_back();
                    } else {
                        owner->arrays.emplace_back();
                    }
                    for_each_original_position(
                        group, i, [&](auto original_position) {
                            ordered_array_indices[original_position] =
                                array_index;
                            if (IsNullable()) {
                                owner->validity.Set(original_position, valid);
                            }
                        });
                }
                retain_take(take, table);
            }

            if (target_type == TargetType::ArrayValueView) {
                owner->array_value_views.resize(offsets.size);
                for (int64_t i = 0; i < offsets.size; ++i) {
                    if (IsNullable() && !owner->validity.Get(i)) {
                        continue;
                    }
                    const auto array_index = ordered_array_indices[i];
                    AssertInfo(
                        array_index >= 0 &&
                            array_index < static_cast<int64_t>(
                                              owner->array_values.size()),
                        "vortex recursive array take mapping {} out of range "
                        "{}",
                        array_index,
                        owner->array_values.size());
                    owner->array_value_views[i] =
                        owner->array_values[array_index].View();
                }
                values.target_type = TargetType::ArrayValueView;
                values.data = owner->array_value_views.data();
                values.byte_width = sizeof(ArrayValueView);
            } else {
                owner->array_views.resize(offsets.size);
                for (int64_t i = 0; i < offsets.size; ++i) {
                    if (IsNullable() && !owner->validity.Get(i)) {
                        continue;
                    }
                    const auto array_index = ordered_array_indices[i];
                    AssertInfo(array_index >= 0 &&
                                   array_index < static_cast<int64_t>(
                                                     owner->arrays.size()),
                               "vortex array take mapping {} out of range {}",
                               array_index,
                               owner->arrays.size());
                    auto& array = owner->arrays[array_index];
                    owner->array_views[i] =
                        ArrayView(const_cast<char*>(array.data()),
                                  array.length(),
                                  array.byte_size(),
                                  array.get_element_type(),
                                  array.get_offsets_data());
                }
                values.target_type = TargetType::ArrayView;
                values.data = owner->array_views.data();
                values.byte_width = sizeof(ArrayView);
            }
            break;
        }
        default:
            return nullptr;
    }

    if (values.target_type == TargetType::None) {
        AssertInfo(IsFixedWidthTargetType(target_type),
                   "vortex primitive take target {} is not fixed-width",
                   static_cast<int>(target_type));
        values.target_type = target_type;
    }
    const auto validity =
        IsNullable() ? owner->validity.View() : ValidityView{};
    const auto data_skipped =
        has_data_skipped ? owner->data_skipped.View() : ValidityView{};
    return std::make_unique<VortexTakeResult>(values,
                                              validity,
                                              data_skipped,
                                              std::move(owner),
                                              offsets.size,
                                              data_type_);
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
    const auto target_type = PrimitiveTargetType(data_type_);
    auto take = Take(
        op_ctx, TakeOptions{OffsetView::From(offsets, count), target_type});
    AssertInfo(take != nullptr,
               "vortex primitive take is unsupported for type {}",
               data_type_);
    AssertInfo(take->Size() == count,
               "vortex primitive take returned {} rows, expected {}",
               take->Size(),
               count);
    auto owned = take->GetOwn();
    AssertInfo(owned.values.target_type == target_type,
               "vortex primitive expected target {}, got {}",
               static_cast<int>(target_type),
               static_cast<int>(owned.values.target_type));

    auto copy_values = [&]<typename SrcT, typename DstT>() {
        const auto* values = owned.values.data_as<SrcT>();
        auto* output = static_cast<DstT*>(dst);
        for (int64_t i = 0; i < count; ++i) {
            output[i] = static_cast<DstT>(values[i]);
        }
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
    if (field_meta_.is_nested_array()) {
        ThrowInfo(ErrorCode::Unsupported,
                  "BulkArrayAt does not support nested ARRAY");
    }
    if (count == 0) {
        return;
    }
    auto take = Take(
        op_ctx,
        TakeOptions{OffsetView::From(offsets, count), TargetType::ArrayView});
    AssertInfo(take != nullptr, "vortex array take is unsupported");
    AssertInfo(take->Size() == count,
               "vortex array take returned {} rows, expected {}",
               take->Size(),
               count);
    const auto items = take->Access<ArrayView>();
    for (int64_t i = 0; i < count; ++i) {
        const auto item = items[i];
        fn(item.value.value_or(ArrayView{}), i);
    }
}

void
VortexColumn::BulkArrayValueAt(
    milvus::OpContext* op_ctx,
    std::function<void(ScalarFieldProto&&, size_t)> fn,
    const int64_t* offsets,
    int64_t count) const {
    if (!IsChunkedArrayColumnDataType(data_type_) ||
        !field_meta_.is_nested_array()) {
        ThrowInfo(ErrorCode::Unsupported,
                  "VortexColumn::BulkArrayValueAt only supports recursive "
                  "ARRAY fields");
    }
    if (count == 0) {
        return;
    }
    auto take = Take(op_ctx,
                     TakeOptions{OffsetView::From(offsets, count),
                                 TargetType::ArrayValueView});
    AssertInfo(take != nullptr, "vortex recursive array take is unsupported");
    AssertInfo(take->Size() == count,
               "vortex recursive array take returned {} rows, expected {}",
               take->Size(),
               count);
    const auto items = take->Access<ArrayValueView>();
    for (int64_t i = 0; i < count; ++i) {
        const auto item = items[i];
        if (!item.value.has_value()) {
            fn(ScalarFieldProto{}, i);
            continue;
        }
        fn(item.value->output_data(), i);
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
        const auto& planner = Planner();
        int64_t global_offset = 0;
        for (int64_t chunk_id = 0; chunk_id < num_chunks(); ++chunk_id) {
            const auto file_rows = planner.CellRows(chunk_id);
            if (!SupportsDirectDataScan(chunk_id)) {
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
            } else {
                auto scan = OpenDataScanForFile(op_ctx, chunk_id, 0, file_rows);
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
                    AssertInfo(
                        batch->num_columns() == 1,
                        "vortex string-like scan expects one column, got {}",
                        batch->num_columns());
                    ValidateVortexArrowArray(field_id_,
                                             data_type_,
                                             IsNullable(),
                                             batch->column(0),
                                             true);
                    ArrowStringLikeColumn column(batch->column(0));
                    for (int64_t i = 0; i < column.length(); ++i) {
                        fn(column.ValueAt(i),
                           global_offset + row_offset + i,
                           column.IsValid(i));
                    }
                    row_offset += column.length();
                }
            }
            global_offset += file_rows;
        }
        return;
    }

    if (count == 0) {
        return;
    }
    const auto target_type = data_type_ == DataType::JSON
                                 ? TargetType::Json
                                 : TargetType::StringView;
    auto take = Take(
        op_ctx, TakeOptions{OffsetView::From(offsets, count), target_type});
    AssertInfo(take != nullptr,
               "vortex string-like take is unsupported for type {}",
               data_type_);
    AssertInfo(take->Size() == count,
               "vortex string-like take returned {} rows, expected {}",
               take->Size(),
               count);
    if (data_type_ == DataType::JSON) {
        const auto items = take->Access<Json>();
        for (int64_t i = 0; i < count; ++i) {
            const auto item = items[i];
            fn(item.value.value_or(Json{}), i, item.is_valid);
        }
    } else {
        const auto items = take->Access<std::string_view>();
        for (int64_t i = 0; i < count; ++i) {
            const auto item = items[i];
            fn(item.value.value_or(std::string_view{}), i, item.is_valid);
        }
    }
}

ChunkedColumnInterface::ScanResult
VortexColumn::Scan(milvus::OpContext* op_ctx,
                   const ScanOptions& options) const {
    AssertInfo(options.start_offset >= 0 &&
                   options.start_offset <= static_cast<int64_t>(NumRows()),
               "vortex scan start {} out of rows {}",
               options.start_offset,
               NumRows());
    if (options.output == ScanOutput::Data) {
        // Dense data scans do not accept pushed-down predicates. Predicates
        // that must be evaluated by Milvus should use a plain data scan and
        // let the expression layer compare values from the returned batches.
        if (options.predicate != ScanPredicate::None) {
            return nullptr;
        }

        return std::make_unique<VortexScanCursor>(this, op_ctx, options);
    }

    // RowId output is only meaningful for pushed-down predicates. Without a
    // predicate there is no sparse result to return, so let callers fall back.
    if (options.predicate == ScanPredicate::None) {
        return nullptr;
    }
    return std::make_unique<VortexScanCursor>(this, op_ctx, options);
}

}  // namespace milvus
