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

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumnFilter.h"
#include "mmap/ChunkedColumnInterface.h"

namespace milvus {
namespace detail {

using RawTakeCellPin = std::function<PinWrapper<Chunk*>(int64_t)>;

struct OwnedTakeStorage {
    std::vector<int8_t> int8_values;
    std::vector<int16_t> int16_values;
    std::vector<int32_t> int32_values;
    std::vector<int64_t> int64_values;
    std::vector<float> float_values;
    std::vector<double> double_values;
    FixedVector<bool> bool_values;

    std::vector<std::string> strings;
    std::vector<std::string_view> string_views;
    std::vector<Json> json_values;
    std::vector<Array> arrays;
    std::vector<ArrayView> array_views;
    std::vector<ArrayValue> array_values;
    std::vector<ArrayValueView> array_value_views;
    FixedVector<bool> validity;
    FixedVector<bool> data_skipped;
};

// TakeResult owns its logical input so callers do not need to retain the
// OffsetView. Keep the original element width and resolve physical locations
// lazily: Raw expression evaluation then uses 4 bytes per int32 offset instead
// of materializing a much larger TakeLocation for every candidate, and masked
// candidates never pay the segment-offset-to-Cell lookup cost.
class OwnedTakeOffsets {
 public:
    OwnedTakeOffsets(const OffsetView& offsets, int64_t num_rows)
        : element_type_(offsets.element_type) {
        AssertInfo(offsets.size >= 0,
                   "take offset count must be non-negative, got {}",
                   offsets.size);
        AssertInfo(offsets.size == 0 || offsets.data != nullptr,
                   "take offsets are null with count {}",
                   offsets.size);
        if (offsets.size == 0) {
            return;
        }
        if (element_type_ == OffsetElementType::Int32) {
            const auto* begin = static_cast<const int32_t*>(offsets.data);
            CopyAndValidate(begin, offsets.size, num_rows, &int32_offsets_);
        } else {
            const auto* begin = static_cast<const int64_t*>(offsets.data);
            CopyAndValidate(begin, offsets.size, num_rows, &int64_offsets_);
        }
    }

    int64_t
    Size() const {
        return element_type_ == OffsetElementType::Int32
                   ? static_cast<int64_t>(int32_offsets_.size())
                   : static_cast<int64_t>(int64_offsets_.size());
    }

    int64_t
    AtUnchecked(int64_t index) const {
        return element_type_ == OffsetElementType::Int32
                   ? int32_offsets_[index]
                   : int64_offsets_[index];
    }

 private:
    template <typename T>
    static void
    CopyAndValidate(const T* input,
                    int64_t size,
                    int64_t num_rows,
                    std::vector<T>* output) {
        output->reserve(size);
        for (int64_t i = 0; i < size; ++i) {
            const auto offset = input[i];
            AssertInfo(offset >= 0 && offset < num_rows,
                       "take offset {} is outside column rows {}",
                       offset,
                       num_rows);
            output->emplace_back(offset);
        }
    }

    OffsetElementType element_type_;
    std::vector<int32_t> int32_offsets_;
    std::vector<int64_t> int64_offsets_;
};

struct RawTakeLocation {
    int64_t source_cell_id = -1;
    size_t cell_offset = 0;
};

std::optional<ChunkedColumnInterface::TargetType>
ValidateTakeTargetType(DataType data_type,
                       ChunkedColumnInterface::TargetType requested) {
    AssertInfo(requested != ChunkedColumnInterface::TargetType::None,
               "take target type must be specified");
    AssertInfo(CanReadAsTargetType(data_type, requested),
               "take target {} does not match column type {}",
               static_cast<int>(requested),
               data_type);
    if (requested == ChunkedColumnInterface::TargetType::VectorArrayView) {
        return std::nullopt;
    }
    return requested;
}

class RawTakeResult final : public ChunkedColumnInterface::TakeResult {
 public:
    RawTakeResult(RawTakeCellPin pin_cell,
                  std::shared_ptr<const ColumnPlanner> planner,
                  OffsetView offsets,
                  ColumnFilterPtr filter,
                  DataType data_type,
                  ChunkedColumnInterface::TargetType target_type,
                  bool nullable)
        : pin_cell_(std::move(pin_cell)),
          planner_(std::move(planner)),
          data_type_(data_type),
          target_type_(target_type),
          nullable_(nullable),
          offsets_(offsets, planner_->NumRows()),
          filter_(std::move(filter)) {
        AssertInfo(static_cast<bool>(pin_cell_),
                   "raw take cell accessor is null");
        AssertInfo(planner_ != nullptr, "raw take planner is null");
    }

    int64_t
    Size() const override {
        return offsets_.Size();
    }

    ChunkedColumnInterface::TargetType
    GetTargetType() const override {
        return target_type_;
    }

    DataType
    GetDataType() const override {
        return data_type_;
    }

 protected:
    ChunkedColumnInterface::TakeItemState
    PrepareItem(int64_t index, bool read_data) const override {
        if (owned_ != nullptr) {
            return {!owned_data_.validity || owned_data_.validity[index],
                    read_data && owned_data_.data_skipped &&
                        owned_data_.data_skipped[index]};
        }
        if (!nullable_ && !read_data) {
            return {true, false};
        }
        const auto& location = ResolveLocation(index);
        const auto preloaded_skip =
            read_data && filter_ != nullptr &&
            filter_->Source() ==
                ColumnFilter::MetricsSource::PreloadedStatistics &&
            ShouldSkipFilteredCell(location.source_cell_id);
        if (!nullable_ && preloaded_skip) {
            return {true, true};
        }
        auto* chunk = PinCell(location.source_cell_id);
        const auto valid = !nullable_ || chunk->isValid(location.cell_offset);
        if (!read_data || filter_ == nullptr) {
            return {valid, false};
        }
        if (preloaded_skip) {
            return {valid, true};
        }
        if (filter_->Source() != ColumnFilter::MetricsSource::LoadedPayload) {
            return {valid, false};
        }
        return {valid, ShouldSkipFilteredCell(location.source_cell_id)};
    }

 public:
    bool
    IsOwned() const override {
        return owned_ != nullptr;
    }

    ChunkedColumnInterface::OwnedTakeData
    GetOwn() const override {
        if (owned_ != nullptr) {
            return owned_data_;
        }

        auto owner = std::make_shared<OwnedTakeStorage>();
        if (nullable_) {
            owner->validity.resize(Size());
        }
        ChunkedColumnInterface::ValueView values;
        values.offset = 0;

        auto visit_values = [this, &owner](auto&& fn) {
            VisitGrouped([&](int64_t index) {
                const auto state = PrepareItem(index, /*read_data=*/true);
                if (nullable_) {
                    owner->validity[index] = state.is_valid;
                }
                if (state.data_skipped) {
                    // Filtering is decided and cached at Cell granularity.
                    // Expand it to the logical offset order only when an
                    // owned result actually contains a skipped position.
                    if (owner->data_skipped.empty()) {
                        owner->data_skipped.resize(Size());
                    }
                    owner->data_skipped[index] = true;
                    return;
                }
                const auto [chunk, chunk_offset] = ResolveBorrowed(index);
                fn(index, chunk, chunk_offset, state.is_valid);
            });
        };

        auto fill_primitive = [this, &visit_values]<typename T>(
                                  std::vector<T>& output) {
            output.resize(Size());
            visit_values(
                [&](int64_t index, Chunk* chunk, size_t chunk_offset, bool) {
                    output[index] = *static_cast<const T*>(
                        static_cast<const void*>(chunk->ValueAt(chunk_offset)));
                });
        };

        switch (data_type_) {
            case DataType::INT8:
                fill_primitive(owner->int8_values);
                values.data = owner->int8_values.data();
                values.byte_width = sizeof(int8_t);
                break;
            case DataType::INT16:
                fill_primitive(owner->int16_values);
                values.data = owner->int16_values.data();
                values.byte_width = sizeof(int16_t);
                break;
            case DataType::INT32:
                fill_primitive(owner->int32_values);
                values.data = owner->int32_values.data();
                values.byte_width = sizeof(int32_t);
                break;
            case DataType::INT64:
            case DataType::TIMESTAMPTZ:
                fill_primitive(owner->int64_values);
                values.data = owner->int64_values.data();
                values.byte_width = sizeof(int64_t);
                break;
            case DataType::FLOAT:
                fill_primitive(owner->float_values);
                values.data = owner->float_values.data();
                values.byte_width = sizeof(float);
                break;
            case DataType::DOUBLE:
                fill_primitive(owner->double_values);
                values.data = owner->double_values.data();
                values.byte_width = sizeof(double);
                break;
            case DataType::BOOL:
                owner->bool_values.resize(Size());
                visit_values([&](int64_t index,
                                 Chunk* chunk,
                                 size_t chunk_offset,
                                 bool) {
                    owner->bool_values[index] = *static_cast<const bool*>(
                        static_cast<const void*>(chunk->ValueAt(chunk_offset)));
                });
                values.data = owner->bool_values.data();
                values.byte_width = sizeof(bool);
                break;
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
            case DataType::GEOMETRY:
                owner->strings.resize(Size());
                visit_values([&](int64_t index,
                                 Chunk* chunk,
                                 size_t chunk_offset,
                                 bool valid) {
                    if (valid) {
                        owner->strings[index] =
                            static_cast<StringChunk*>(chunk)->operator[](
                                chunk_offset);
                    }
                });
                owner->string_views.resize(Size());
                for (int64_t i = 0; i < Size(); ++i) {
                    owner->string_views[i] = owner->strings[i];
                }
                values.data = owner->string_views.data();
                values.byte_width = sizeof(std::string_view);
                break;
            case DataType::JSON:
                owner->json_values.resize(Size());
                visit_values([&](int64_t index,
                                 Chunk* chunk,
                                 size_t chunk_offset,
                                 bool valid) {
                    if (!valid) {
                        return;
                    }
                    const auto view =
                        static_cast<StringChunk*>(chunk)->operator[](
                            chunk_offset);
                    owner->json_values[index] =
                        Json(simdjson::padded_string(view.data(), view.size()));
                });
                values.data = owner->json_values.data();
                values.byte_width = sizeof(Json);
                break;
            case DataType::ARRAY:
                if (target_type_ == TargetType::ArrayValueView) {
                    owner->array_values.resize(Size());
                    owner->array_value_views.resize(Size());
                    visit_values([&](int64_t index,
                                     Chunk* chunk,
                                     size_t chunk_offset,
                                     bool valid) {
                        if (!valid) {
                            return;
                        }
                        auto* array_chunk =
                            dynamic_cast<ColumnarArrayChunk*>(chunk);
                        AssertInfo(array_chunk != nullptr,
                                   "raw take Cell is not a recursive array");
                        auto view =
                            array_chunk->View<ArrayValueView>(chunk_offset);
                        owner->array_values[index] =
                            ArrayValue(view.output_data(), view.type());
                        owner->array_value_views[index] =
                            owner->array_values[index].View();
                    });
                    values.data = owner->array_value_views.data();
                    values.byte_width = sizeof(ArrayValueView);
                } else {
                    owner->arrays.resize(Size());
                    owner->array_views.resize(Size());
                    visit_values([&](int64_t index,
                                     Chunk* chunk,
                                     size_t chunk_offset,
                                     bool valid) {
                        if (!valid) {
                            return;
                        }
                        auto* array_chunk = dynamic_cast<ArrayChunk*>(chunk);
                        AssertInfo(array_chunk != nullptr,
                                   "raw take Cell is not a flat array");
                        array_chunk->View(chunk_offset)
                            .output_data(owner->arrays[index]);
                        auto& array = owner->arrays[index];
                        owner->array_views[index] =
                            ArrayView(const_cast<char*>(array.data()),
                                      array.length(),
                                      array.byte_size(),
                                      array.get_element_type(),
                                      array.get_offsets_data());
                    });
                    values.data = owner->array_views.data();
                    values.byte_width = sizeof(ArrayView);
                }
                break;
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "unsupported raw owned take type {}",
                          data_type_);
        }

        values.target_type = target_type_;

        owned_ = owner;
        owned_data_ = ChunkedColumnInterface::OwnedTakeData{
            values,
            owner->validity.empty()
                ? ValidityView{}
                : ValidityView::FromExpanded(owner->validity.data()),
            owner,
            Size(),
            owner->data_skipped.empty()
                ? ValidityView{}
                : ValidityView::FromExpanded(owner->data_skipped.data())};
        ResetBorrowedPin();
        return owned_data_;
    }

 protected:
    const void*
    FixedValueAt(int64_t index) const override {
        if (owned_ != nullptr) {
            return static_cast<const char*>(owned_data_.values.data) +
                   (owned_data_.values.offset + index) *
                       owned_data_.values.byte_width;
        }
        const auto [chunk, offset] = ResolveBorrowed(index);
        return chunk->ValueAt(offset);
    }

    std::string_view
    StringViewAt(int64_t index) const override {
        if (owned_ != nullptr) {
            return owned_data_.values.data_as<std::string_view>()[index];
        }
        const auto [chunk, offset] = ResolveBorrowed(index);
        return static_cast<StringChunk*>(chunk)->operator[](offset);
    }

    Json
    JsonAt(int64_t index) const override {
        if (owned_ != nullptr) {
            return owned_data_.values.data_as<Json>()[index];
        }
        return Json(StringViewAt(index));
    }

    ArrayView
    ArrayAt(int64_t index) const override {
        if (owned_ != nullptr) {
            return owned_data_.values.data_as<ArrayView>()[index];
        }
        const auto [chunk, offset] = ResolveBorrowed(index);
        auto* array_chunk = dynamic_cast<ArrayChunk*>(chunk);
        AssertInfo(array_chunk != nullptr, "raw take Cell is not a flat array");
        return array_chunk->View(offset);
    }

    ArrayValueView
    ArrayValueAt(int64_t index) const override {
        if (owned_ != nullptr) {
            return owned_data_.values.data_as<ArrayValueView>()[index];
        }
        const auto [chunk, offset] = ResolveBorrowed(index);
        auto* array_chunk = dynamic_cast<ColumnarArrayChunk*>(chunk);
        AssertInfo(array_chunk != nullptr,
                   "raw take Cell is not a recursive array");
        return array_chunk->View<ArrayValueView>(offset);
    }

 private:
    struct CellGroup {
        int64_t cell_id;
        std::vector<int64_t> positions;
    };

    Chunk*
    PinCell(int64_t cell_id) const {
        if (current_pin_.has_value() && current_cell_id_ == cell_id) {
            return current_chunk_;
        }
        ResetBorrowedPin();
        auto next = pin_cell_(cell_id);
        auto* chunk = next.get();
        AssertInfo(chunk != nullptr, "raw take pinned null cell {}", cell_id);
        current_pin_.emplace(std::move(next));
        current_cell_id_ = cell_id;
        current_chunk_ = chunk;
        return chunk;
    }

    std::pair<Chunk*, size_t>
    ResolveBorrowed(int64_t index) const {
        const auto& location = ResolveLocation(index);
        return {PinCell(location.source_cell_id), location.cell_offset};
    }

    const RawTakeLocation&
    ResolveLocation(int64_t index) const {
        if (cached_location_index_ == index) {
            return cached_location_;
        }
        const auto location = planner_->Locate(offsets_.AtUnchecked(index));
        cached_location_ = RawTakeLocation{
            location.cell_id, static_cast<size_t>(location.cell_offset)};
        cached_location_index_ = index;
        return cached_location_;
    }

    bool
    ShouldSkipFilteredCell(int64_t cell_id) const {
        AssertInfo(filter_ != nullptr, "raw take filter is null");
        if (filter_->Source() == ColumnFilter::MetricsSource::LoadedPayload) {
            AssertInfo(current_cell_id_ == cell_id && current_chunk_ != nullptr,
                       "loaded-payload filter Cell {} is not pinned",
                       cell_id);
        }
        if (current_filter_cell_id_ == cell_id &&
            current_filter_skip_.has_value()) {
            return *current_filter_skip_;
        }
        auto [it, inserted] = filter_skip_by_cell_.try_emplace(cell_id, false);
        if (inserted) {
            it->second = filter_->CanSkipPhysicalCell(cell_id);
        }
        current_filter_cell_id_ = cell_id;
        current_filter_skip_ = it->second;
        return *current_filter_skip_;
    }

    template <typename Fn>
    void
    VisitGrouped(Fn&& fn) const {
        std::vector<CellGroup> cell_groups;
        std::unordered_map<int64_t, size_t> cell_to_group;
        const auto max_groups =
            static_cast<size_t>(std::min(Size(), planner_->NumCells()));
        cell_groups.reserve(max_groups);
        cell_to_group.reserve(max_groups);
        for (int64_t position = 0; position < Size(); ++position) {
            const auto cell_id = ResolveLocation(position).source_cell_id;
            auto [it, inserted] =
                cell_to_group.emplace(cell_id, cell_groups.size());
            if (inserted) {
                cell_groups.emplace_back(CellGroup{cell_id, {}});
            }
            cell_groups[it->second].positions.emplace_back(position);
        }

        for (const auto& group : cell_groups) {
            for (const auto position : group.positions) {
                fn(position);
            }
        }
    }

    void
    ResetBorrowedPin() const {
        current_pin_.reset();
        current_cell_id_ = -1;
        current_chunk_ = nullptr;
    }

    RawTakeCellPin pin_cell_;
    std::shared_ptr<const ColumnPlanner> planner_;
    DataType data_type_;
    ChunkedColumnInterface::TargetType target_type_;
    bool nullable_;
    OwnedTakeOffsets offsets_;
    ColumnFilterPtr filter_;
    mutable std::unordered_map<int64_t, bool> filter_skip_by_cell_;
    mutable int64_t current_filter_cell_id_{-1};
    mutable std::optional<bool> current_filter_skip_;
    mutable int64_t cached_location_index_{-1};
    mutable RawTakeLocation cached_location_;
    mutable std::optional<PinWrapper<Chunk*>> current_pin_;
    mutable int64_t current_cell_id_{-1};
    mutable Chunk* current_chunk_{nullptr};
    mutable std::shared_ptr<OwnedTakeStorage> owned_;
    mutable ChunkedColumnInterface::OwnedTakeData owned_data_;
};

}  // namespace detail

ChunkedColumnInterface::TakeResultPtr
ChunkedColumnInterface::Take(milvus::OpContext* op_ctx,
                             TakeOptions options) const {
    AssertInfo(options.offsets.size >= 0,
               "take offset count must be non-negative, got {}",
               options.offsets.size);
    AssertInfo(options.offsets.size == 0 || options.offsets.data != nullptr,
               "take offsets are null with count {}",
               options.offsets.size);
    auto data_type = GetDefaultScanDataType();
    if (!data_type.has_value()) {
        return nullptr;
    }
    auto target_type =
        detail::ValidateTakeTargetType(*data_type, options.target_type);
    if (!target_type.has_value()) {
        return nullptr;
    }
    auto pin_cell = MakeTakeCellPin(op_ctx);
    if (!pin_cell) {
        return nullptr;
    }
    auto planner = PlannerHandle();
    return std::make_unique<detail::RawTakeResult>(std::move(pin_cell),
                                                   std::move(planner),
                                                   options.offsets,
                                                   std::move(options.filter),
                                                   *data_type,
                                                   *target_type,
                                                   IsNullable());
}

}  // namespace milvus
