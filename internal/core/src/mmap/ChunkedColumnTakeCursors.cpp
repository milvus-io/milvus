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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumnInterface.h"

namespace milvus {
namespace detail {

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
    FixedVector<bool> validity;
};

std::optional<ChunkedColumnInterface::ScanValueKind>
ResolveTakeValueKind(DataType data_type,
                     ChunkedColumnInterface::ScanValueKind requested) {
    const auto physical = GetScanValueKindForDataType(data_type);
    if (!physical.has_value() ||
        *physical == ChunkedColumnInterface::ScanValueKind::VectorArrayView) {
        return std::nullopt;
    }

    const auto resolved =
        requested == ChunkedColumnInterface::ScanValueKind::Default ? *physical
                                                                    : requested;
    AssertInfo(resolved == *physical,
               "take value kind {} does not match column type {}, expected {}",
               static_cast<int>(resolved),
               data_type,
               static_cast<int>(*physical));
    return resolved;
}

class RawTakeResult final : public ChunkedColumnInterface::TakeResult {
 public:
    RawTakeResult(ChunkedColumnInterface::TakeCellPin pin_cell,
                  const ChunkedColumnInterface* column,
                  ChunkedColumnInterface::TakePlan plan,
                  DataType data_type,
                  ChunkedColumnInterface::ScanValueKind value_kind)
        : pin_cell_(std::move(pin_cell)),
          data_type_(data_type),
          value_kind_(value_kind),
          nullable_(column->IsNullable()),
          locations_(std::move(plan.locations)) {
        AssertInfo(static_cast<bool>(pin_cell_),
                   "raw take cell accessor is null");
    }

    int64_t
    Size() const override {
        return static_cast<int64_t>(locations_.size());
    }

    ChunkedColumnInterface::ScanValueKind
    Kind() const override {
        return value_kind_;
    }

    DataType
    GetDataType() const override {
        return data_type_;
    }

    bool
    IsValid(int64_t index) const override {
        CheckIndex(index);
        if (owned_ != nullptr) {
            return !owned_data_.validity ||
                   owned_data_.validity[index];
        }
        const auto [chunk, offset] = ResolveBorrowed(index);
        return !nullable_ || chunk->isValid(offset);
    }

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
            VisitGrouped([&](int64_t index, Chunk* chunk, size_t chunk_offset) {
                const auto valid = !nullable_ || chunk->isValid(chunk_offset);
                if (nullable_) {
                    owner->validity[index] = valid;
                }
                fn(index, chunk, chunk_offset, valid);
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
                owner->arrays.resize(Size());
                owner->array_views.resize(Size());
                visit_values([&](int64_t index,
                                 Chunk* chunk,
                                 size_t chunk_offset,
                                 bool valid) {
                    if (!valid) {
                        return;
                    }
                    static_cast<ArrayChunk*>(chunk)
                        ->View(chunk_offset)
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
                break;
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "unsupported raw owned take type {}",
                          data_type_);
        }

        values.kind = value_kind_;

        owned_ = owner;
        owned_data_ = ChunkedColumnInterface::OwnedTakeData{
            values,
            owner->validity.empty()
                ? ValidityView{}
                : ValidityView::FromExpanded(owner->validity.data()),
            owner,
            Size()};
        ResetBorrowedPin();
        return owned_data_;
    }

 protected:
    const void*
    FixedValueAt(int64_t index) const override {
        CheckIndex(index);
        if (owned_ != nullptr) {
            AssertInfo(owned_data_.values.data != nullptr &&
                           owned_data_.values.byte_width > 0,
                       "invalid owned fixed-width take result");
            return static_cast<const char*>(owned_data_.values.data) +
                   (owned_data_.values.offset + index) *
                       owned_data_.values.byte_width;
        }
        const auto [chunk, offset] = ResolveBorrowed(index);
        return chunk->ValueAt(offset);
    }

    std::string_view
    StringViewAt(int64_t index) const override {
        CheckIndex(index);
        if (owned_ != nullptr) {
            return owned_data_.values.data_as<std::string_view>()[index];
        }
        const auto [chunk, offset] = ResolveBorrowed(index);
        return static_cast<StringChunk*>(chunk)->operator[](offset);
    }

    Json
    JsonAt(int64_t index) const override {
        CheckIndex(index);
        if (owned_ != nullptr) {
            return owned_data_.values.data_as<Json>()[index];
        }
        return Json(StringViewAt(index));
    }

    ArrayView
    ArrayAt(int64_t index) const override {
        CheckIndex(index);
        if (owned_ != nullptr) {
            return owned_data_.values.data_as<ArrayView>()[index];
        }
        const auto [chunk, offset] = ResolveBorrowed(index);
        return static_cast<ArrayChunk*>(chunk)->View(offset);
    }

 private:
    struct CellGroup {
        int64_t cell_id;
        std::vector<int64_t> positions;
    };

    void
    CheckIndex(int64_t index) const {
        AssertInfo(index >= 0 && index < Size(),
                   "raw take index {} out of range {}",
                   index,
                   Size());
    }

    Chunk*
    PinCell(int64_t cell_id) const {
        if (current_pin_.has_value() && current_cell_id_ == cell_id) {
            return current_chunk_;
        }
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
        CheckIndex(index);
        const auto& location = locations_[index];
        return {PinCell(location.source_cell_id), location.cell_offset};
    }

    template <typename Fn>
    void
    VisitGrouped(Fn&& fn) const {
        std::vector<CellGroup> cell_groups;
        std::unordered_map<int64_t, size_t> cell_to_group;
        cell_groups.reserve(locations_.size());
        cell_to_group.reserve(locations_.size());
        for (int64_t position = 0; position < Size(); ++position) {
            const auto cell_id = locations_[position].source_cell_id;
            auto [it, inserted] =
                cell_to_group.emplace(cell_id, cell_groups.size());
            if (inserted) {
                cell_groups.emplace_back(CellGroup{cell_id, {}});
            }
            cell_groups[it->second].positions.emplace_back(position);
        }

        for (const auto& group : cell_groups) {
            auto* chunk = PinCell(group.cell_id);
            for (const auto position : group.positions) {
                const auto& location = locations_[position];
                fn(position, chunk, location.cell_offset);
            }
        }
    }

    void
    ResetBorrowedPin() const {
        current_pin_.reset();
        current_cell_id_ = -1;
        current_chunk_ = nullptr;
    }

    ChunkedColumnInterface::TakeCellPin pin_cell_;
    DataType data_type_;
    ChunkedColumnInterface::ScanValueKind value_kind_;
    bool nullable_;
    std::vector<ChunkedColumnInterface::TakeLocation> locations_;
    mutable std::optional<PinWrapper<Chunk*>> current_pin_;
    mutable int64_t current_cell_id_{-1};
    mutable Chunk* current_chunk_{nullptr};
    mutable std::shared_ptr<OwnedTakeStorage> owned_;
    mutable ChunkedColumnInterface::OwnedTakeData owned_data_;
};

}  // namespace detail

ChunkedColumnInterface::TakeResultPtr
ChunkedColumnInterface::Take(milvus::OpContext* op_ctx,
                             const TakeOptions& options) const {
    return Take(op_ctx,
                Planner().PlanTake(options.offsets),
                options.value_kind);
}

ChunkedColumnInterface::TakeResultPtr
ChunkedColumnInterface::Take(milvus::OpContext* op_ctx,
                             TakePlan plan,
                             ScanValueKind requested_kind) const {
    auto data_type = GetDefaultScanDataType();
    if (!data_type.has_value()) {
        return nullptr;
    }
    auto value_kind = detail::ResolveTakeValueKind(*data_type, requested_kind);
    if (!value_kind.has_value()) {
        return nullptr;
    }
    auto pin_cell = MakeTakeCellPin(op_ctx);
    if (!pin_cell) {
        return nullptr;
    }
    return std::make_unique<detail::RawTakeResult>(
        std::move(pin_cell), this, std::move(plan), *data_type, *value_kind);
}

}  // namespace milvus
