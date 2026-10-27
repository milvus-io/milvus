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
#include <array>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <folly/Portability.h>

#include "mmap/ChunkedColumnFilter.h"
#include "mmap/ChunkedColumnInterface.h"
#include "mmap/detail/PinnedStringViews.h"
#include "mmap/detail/PreparedCellSlots.h"

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

class RawTakeBinding;

class RawTakeResult final : public ChunkedColumnInterface::TakeResult {
 public:
    RawTakeResult(RawTakeCellPin pin_cell,
                  std::shared_ptr<const ColumnPlanner> planner,
                  OffsetView offsets,
                  ColumnFilterPtr filter,
                  DataType data_type,
                  ChunkedColumnInterface::TargetType target_type,
                  bool nullable,
                  TakeCellSource source,
                  std::shared_ptr<TakeContext> context)
        : pin_cell_(std::move(pin_cell)),
          planner_(std::move(planner)),
          data_type_(data_type),
          target_type_(target_type),
          nullable_(nullable),
          offsets_(offsets),
          filter_(std::move(filter)),
          source_(std::move(source)),
          context_(std::move(context)) {
        AssertInfo(
            static_cast<bool>(pin_cell_) || (context_ && source_.identity &&
                                             source_.pin && source_.project),
            "raw take cell accessor is null");
        AssertInfo(planner_ != nullptr, "raw take planner is null");
    }

    int64_t
    Size() const override {
        return offsets_.size;
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
    int64_t
    PrepareBorrowedBatch(int64_t first, int64_t maximum) const override {
        // Do not clean up an already-active OUTER borrow on reentrant error.
        AssertInfo(!in_borrowed_batch_, "nested take view consumer");
        AssertInfo(first >= 0 && maximum > 0 && first <= Size() &&
                       maximum <= Size() - first,
                   "invalid take view prefix");
        in_borrowed_batch_ = true;
        try {
            if (owned_ != nullptr || !context_ || !source_.identity) {
                return maximum;
            }
            AssertInfo(source_.identity && source_.pin && source_.project,
                       "incomplete take cell source");
            // estimated_bytes is a backend callback and may reenter this
            // context. A new physical publication invalidates the local
            // missing proof; retain the public checked fallback in that case.
            const auto publications_before = context_->Stats().bulk_pins;
            // Bounded scratch only: arbitrary-length Take/EvalOffsets inputs
            // continue through as many prefixes as necessary. This is not a
            // 64-lane bitmap/callback restriction or a source-ID slot table.
            const auto limit = std::min<int64_t>(maximum, kPreparedRows);
            // Fixed bounded scratch is initialized once per result/binding,
            // not value-initialized again for every input. Active lanes and
            // unique-cell records are overwritten before their count publishes.
            // Ordinary noncontextual Take never allocates this workspace.
            if (prepared_workspace_ == nullptr) {
                prepared_workspace_ = std::make_unique<PreparedWorkspace>();
            }
            auto& workspace = *prepared_workspace_;
            // CellLocation has default member initializers. Keep this bounded
            // scratch too, rather than initializing all64 slots per prefix
            // immediately before LocateBatch overwrites every active lane.
            auto& locations = workspace.locations;
            planner_->LocateBatch(offsets_, first, limit, locations.data());
            prepared_first_ = first;
            auto& cells = workspace.cells;
            std::array<int64_t, kPreparedRows> cell_ids;
            std::array<size_t, kPreparedRows> lane_slots;
            std::array<size_t, kPreparedRows> missing_slots;
            auto& missing = workspace.missing;
            missing.clear();
            missing.reserve(limit);
            PreparedCellSlots unique_slots;
            int64_t lane_count = 0;
            size_t estimated_missing = 0;
            bool temporary_cell = false;
            for (int64_t lane = 0; lane < limit; ++lane) {
                const auto location = locations[lane];
                const auto id = location.cell_id;
                // Match PrepareItem's existing preloaded-statistics fast path:
                // do not pin payload merely to rediscover a skipped cell.
                const bool skipped_without_pin =
                    filter_ != nullptr &&
                    filter_->Source() ==
                        ColumnFilter::MetricsSource::PreloadedStatistics &&
                    (!nullable_ || !filter_->SkippedValidityRequired()) &&
                    ShouldSkipFilteredCell(id);
                if (skipped_without_pin) {
                    lane_slots[lane] = kNoPreparedCell;
                    workspace.row_ids[lane] = id;
                    workspace.row_offsets[lane] = location.cell_offset;
                    ++lane_count;
                    continue;
                }
                // At most64 cells in a prepared prefix; arbitrary chunk IDs
                // remain distinct. Dedup affects source work, never output lanes.
                const auto found = unique_slots.Find(id);
                size_t slot;
                if (found.cell_index != PreparedCellSlots::kEmpty) {
                    slot = found.cell_index;
                } else {
                    // Capture each distinct backing owner once BEFORE a new
                    // cohort can evict FIFO entries. Cells borrow that batch
                    // owner set, not one shared_ptr copy per physical cell.
                    auto hit =
                        context_->FindBorrowed(source_, id, prepared_owners_);
                    if (!hit.has_value()) {
                        const auto bytes = source_.estimated_bytes
                                               ? source_.estimated_bytes(id)
                                               : 0;
                        const bool single_temporary =
                            bytes == 0 || bytes > context_->Budget();
                        if (lane != 0 &&
                            (temporary_cell || single_temporary ||
                             bytes > context_->Budget() - estimated_missing)) {
                            break;
                        }
                        // Only MISSING cells consume the preparation budget.
                        // Unknown/oversized first cells are safe one-cell
                        // temporary cohorts, never an unbounded lookahead.
                        temporary_cell |= single_temporary;
                        if (!single_temporary) {
                            estimated_missing += bytes;
                        }
                        missing_slots[missing.size()] = prepared_cell_count_;
                        missing.push_back(id);
                    }
                    slot = prepared_cell_count_++;
                    cell_ids[slot] = id;
                    cells[slot] = {hit.value_or(nullptr)};
                    unique_slots.Insert(found, id, slot);
                }
                lane_slots[lane] = slot;
                workspace.row_ids[lane] = id;
                workspace.row_offsets[lane] = location.cell_offset;
                ++lane_count;
            }
            const auto count = lane_count;
            AssertInfo(count > 0, "take preparation made no progress");
            if (!missing.empty()) {
                // Find above captured hit leases, so immediate publication is
                // safe even if Acquire evicts those owners from retention.
                auto acquired =
                    context_->Stats().bulk_pins == publications_before
                        ? context_->PinAndPublish(source_, missing)
                        : context_->AcquireUniqueMissing(source_, missing);
                prepared_owners_.Add(acquired);
                for (size_t i = 0; i < missing.size(); ++i) {
                    cells[missing_slots[i]].backing = acquired->cells[i].second;
                }
            }
            for (size_t i = 0; i < prepared_cell_count_; ++i) {
                auto& cell = cells[i];
                // Project each physical cell once; duplicate output lanes
                // reuse the field pointer and the same complete backing owner.
                cell.chunk = ProjectPreparedCell(cell_ids[i], cell.backing);
            }
            for (int64_t lane = 0; lane < count; ++lane) {
                const auto slot = lane_slots[lane];
                auto* chunk =
                    slot == kNoPreparedCell ? nullptr : cells[slot].chunk;
                workspace.row_chunks[lane] = chunk;
                if (target_type_ == TargetType::StringView) {
                    workspace.string_chunks[lane] =
                        static_cast<const StringChunk*>(chunk);
                }
            }
            prepared_count_ = count;
            return count;
        } catch (...) {
            // Prepare is called before the caller's guard is constructed.
            // Restore this borrow only; preserve the original error.
            EndBorrowedBatch();
            throw;
        }
    }

    void
    EndBorrowedBatch() const override {
        prepared_count_ = 0;
        prepared_first_ = -1;
        // Raw metadata does not own a payload and is unreadable when count=0.
        // Keep bounded scratch, but always release the actual borrowed owners.
        prepared_cell_count_ = 0;
        prepared_owners_.Clear();
        borrowed_pins_.clear();
        borrowed_cohorts_.clear();
        current_pin_.reset();
        current_cohort_.reset();
        current_cell_id_ = -1;
        current_chunk_ = nullptr;
        cached_location_index_ = -1;
        in_borrowed_batch_ = false;
    }

    bool
    FillBorrowedBatch(int64_t first,
                      int64_t count,
                      TargetType target_type,
                      void* values,
                      bool* validity,
                      bool* skipped) const override {
        if (owned_ != nullptr || prepared_count_ != count ||
            prepared_first_ != first) {
            return false;
        }
        AssertInfo(count > 0 && target_type == target_type_ && values &&
                       validity && skipped,
                   "invalid prepared take buffers");
        // Dispatch storage representation once, never an Expr operation.
        // Other representations retain the original item access fallback.
        switch (target_type) {
            case TargetType::Bool:
                return FillPrepared<bool>(
                    first, count, values, validity, skipped);
            case TargetType::Int8:
                return FillPrepared<int8_t>(
                    first, count, values, validity, skipped);
            case TargetType::Int16:
                return FillPrepared<int16_t>(
                    first, count, values, validity, skipped);
            case TargetType::Int32:
                return FillPrepared<int32_t>(
                    first, count, values, validity, skipped);
            case TargetType::Int64:
                return FillPrepared<int64_t>(
                    first, count, values, validity, skipped);
            case TargetType::Float:
                return FillPrepared<float>(
                    first, count, values, validity, skipped);
            case TargetType::Double:
                return FillPrepared<double>(
                    first, count, values, validity, skipped);
            case TargetType::StringView:
                return FillPrepared<std::string_view>(
                    first, count, values, validity, skipped);
            default:
                return false;
        }
    }

    ChunkedColumnInterface::TakeItemState
    PrepareItem(int64_t index, bool read_data) const override {
        if (owned_ != nullptr) {
            return {!owned_data_.validity || owned_data_.validity[index],
                    read_data && owned_data_.data_skipped &&
                        owned_data_.data_skipped[index]};
        }
        if (!nullable_ && (!read_data || filter_ == nullptr)) {
            // A following value getter resolves/checks this offset itself.
            // Do not add a second location call to ordinary non-null reads.
            // Validity-only access has no getter, so retain its bounds check.
            if (!read_data) {
                ResolveLocation(index);
            }
            return {true, false};
        }
        const auto& location = ResolveLocation(index);
        const auto preloaded_skip =
            read_data && filter_ != nullptr &&
            filter_->Source() ==
                ColumnFilter::MetricsSource::PreloadedStatistics &&
            ShouldSkipFilteredCell(location.source_cell_id);
        if (preloaded_skip &&
            (!nullable_ || !filter_->SkippedValidityRequired())) {
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
    static constexpr size_t kPreparedRows = 64;
    static_assert(kPreparedRows == PreparedCellSlots::kMaximumCells);
    static constexpr size_t kNoPreparedCell = static_cast<size_t>(-1);

    struct PreparedCell {
        void* backing{nullptr};
        Chunk* chunk{nullptr};
    };

    struct CachedProjection {
        uint64_t publication{0};
        int64_t id{-1};
        void* backing{nullptr};
        Chunk* chunk{nullptr};
    };

    struct PreparedWorkspace {
        std::array<CellLocation, kPreparedRows> locations;
        // Prepare already resolves each lane. Keep the gather inputs in their
        // final contiguous layout instead of repacking the same metadata into
        // two stack arrays in every FillPrepared call. Unique physical cells
        // remain separate from logical lanes (including duplicates/skips).
        std::array<PreparedCell, kPreparedRows> cells;
        std::array<int64_t, kPreparedRows> row_ids;
        std::array<size_t, kPreparedRows> row_offsets;
        std::array<Chunk*, kPreparedRows> row_chunks;
        std::array<const StringChunk*, kPreparedRows> string_chunks;
        std::array<CachedProjection, kPreparedRows> projections;
        // Only bounded IDs, never owners or data; capacity is reused across
        // inputs so a cache-hit prefix does not allocate a missing-ID vector.
        std::vector<int64_t> missing;
    };

    Chunk*
    ProjectPreparedCell(int64_t id, void* backing) const {
        // The caller has ALREADY captured this exact backing's current owner.
        // A cache collision only misses: compare the complete ID and pointer.
        // Publication prevents physical-pointer ABA after eviction/reload.
        // Clear without publication cannot hit FindBorrowed, so its next pin
        // also invalidates this entry. No cache entry retains a payload owner.
        const auto publication = context_->Stats().bulk_pins;
        auto& projections = prepared_workspace_->projections;
        auto& cached =
            projections[static_cast<uint64_t>(id) % projections.size()];
        if (cached.id == id && cached.publication == publication &&
            cached.backing == backing) {
            return cached.chunk;
        }
        auto* chunk = source_.project(backing);
        AssertInfo(chunk != nullptr,
                   "take source projected a null prepared cell");
        // Use the epoch from BEFORE project: a reentrant publication during
        // that callback must not promote an old backing into the newer epoch.
        cached = {publication, id, backing, chunk};
        return chunk;
    }

    template <typename T>
    bool
    FillPrepared(int64_t first,
                 int64_t count,
                 void* output,
                 bool* validity,
                 bool* skipped) const {
        auto* values = static_cast<T*>(output);
        const auto& workspace = *prepared_workspace_;
        if (!nullable_ && filter_ == nullptr) {
            // These exact lanes are already resolved and pinned. There is no
            // per-row virtual PrepareItem/GetValue, Resolve or PinCell here.
            if constexpr (std::is_same_v<T, std::string_view>) {
                // Keep the canonical getter's storage loop in a small TU. The
                // bounded prefix already holds its pointer/row metadata;
                // the logical Take input remains unbounded and no value or
                // backing owner is copied. Nullable/SkipIndex stays below.
                AssertInfo(count <= kPreparedRows,
                           "prepared string prefix exceeds its capacity");
                storage::GatherPinnedStringViews(workspace.string_chunks.data(),
                                                 workspace.row_offsets.data(),
                                                 count,
                                                 values,
                                                 validity,
                                                 skipped);
                return true;
            }
            for (int64_t lane = 0; lane < count; ++lane) {
                const auto offset = workspace.row_offsets[lane];
                auto* chunk = workspace.row_chunks[lane];
                if constexpr (!std::is_same_v<T, std::string_view>) {
                    values[lane] = *static_cast<const T*>(
                        static_cast<const void*>(chunk->ValueAt(offset)));
                }
                validity[lane] = true;
                skipped[lane] = false;
            }
        } else {
            // Original item access remains the authority for nullable,
            // preloaded/loaded SkipIndex, skipped-value and error semantics.
            for (int64_t lane = 0; lane < count; ++lane) {
                const auto item = GetUnchecked<T>(first + lane);
                validity[lane] = item.is_valid;
                skipped[lane] = item.data_skipped;
                values[lane] = item.value.has_value() ? *item.value : T{};
                if constexpr (std::is_same_v<T, std::string_view>) {
                    if (!values[lane].empty()) {
                        __builtin_prefetch(values[lane].data(), 0, 1);
                    }
                }
            }
        }
        return true;
    }

    struct CellGroup {
        int64_t cell_id;
        std::vector<int64_t> positions;
    };

    Chunk*
    PinCell(int64_t cell_id) const {
        if (prepared_count_ > 0 && cached_location_index_ >= prepared_first_ &&
            cached_location_index_ - prepared_first_ < prepared_count_) {
            const auto lane = cached_location_index_ - prepared_first_;
            AssertInfo(prepared_workspace_->row_ids[lane] == cell_id &&
                           prepared_workspace_->row_chunks[lane] != nullptr,
                       "take lane has no prepared payload");
            // Required by the existing LoadedPayload filter's current-cell
            // contract; ownership is held by prepared_owners_, not copied here.
            current_cell_id_ = cell_id;
            current_chunk_ = prepared_workspace_->row_chunks[lane];
            return current_chunk_;
        }
        if (current_chunk_ != nullptr && current_cell_id_ == cell_id) {
            return current_chunk_;
        }
        if (current_cohort_ != nullptr) {
            // Oversized cohorts are not retained by the context but their
            // already-pinned cells must still be reused inside this result.
            for (const auto& [id, cell] : current_cohort_->cells) {
                if (id == cell_id) {
                    current_cell_id_ = id;
                    current_chunk_ = source_.project(cell);
                    return current_chunk_;
                }
            }
        }
        ResetBorrowedPin();
        if (context_ && source_.identity) {
            auto selected = context_->Find(source_, cell_id);
            if (!selected.has_value()) {
                // Bound each physical bulk-pin cohort independently of the
                // caller's logical batch size. Keep the current cell first;
                // optional lookahead combines missing cells without an O(N)
                // plan for a large offset request.
                std::vector<int64_t> ids{cell_id};
                size_t estimated = source_.estimated_bytes
                                       ? source_.estimated_bytes(cell_id)
                                       : 0;
                for (int64_t i = cached_location_index_ + 1;
                     i < Size() && i < cached_location_index_ + 32 &&
                     ids.size() < 32 && estimated != 0 &&
                     estimated < context_->Budget();
                     ++i) {
                    const auto id =
                        planner_->Locate(offsets_.AtUnchecked(i)).cell_id;
                    if (filter_ != nullptr &&
                        filter_->Source() ==
                            ColumnFilter::MetricsSource::PreloadedStatistics &&
                        (!nullable_ || !filter_->SkippedValidityRequired()) &&
                        ShouldSkipFilteredCell(id)) {
                        continue;
                    }
                    if (std::find(ids.begin(), ids.end(), id) == ids.end()) {
                        const auto bytes = source_.estimated_bytes(id);
                        if (bytes == 0 ||
                            bytes > context_->Budget() - estimated) {
                            continue;
                        }
                        ids.push_back(id);
                        estimated += bytes;
                    }
                }
                auto cells = context_->Acquire(source_, ids);
                selected = std::move(cells.front());
            }
            current_cohort_ = std::move(selected->cohort);
            current_chunk_ = source_.project(selected->cell);
            AssertInfo(current_chunk_ != nullptr,
                       "take source projected null cell {}",
                       cell_id);
            current_cell_id_ = cell_id;
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
        const auto& location = ResolveLocation(index);
        return {PinCell(location.source_cell_id), location.cell_offset};
    }

    // Keep this checked per-item adapter in its getter. Enlarging the batch
    // implementation must not add a PLT call to ordinary non-batch reads.
    FOLLY_ALWAYS_INLINE const RawTakeLocation&
    ResolveLocation(int64_t index) const {
        if (cached_location_index_ == index) {
            return cached_location_;
        }
        if (prepared_count_ > 0 && index >= prepared_first_ &&
            index - prepared_first_ < prepared_count_) {
            const auto lane = index - prepared_first_;
            cached_location_ = {prepared_workspace_->row_ids[lane],
                                prepared_workspace_->row_offsets[lane]};
        } else {
            const auto location = planner_->Locate(offsets_.AtUnchecked(index));
            cached_location_ = RawTakeLocation{
                location.cell_id, static_cast<size_t>(location.cell_offset)};
        }
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
        if (in_borrowed_batch_) {
            if (current_pin_.has_value()) {
                borrowed_pins_.push_back(std::move(*current_pin_));
            }
            if (current_cohort_ != nullptr) {
                borrowed_cohorts_.push_back(std::move(current_cohort_));
            }
        }
        current_pin_.reset();
        current_cohort_.reset();
        current_cell_id_ = -1;
        current_chunk_ = nullptr;
    }

    friend class RawTakeBinding;

    // Only an explicitly reusable binding may change input. Ordinary Take
    // results never expose these operations and keep their finite input.
    void
    BeginInput(OffsetView offsets) {
        offsets_ = offsets;
    }

    void
    EndInput() noexcept {
        offsets_ = {};
        EndBorrowedBatch();
        owned_data_ = {};
        owned_.reset();
        current_filter_cell_id_ = -1;
        current_filter_skip_.reset();
        // GetOwn and legacy borrowed access may visit an arbitrary number of
        // cells. Do not retain their request-sized capacity in a binding.
        decltype(filter_skip_by_cell_){}.swap(filter_skip_by_cell_);
        decltype(borrowed_pins_){}.swap(borrowed_pins_);
        decltype(borrowed_cohorts_){}.swap(borrowed_cohorts_);
        // Workspace and prepared owners are bounded by the existing64-row
        // prefix. Keep metadata capacity only, never an active payload lease.
    }

    RawTakeCellPin pin_cell_;
    std::shared_ptr<const ColumnPlanner> planner_;
    DataType data_type_;
    ChunkedColumnInterface::TargetType target_type_;
    bool nullable_;
    OffsetView offsets_;
    ColumnFilterPtr filter_;
    TakeCellSource source_;
    std::shared_ptr<TakeContext> context_;
    mutable std::unordered_map<int64_t, bool> filter_skip_by_cell_;
    mutable int64_t current_filter_cell_id_{-1};
    mutable std::optional<bool> current_filter_skip_;
    mutable int64_t cached_location_index_{-1};
    mutable RawTakeLocation cached_location_;
    mutable std::optional<PinWrapper<Chunk*>> current_pin_;
    mutable int64_t current_cell_id_{-1};
    mutable Chunk* current_chunk_{nullptr};
    mutable std::shared_ptr<const TakeCellCohort> current_cohort_;
    mutable bool in_borrowed_batch_{false};
    mutable int64_t prepared_first_{-1};
    mutable int64_t prepared_count_{0};
    // Column/source/field configuration is immutable for this result. Arbitrary
    // physical IDs remain supported; conflicting slots simply project again.
    // Allocate only for contextual borrowed batches, not ordinary Take.
    mutable std::unique_ptr<PreparedWorkspace> prepared_workspace_;
    mutable size_t prepared_cell_count_{0};
    mutable TakeBatchOwners prepared_owners_;
    mutable std::vector<PinWrapper<Chunk*>> borrowed_pins_;
    mutable std::vector<std::shared_ptr<const TakeCellCohort>>
        borrowed_cohorts_;
    mutable std::shared_ptr<OwnedTakeStorage> owned_;
    mutable ChunkedColumnInterface::OwnedTakeData owned_data_;
};

class RawTakeBinding final : public TakeBinding {
 public:
    RawTakeBinding(RawTakeCellPin pin_cell,
                   std::shared_ptr<const ColumnPlanner> planner,
                   ColumnFilterPtr filter,
                   DataType data_type,
                   TargetType target_type,
                   bool nullable,
                   TakeCellSource source,
                   std::shared_ptr<TakeContext> context)
        : result_(std::move(pin_cell),
                  std::move(planner),
                  OffsetView{},
                  std::move(filter),
                  data_type,
                  target_type,
                  nullable,
                  std::move(source),
                  std::move(context)) {
    }

 private:
    const TakeResult&
    BeginInput(OffsetView offsets) override {
        result_.BeginInput(offsets);
        return result_;
    }

    void
    EndInput() noexcept override {
        result_.EndInput();
    }

    RawTakeResult result_;
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
    auto source =
        options.context ? MakeTakeCellSource(op_ctx) : TakeCellSource{};
    // A complete contextual source is the actual pin path; do not allocate
    // an unused legacy closure as well. Ordinary Take is still legacy-only.
    auto pin_cell =
        source.identity ? detail::RawTakeCellPin{} : MakeTakeCellPin(op_ctx);
    if (!pin_cell && !source.identity) {
        return nullptr;
    }
    auto planner = PlannerHandle();
    return std::make_unique<detail::RawTakeResult>(std::move(pin_cell),
                                                   std::move(planner),
                                                   options.offsets,
                                                   std::move(options.filter),
                                                   *data_type,
                                                   *target_type,
                                                   IsNullable(),
                                                   std::move(source),
                                                   std::move(options.context));
}

std::unique_ptr<TakeBinding>
ChunkedColumnInterface::CreateTakeBinding(milvus::OpContext* op_ctx,
                                          TakeBindingOptions options) const {
    auto data_type = GetDefaultScanDataType();
    if (!data_type.has_value()) {
        return nullptr;
    }
    auto target_type =
        detail::ValidateTakeTargetType(*data_type, options.target_type);
    if (!target_type.has_value()) {
        return nullptr;
    }
    auto source =
        options.context ? MakeTakeCellSource(op_ctx) : TakeCellSource{};
    auto pin_cell =
        source.identity ? detail::RawTakeCellPin{} : MakeTakeCellPin(op_ctx);
    if (!pin_cell && !source.identity) {
        return nullptr;
    }
    return std::make_unique<detail::RawTakeBinding>(std::move(pin_cell),
                                                    PlannerHandle(),
                                                    std::move(options.filter),
                                                    *data_type,
                                                    *target_type,
                                                    IsNullable(),
                                                    std::move(source),
                                                    std::move(options.context));
}

}  // namespace milvus
