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

#include "segcore/UpdatesRecord.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <mutex>
#include <numeric>
#include <optional>
#include <shared_mutex>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include "common/EasyAssert.h"

namespace milvus::segcore::experimental {
namespace {
void
Require(bool valid, const char* message) {
    if (!valid)
        throw SegcoreError(UnexpectedError, message);
}
template <typename T>
void
ReserveAppend(std::vector<T>& values, size_t count) {
    Require(count <= values.max_size() - values.size(),
            "Update vector is too large");
    const auto needed = values.size() + count;
    if (needed > values.capacity()) {
        const auto doubled = values.capacity() > values.max_size() / 2
                                 ? needed
                                 : values.capacity() * 2;
        values.reserve(std::max(needed, doubled));
    }
}
template <typename T>
struct Delta {
    uint64_t timestamp;
    int64_t offset;
    T value;
};
template <typename T>
struct TypedLog {
    using ValueType = T;
    std::vector<Delta<T>> entries;
    std::vector<uint8_t> validity;  // Empty for non-nullable fields.
    UpdateValueView
    View(size_t position) const {
        if (!validity.empty() && !validity[position])
            return std::monostate{};
        if constexpr (std::is_same_v<T, std::string>)
            return std::string_view(entries[position].value);
        else
            return entries[position].value;
    }
};
UpdateValue
Own(const UpdateValueView& value) {
    return std::visit(
        [](const auto& v) -> UpdateValue {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>,
                                         std::string_view>)
                return std::string(v);
            else
                return v;
        },
        value);
}
struct Field {
    UpdateFieldType type;
    bool nullable;
    UpdatesRecord::BaseReader base;
    std::variant<TypedLog<int64_t>, TypedLog<std::string>> log;
    std::unordered_map<int64_t, std::vector<size_t>> offsets;
    TargetBitmap updated_offsets;
    size_t updated_cells = 0;
    size_t directory_refs = 0;
    void
    Validate(const UpdateValue& value) const {
        if (std::holds_alternative<std::monostate>(value))
            Require(nullable, "SET NULL on a non-nullable update field");
        else
            Require(type == UpdateFieldType::Int64
                        ? std::holds_alternative<int64_t>(value)
                        : std::holds_alternative<std::string>(value),
                    "Update field type mismatch");
    }
};
}  // namespace

class UpdatesRecord::Impl {
 public:
    explicit Impl(size_t count) : row_count(count) {
    }
    const size_t row_count;
    mutable std::shared_mutex mutex;
    std::unordered_map<int64_t, std::unique_ptr<Field>> fields;
    std::optional<uint64_t> last_ts;
    std::atomic<uint64_t> value_rows{0}, base_rows{0}, correction_rows{0},
        old_index_reads{0}, value_reads{0}, ts_entries_visited{0},
        ts_selected_rows{0}, ts_unique_reads{0}, ts_dedup_reads{0},
        ts_visited_bitmap_bytes{0}, lookup_latest{0}, lookup_history{0},
        overlay_batches{0}, overlay_history_rows{0}, overlay_rows{0};
    Field&
    GetField(int64_t id) const {
        auto it = fields.find(id);
        Require(it != fields.end(), "Update field is not registered");
        return *it->second;
    }
    void
    ValidateOffset(int64_t offset) const {
        Require(offset >= 0 && static_cast<size_t>(offset) < row_count,
                "Update offset is outside the segment");
    }
    void
    ValidateRange(int64_t begin, size_t count) const {
        Require(begin >= 0 && static_cast<size_t>(begin) <= row_count &&
                    count <= row_count - begin,
                "Update range is outside the segment");
    }
    // Caller holds a read/write lock. The directory and vector are published together.
    template <typename Log>
    std::optional<size_t>
    Visible(const Field& field, const Log& log, int64_t offset, uint64_t ts) {
        auto found = field.offsets.find(offset);
        if (found == field.offsets.end() || found->second.empty())
            return std::nullopt;
        const auto& history = found->second;
        if (log.entries[history.back()].timestamp <= ts) {
            lookup_latest.fetch_add(1, std::memory_order_relaxed);
            return history.back();
        }
        lookup_history.fetch_add(1, std::memory_order_relaxed);
        auto end =
            std::upper_bound(history.begin(),
                             history.end(),
                             ts,
                             [&](uint64_t value, size_t position) {
                                 return value < log.entries[position].timestamp;
                             });
        return end == history.begin() ? std::nullopt
                                      : std::optional<size_t>(*--end);
    }
};

UpdatesRecord::UpdatesRecord(size_t count)
    : impl_(std::make_shared<Impl>(count)) {
    Require(count <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
            "Update row count exceeds offset range");
}
UpdatesRecord::UpdatesRecord(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {
}
UpdatesRecord::~UpdatesRecord() = default;
size_t
UpdatesRecord::RowCount() const {
    return impl_->row_count;
}
bool
UpdatesRecord::HasField(int64_t id) const {
    std::shared_lock lock(impl_->mutex);
    return impl_->fields.count(id) != 0;
}
void
UpdatesRecord::RegisterField(int64_t id,
                             UpdateFieldType type,
                             bool nullable,
                             BaseReader base) {
    std::unique_lock lock(impl_->mutex);
    Require(!impl_->fields.count(id) && !impl_->last_ts,
            "Register update fields before applying mutations");
    Require(static_cast<bool>(base), "Missing base lookup callback");
    Require(type == UpdateFieldType::Int64 || type == UpdateFieldType::VarChar,
            "Unsupported update field type");
    auto field = std::make_unique<Field>();
    field->type = type;
    field->nullable = nullable;
    field->base = std::move(base);
    field->updated_offsets = TargetBitmap(impl_->row_count, false);
    if (type == UpdateFieldType::VarChar)
        field->log.emplace<TypedLog<std::string>>();
    impl_->fields.emplace(id, std::move(field));
}

size_t
UpdatesRecord::Apply(const std::vector<UpdateValue>& keys,
                     uint64_t ts,
                     const std::vector<UpdateAssignment>& assignments,
                     const ResolveRows& resolve) {
    std::unique_lock lock(impl_->mutex);
    Require(ts < std::numeric_limits<uint64_t>::max(),
            "Reserved update timestamp");
    Require(!impl_->last_ts || ts >= *impl_->last_ts,
            "Updates must be ordered by timestamp");
    Require(!assignments.empty() && static_cast<bool>(resolve),
            "Missing update assignments or PK resolver");
    for (const auto& key : keys)
        Require(!std::holds_alternative<std::monostate>(key),
                "Primary key cannot be NULL");
    std::unordered_set<int64_t> fields;
    for (const auto& assignment : assignments) {
        Require(fields.insert(assignment.field_id).second,
                "Duplicate update assignment");
        impl_->GetField(assignment.field_id).Validate(assignment.value);
    }
    auto offsets = resolve(keys, ts);
    std::sort(offsets.begin(), offsets.end());
    offsets.erase(std::unique(offsets.begin(), offsets.end()), offsets.end());
    for (auto offset : offsets) impl_->ValidateOffset(offset);

    // Preallocate all vector/directory growth and string copies. A preparation
    // failure may leave empty directory entries, never published deltas or bits.
    std::vector<std::vector<std::string>> strings(assignments.size());
    for (size_t i = 0; i < assignments.size(); ++i) {
        const auto& assignment = assignments[i];
        auto& field = impl_->GetField(assignment.field_id);
        std::visit(
            [&](auto& log) {
                ReserveAppend(log.entries, offsets.size());
                if (field.nullable)
                    ReserveAppend(log.validity, offsets.size());
                for (auto offset : offsets) {
                    auto& history = field.offsets[offset];
                    Require(history.empty() ||
                                log.entries[history.back()].timestamp < ts,
                            "Each cell requires strictly increasing update "
                            "timestamps");
                    ReserveAppend(history, 1);
                }
            },
            field.log);
        if (const auto* value = std::get_if<std::string>(&assignment.value))
            strings[i].assign(offsets.size(), *value);
    }
    // Nothing below allocates: vector positions remain stable across relocation.
    for (size_t i = 0; i < assignments.size(); ++i) {
        const auto& assignment = assignments[i];
        auto& field = impl_->GetField(assignment.field_id);
        const bool valid =
            !std::holds_alternative<std::monostate>(assignment.value);
        std::visit(
            [&](auto& log) {
                using T = typename std::decay_t<decltype(log)>::ValueType;
                for (size_t j = 0; j < offsets.size(); ++j) {
                    const auto offset = offsets[j];
                    T value{};
                    if (valid) {
                        if constexpr (std::is_same_v<T, std::string>)
                            value = std::move(strings[i][j]);
                        else
                            value = std::get<int64_t>(assignment.value);
                    }
                    auto& history = field.offsets.at(offset);
                    history.push_back(log.entries.size());
                    ++field.directory_refs;
                    log.entries.push_back({ts, offset, std::move(value)});
                    if (field.nullable)
                        log.validity.push_back(valid);
                    if (!field.updated_offsets[offset])
                        ++field.updated_cells;
                    field.updated_offsets.set(offset);
                }
            },
            field.log);
    }
    impl_->last_ts = ts;
    return offsets.size();
}

void
UpdatesRecord::VisitUpdates(
    int64_t id,
    uint64_t ts,
    const std::function<void(int64_t, const UpdateValueView&)>& visit) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->GetField(id);
    std::visit(
        [&](const auto& log) {
            const auto& entries = log.entries;
            auto end = std::upper_bound(entries.begin(),
                                        entries.end(),
                                        ts,
                                        [](uint64_t value, const auto& entry) {
                                            return value < entry.timestamp;
                                        });
            uint64_t scanned = 0, selected = 0;
            if (entries.size() == field.updated_cells) {
                impl_->ts_unique_reads.fetch_add(1, std::memory_order_relaxed);
                while (end != entries.begin()) {
                    const auto& entry = *--end;
                    visit(entry.offset, log.View(&entry - entries.data()));
                    ++scanned;
                    ++selected;
                }
            } else {
                TargetBitmap visited(impl_->row_count, false);
                impl_->ts_dedup_reads.fetch_add(1, std::memory_order_relaxed);
                impl_->ts_visited_bitmap_bytes.fetch_add(
                    visited.size_in_bytes(), std::memory_order_relaxed);
                while (end != entries.begin()) {
                    const auto& entry = *--end;
                    ++scanned;
                    if (visited[entry.offset])
                        continue;
                    // NULL/non-match hides older values too.
                    visited.set(entry.offset);
                    visit(entry.offset, log.View(&entry - entries.data()));
                    if (++selected == field.updated_cells)
                        break;
                }
            }
            impl_->ts_entries_visited.fetch_add(scanned,
                                                std::memory_order_relaxed);
            impl_->ts_selected_rows.fetch_add(selected,
                                              std::memory_order_relaxed);
            impl_->correction_rows.fetch_add(selected,
                                             std::memory_order_relaxed);
        },
        field.log);
}

void
UpdatesRecord::CorrectEquality(int64_t id,
                               uint64_t ts,
                               const UpdateValue& wanted,
                               bool equal,
                               TargetBitmap& result,
                               TargetBitmap* validity) const {
    Require(result.size() == impl_->row_count &&
                (!validity || validity->size() == impl_->row_count),
            "Update correction bitmap size mismatch");
    UpdateValueView wanted_view = std::visit(
        [](const auto& v) -> UpdateValueView {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>,
                                         std::string>)
                return std::string_view(v);
            else
                return v;
        },
        wanted);
    VisitUpdates(id, ts, [&](int64_t offset, const UpdateValueView& value) {
        const bool valid = !std::holds_alternative<std::monostate>(value);
        result[offset] = valid && ((value == wanted_view) == equal);
        if (validity)
            (*validity)[offset] = valid;
    });
}

UpdatesRecord::Values
UpdatesRecord::Gather(int64_t id,
                      const std::vector<int64_t>& offsets,
                      uint64_t ts) const {
    Values values(offsets.size());
    std::vector<int64_t> missing;
    std::vector<size_t> positions;
    BaseReader base;
    {
        std::shared_lock lock(impl_->mutex);
        const auto& field = impl_->GetField(id);
        base = field.base;
        std::visit(
            [&](const auto& log) {
                for (size_t i = 0; i < offsets.size(); ++i) {
                    impl_->ValidateOffset(offsets[i]);
                    if (auto position =
                            impl_->Visible(field, log, offsets[i], ts))
                        values[i] = std::make_shared<const UpdateValue>(
                            Own(log.View(*position)));
                    else {
                        missing.push_back(offsets[i]);
                        positions.push_back(i);
                    }
                }
            },
            field.log);
    }
    if (!missing.empty()) {
        auto original = base(missing);
        Require(original.size() == missing.size(),
                "Incomplete base lookup result");
        for (size_t i = 0; i < original.size(); ++i) {
            Require(original[i] != nullptr, "Missing base value");
            values[positions[i]] = std::move(original[i]);
        }
    }
    impl_->value_rows.fetch_add(offsets.size(), std::memory_order_relaxed);
    impl_->base_rows.fetch_add(missing.size(), std::memory_order_relaxed);
    return values;
}
std::shared_ptr<const UpdateValue>
UpdatesRecord::Lookup(int64_t id, int64_t offset, uint64_t ts) const {
    return Gather(id, {offset}, ts).front();
}
UpdatesRecord::Values
UpdatesRecord::Scan(int64_t id,
                    int64_t begin,
                    size_t count,
                    uint64_t ts) const {
    impl_->ValidateRange(begin, count);
    std::vector<int64_t> offsets(count);
    std::iota(offsets.begin(), offsets.end(), begin);
    return Gather(id, offsets, ts);
}
std::vector<UpdatesRecord::RangeUpdate>
UpdatesRecord::ReadRangeUpdates(int64_t id,
                                uint64_t ts,
                                int64_t begin,
                                size_t count,
                                const TargetBitmapView* candidates,
                                size_t candidate_offset) const {
    impl_->ValidateRange(begin, count);
    const bool masked = candidates && !candidates->empty();
    Require(!masked || (candidate_offset <= candidates->size() &&
                        count <= candidates->size() - candidate_offset),
            "Invalid update candidate mask range");
    std::vector<RangeUpdate> patches;
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->GetField(id);
    auto updated = field.updated_offsets.view(begin, count);
    uint64_t visited = 0;
    std::visit(
        [&](const auto& log) {
            for (auto pos = updated.find_first(); pos;
                 pos = updated.find_next(*pos)) {
                if (masked && !(*candidates)[candidate_offset + *pos])
                    continue;
                ++visited;
                if (auto version = impl_->Visible(field, log, begin + *pos, ts))
                    patches.push_back({*pos,
                                       std::make_shared<const UpdateValue>(
                                           Own(log.View(*version)))});
            }
        },
        field.log);
    impl_->overlay_batches.fetch_add(1, std::memory_order_relaxed);
    impl_->overlay_history_rows.fetch_add(visited, std::memory_order_relaxed);
    impl_->overlay_rows.fetch_add(patches.size(), std::memory_order_relaxed);
    return patches;
}

UpdatesRecord::Stats
UpdatesRecord::GetStats() const {
    std::shared_lock lock(impl_->mutex);
    Stats stats;
    for (const auto& [id, field] : impl_->fields) {
        stats.updated_cells += field->updated_cells;
        stats.updated_offset_bitmap_bytes +=
            field->updated_offsets.size_in_bytes();
        stats.update_versions += std::visit(
            [](const auto& log) { return log.entries.size(); }, field->log);
        stats.directory_refs += field->directory_refs;
    }
#define READ_STAT(name) stats.name = impl_->name.load(std::memory_order_relaxed)
    READ_STAT(value_rows);
    READ_STAT(base_rows);
    READ_STAT(correction_rows);
    READ_STAT(old_index_reads);
    READ_STAT(value_reads);
    READ_STAT(ts_entries_visited);
    READ_STAT(ts_selected_rows);
    READ_STAT(ts_unique_reads);
    READ_STAT(ts_dedup_reads);
    READ_STAT(ts_visited_bitmap_bytes);
    READ_STAT(lookup_latest);
    READ_STAT(lookup_history);
    READ_STAT(overlay_batches);
    READ_STAT(overlay_history_rows);
    READ_STAT(overlay_rows);
#undef READ_STAT
    return stats;
}
void
UpdatesRecord::RecordOldIndexRead() const {
    impl_->old_index_reads.fetch_add(1, std::memory_order_relaxed);
}
void
UpdatesRecord::RecordRead() const {
    impl_->value_reads.fetch_add(1, std::memory_order_relaxed);
}
detail::UpdateReadBinding
UpdatesRecord::CaptureReadBinding(int64_t id, uint64_t ts) const {
    std::shared_lock lock(impl_->mutex);
    return {std::shared_ptr<const UpdatesRecord>(new UpdatesRecord(impl_)),
            id,
            ts,
            static_cast<int64_t>(impl_->row_count),
            impl_->GetField(id).type};
}
}  // namespace milvus::segcore::experimental
