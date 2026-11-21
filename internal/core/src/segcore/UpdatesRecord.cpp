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

#include "segcore/UpdatesRecord.h"

#include "segcore/PkUpdateReadObserver.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>

#include "google/protobuf/util/message_differencer.h"

namespace milvus::segcore {
namespace {
using MutationKey = std::pair<Timestamp, uint32_t>;
using Directory = std::unordered_map<int64_t, std::vector<size_t>>;
using Commit = std::function<void()>;

void
RequireUpdate(bool condition, const char* message) {
    if (!condition) {
        ThrowInfo(DataFormatBroken, "Invalid update record: {}", message);
    }
}

template <typename T>
void
ReserveAppend(std::vector<T>& values, size_t added) {
    if (values.capacity() - values.size() < added) {
        values.reserve(std::max(values.size() + added, values.capacity() * 2));
    }
}

template <typename T>
bool
EqualValue(const T& left, const T& right) {
    return left == right;
}
bool
EqualValue(const UpdateArray& left, const UpdateArray& right) {
    return google::protobuf::util::MessageDifferencer::Equals(*left, *right);
}
bool
EqualValue(const UpdateString& left, const UpdateString& right) {
    return *left == *right;
}

template <typename T>
struct TypedLog {
    struct Delta {
        MutationKey key;
        int64_t offset;
        T value;
        bool is_null;
    };
    std::vector<Delta> entries;
    Directory positions;
    std::vector<bool> membership;
    std::optional<Timestamp> collected_through;

    TypedLog
    Compact(Timestamp safe_ts) const {
        // Mark only superseded positions. Equal-timestamp siblings are
        // ordered by source ordinal, so the last prefix entry wins.
        std::vector<bool> remove(entries.size());
        size_t retained = entries.size();
        for (const auto& [row, history] : positions) {
            auto end = std::upper_bound(history.begin(),
                                        history.end(),
                                        safe_ts,
                                        [&](Timestamp ts, size_t pos) {
                                            return ts < entries[pos].key.first;
                                        });
            if (end == history.begin())
                continue;
            for (auto it = history.begin(); it != std::prev(end); ++it) {
                remove[*it] = true;
                --retained;
            }
        }
        TypedLog result;
        result.collected_through = safe_ts;
        result.entries.reserve(retained);
        result.membership = membership;  // Every history retains a winner.
        for (size_t i = 0; i < entries.size(); ++i) {
            if (!remove[i]) {
                result.positions[entries[i].offset].push_back(
                    result.entries.size());
                result.entries.push_back(entries[i]);
            }
        }
        return result;
    }

    Commit
    Prepare(const std::vector<int64_t>& rows,
            MutationKey key,
            const T& value,
            bool is_null,
            size_t capacity) {
        auto additions = std::make_shared<std::vector<Delta>>();
        for (auto row : rows) {
            auto it = positions.find(row);
            if (it != positions.end() &&
                entries[it->second.back()].key >= key) {
                auto pos = std::lower_bound(it->second.begin(),
                                            it->second.end(),
                                            key,
                                            [&](size_t i, MutationKey target) {
                                                return entries[i].key < target;
                                            });
                if (pos != it->second.end() && entries[*pos].key == key) {
                    const auto& old = entries[*pos];
                    RequireUpdate(old.is_null == is_null &&
                                      (is_null || EqualValue(old.value, value)),
                                  "conflicting payload for replayed cell");
                    continue;
                }
            }
            // All mutations through this field's cutoff were applied before
            // collection. Missing prefix identities must not be resurrected.
            if (collected_through && key.first <= *collected_through)
                continue;
            additions->push_back({key, row, value, is_null});
        }
        if (additions->empty()) {
            return [] {};
        }
        if (!entries.empty() && key < entries.back().key) {
            // A recut history fragment may contribute a previously unseen cell
            // older than the live suffix. Only this field needs a replacement.
            auto rebuilt = std::make_shared<TypedLog>();
            rebuilt->collected_through = collected_through;
            auto middle =
                std::upper_bound(entries.begin(),
                                 entries.end(),
                                 key,
                                 [](MutationKey target, const Delta& item) {
                                     return target < item.key;
                                 });
            auto& output = rebuilt->entries;
            output.reserve(entries.size() + additions->size());
            output.insert(output.end(), entries.begin(), middle);
            output.insert(output.end(), additions->begin(), additions->end());
            output.insert(output.end(), middle, entries.end());
            rebuilt->membership.resize(capacity);
            for (size_t i = 0; i < output.size(); ++i) {
                rebuilt->positions[output[i].offset].push_back(i);
                rebuilt->membership[output[i].offset] = true;
            }
            return [this, rebuilt] { *this = std::move(*rebuilt); };
        }

        // Preallocate entries, dictionary nodes and each history under the
        // write lock. Existing histories change capacity only until commit.
        auto new_rows = std::make_shared<Directory>();
        size_t pos = entries.size();
        for (const auto& delta : *additions) {
            auto existing = positions.find(delta.offset);
            if (existing == positions.end()) {
                (*new_rows)[delta.offset].push_back(pos);
            } else {
                ReserveAppend(existing->second, 1);
            }
            ++pos;
        }
        ReserveAppend(entries, additions->size());
        const size_t needed = positions.size() + new_rows->size();
        if (needed > positions.bucket_count() ||
            (!new_rows->empty() && positions.empty())) {
            positions.reserve(std::max(needed, positions.bucket_count() * 2));
        }
        membership.reserve(capacity);
        return [this, additions, new_rows, capacity] {
            membership.resize(capacity);
            for (auto& delta : *additions) {
                auto existing = positions.find(delta.offset);
                if (existing != positions.end()) {
                    existing->second.push_back(entries.size());
                }
                membership[delta.offset] = true;
                entries.push_back(std::move(delta));
            }
            positions.merge(*new_rows);
        };
    }

    bool
    Contains(int64_t row) const {
        return row >= 0 && static_cast<size_t>(row) < membership.size() &&
               membership[row];
    }

    std::optional<UpdateValue>
    Lookup(int64_t row, Timestamp ts) const {
        if (!Contains(row)) {
            return std::nullopt;
        }
        return LookupKnownRow(row, ts);
    }

    std::optional<UpdateValue>
    LookupKnownRow(int64_t row, Timestamp ts) const {
        const auto& history = positions.at(row);
        size_t selected = history.back();
        if (entries[selected].key.first > ts) {
            auto end =
                std::upper_bound(history.begin(),
                                 history.end(),
                                 ts,
                                 [&](Timestamp target, size_t i) {
                                     return target < entries[i].key.first;
                                 });
            if (end == history.begin()) {
                return std::nullopt;
            }
            selected = *std::prev(end);
        }
        const auto& entry = entries[selected];
        return entry.is_null ? UpdateValue{} : UpdateValue(entry.value);
    }
};

using Log = std::variant<TypedLog<bool>,
                         TypedLog<int8_t>,
                         TypedLog<int16_t>,
                         TypedLog<int32_t>,
                         TypedLog<int64_t>,
                         TypedLog<UpdateString>,
                         TypedLog<UpdateArray>>;

template <typename T>
bool
Fits(int32_t value) {
    return value >= std::numeric_limits<T>::min() &&
           value <= std::numeric_limits<T>::max();
}

bool
ValidArray(DataType element, const proto::schema::ScalarField& array) {
    switch (element) {
        case DataType::BOOL:
            return array.has_bool_data();
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
            return array.has_int_data() &&
                   std::all_of(array.int_data().data().begin(),
                               array.int_data().data().end(),
                               [element](int32_t n) {
                                   return (element != DataType::INT8 ||
                                           Fits<int8_t>(n)) &&
                                          (element != DataType::INT16 ||
                                           Fits<int16_t>(n));
                               });
        case DataType::INT64:
            return array.has_long_data();
        case DataType::FLOAT:
            return array.has_float_data();
        case DataType::DOUBLE:
            return array.has_double_data();
        case DataType::VARCHAR:
            return array.has_string_data();
        default:
            return false;
    }
}

// NULL uses a typed placeholder only to dispatch to the correct log.
UpdateValue
Decode(const FieldMeta& meta, const proto::schema::UpdateLiteral& literal) {
    bool null = literal.has_null_value();
    RequireUpdate(!null || (meta.is_nullable() && literal.null_value() == 0),
                  "illegal NULL assignment");
    const auto& v = literal.scalar_value();
    switch (meta.get_data_type()) {
        case DataType::BOOL:
            RequireUpdate(null || v.has_bool_data(), "expected BOOL");
            return v.bool_data();
        case DataType::INT8:
            RequireUpdate(
                null || (v.has_int_data() && Fits<int8_t>(v.int_data())),
                "expected INT8");
            return static_cast<int8_t>(v.int_data());
        case DataType::INT16:
            RequireUpdate(
                null || (v.has_int_data() && Fits<int16_t>(v.int_data())),
                "expected INT16");
            return static_cast<int16_t>(v.int_data());
        case DataType::INT32:
            RequireUpdate(null || v.has_int_data(), "expected INT32");
            return v.int_data();
        case DataType::INT64:
            RequireUpdate(null || v.has_long_data(), "expected INT64");
            return v.long_data();
        case DataType::TIMESTAMPTZ:
            RequireUpdate(null || v.has_timestamptz_data(),
                          "expected TIMESTAMPTZ");
            return v.timestamptz_data();
        case DataType::VARCHAR:
            RequireUpdate(null || v.has_string_data(), "expected VARCHAR");
            return null ? UpdateString{}
                        : std::make_shared<const std::string>(v.string_data());
        case DataType::ARRAY: {
            const auto element = meta.get_element_type();
            RequireUpdate(
                element == DataType::BOOL || element == DataType::INT8 ||
                    element == DataType::INT16 || element == DataType::INT32 ||
                    element == DataType::INT64 || element == DataType::FLOAT ||
                    element == DataType::DOUBLE || element == DataType::VARCHAR,
                "unsupported ARRAY element");
            RequireUpdate(null || (literal.has_array_value() &&
                                   ValidArray(element, literal.array_value())),
                          "expected scalar ARRAY");
            return null ? UpdateArray{}
                        : std::make_shared<const proto::schema::ScalarField>(
                              literal.array_value());
        }
        default:
            ThrowInfo(DataFormatBroken,
                      "Unsupported update type {}",
                      meta.get_data_type());
    }
}

}  // namespace

thread_local PkUpdateReadObserver pk_update_read_observer = nullptr;

struct UpdatesRecord::Impl {
    std::shared_mutex mutex;
    std::atomic<size_t> reverse_scans{0}, reverse_selected{0},
        overlay_batches{0}, dictionary_probes{0};
    std::map<int64_t, Log> fields;
    size_t row_count = 0;
    std::optional<Timestamp> collected_through;
};

UpdatesRecord::UpdatesRecord() : impl_(std::make_unique<Impl>()) {
}
UpdatesRecord::~UpdatesRecord() = default;

void
UpdatesRecord::GC(Timestamp safe_ts) {
    decltype(impl_->fields) replacement;
    std::unique_lock lock(impl_->mutex);
    if (impl_->collected_through && safe_ts <= *impl_->collected_through)
        return;
    for (const auto& [field, log] : impl_->fields) {
        replacement.emplace(field,
                            std::visit(
                                [safe_ts](const auto& typed) -> Log {
                                    return typed.Compact(safe_ts);
                                },
                                log));
    }
    // Publish all rebuilt dictionaries and vectors together. Readers holding
    // the shared lock have finished; returned values own their payloads.
    impl_->fields.swap(replacement);
    impl_->collected_through = safe_ts;
}

void
UpdatesRecord::Grow(size_t rows) {
    std::unique_lock lock(impl_->mutex);
    if (rows <= impl_->row_count)
        return;
    for (auto& [field, log] : impl_->fields) {
        std::visit(
            [rows](auto& typed) {
                ReserveAppend(typed.membership, rows - typed.membership.size());
            },
            log);
    }
    for (auto& [field, log] : impl_->fields) {
        std::visit([rows](auto& typed) { typed.membership.resize(rows); }, log);
    }
    impl_->row_count = rows;
}

void
UpdatesRecord::Apply(const Schema& schema,
                     size_t row_count,
                     const std::vector<int64_t>& offsets,
                     Timestamp timestamp,
                     uint32_t ordinal,
                     const std::vector<UpdateAssignment>& assignments,
                     const std::function<void()>& before_publish) {
    RequireUpdate(!assignments.empty(), "empty assignment list");
    std::vector<int64_t> rows(offsets);
    std::sort(rows.begin(), rows.end());
    rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
    RequireUpdate(
        rows.empty() ||
            (rows.front() >= 0 && static_cast<size_t>(rows.back()) < row_count),
        "offset outside physical segment");
    std::set<int64_t> seen;
    std::map<int64_t, Log> new_fields;
    std::vector<Commit> commits;
    std::unique_lock lock(impl_->mutex);
    const size_t capacity = std::max(impl_->row_count, row_count);
    for (const auto& a : assignments) {
        RequireUpdate(
            a.field.get() >= START_USER_FIELDID &&
                schema.get_primary_field_id().value_or(FieldId(-1)).get() !=
                    a.field.get() &&
                schema.get_ttl_field_id().value_or(FieldId(-1)).get() !=
                    a.field.get() &&
                schema.has_field(a.field),
            "unknown or restricted update field");
        RequireUpdate(seen.insert(a.field.get()).second,
                      "duplicate field assignment");
        auto value = Decode(schema[a.field], a.literal);
        if (rows.empty())
            continue;
        auto it = impl_->fields.find(a.field.get());
        std::visit(
            [&](const auto& payload) {
                using T = std::decay_t<decltype(payload)>;
                if constexpr (!std::is_same_v<T, std::monostate>) {
                    Log& log =
                        it == impl_->fields.end()
                            ? new_fields
                                  .try_emplace(a.field.get(),
                                               std::in_place_type<TypedLog<T>>)
                                  .first->second
                            : it->second;
                    auto* typed = std::get_if<TypedLog<T>>(&log);
                    RequireUpdate(typed != nullptr,
                                  "update field type changed");
                    commits.push_back(typed->Prepare(rows,
                                                     {timestamp, ordinal},
                                                     payload,
                                                     a.literal.has_null_value(),
                                                     capacity));
                }
            },
            value);
    }
    if (before_publish)
        before_publish();
    for (auto& commit : commits) commit();
    impl_->fields.merge(new_fields);
    impl_->row_count = capacity;
}

std::optional<UpdateValue>
UpdatesRecord::Lookup(FieldId field,
                      int64_t offset,
                      Timestamp timestamp) const {
    std::shared_lock lock(impl_->mutex);
    auto it = impl_->fields.find(field.get());
    if (it == impl_->fields.end())
        return std::nullopt;
    return std::visit(
        [&](const auto& log) { return log.Lookup(offset, timestamp); },
        it->second);
}

UpdatesRecord::ReadDiagnostics
UpdatesRecord::Diagnostics() const {
    return {impl_->reverse_scans.load(std::memory_order_relaxed),
            impl_->reverse_selected.load(std::memory_order_relaxed),
            impl_->overlay_batches.load(std::memory_order_relaxed),
            impl_->dictionary_probes.load(std::memory_order_relaxed)};
}

UpdatesRecord::ReadStats
UpdatesRecord::VisitUpdates(
    FieldId field,
    Timestamp timestamp,
    const std::function<void(int64_t, const UpdateValue&)>& visit) const {
    ReadStats stats;
    size_t visited_bitmap_bits = 0;
    std::shared_lock lock(impl_->mutex);
    auto it = impl_->fields.find(field.get());
    if (it == impl_->fields.end())
        return stats;
    std::visit(
        [&](const auto& log) {
            auto end = std::upper_bound(log.entries.begin(),
                                        log.entries.end(),
                                        timestamp,
                                        [](Timestamp t, const auto& delta) {
                                            return t < delta.key.first;
                                        });
            if (end == log.entries.begin())
                return;
            stats.used_visited = log.positions.size() != log.entries.size();
            visited_bitmap_bits =
                stats.used_visited ? log.membership.size() : 0;
            std::vector<bool> visited(visited_bitmap_bits);
            while (end != log.entries.begin()) {
                const auto& delta = *--end;
                ++stats.scanned_entries;
                if (stats.used_visited) {
                    if (visited[delta.offset])
                        continue;
                    visited[delta.offset] = true;
                }
                visit(delta.offset,
                      delta.is_null ? UpdateValue{} : UpdateValue(delta.value));
                if (++stats.selected_offsets == log.positions.size())
                    break;
            }
        },
        it->second);
    impl_->reverse_scans.fetch_add(1, std::memory_order_relaxed);
    impl_->reverse_selected.fetch_add(stats.selected_offsets,
                                      std::memory_order_relaxed);
    ObservePkUpdateRead(
        field, {PkUpdateReadPath::Reverse, 0, stats, visited_bitmap_bits});
    return stats;
}

std::vector<UpdatesRecord::Patch>
UpdatesRecord::ReadRangeUpdates(FieldId field,
                                Timestamp timestamp,
                                int64_t start,
                                size_t count,
                                const int32_t* offsets,
                                const TargetBitmapView& candidates,
                                size_t mask_start,
                                ReadStats* stats) const {
    const auto observer = pk_update_read_observer;
    ReadStats measured;
    auto patches = ReadBatch(field,
                             timestamp,
                             start,
                             count,
                             offsets,
                             candidates,
                             mask_start,
                             observer ? &measured : stats);
    if (observer) {
        if (stats)
            *stats = measured;
        observer(field, {PkUpdateReadPath::Overlay, count, measured});
    }
    return patches;
}

std::vector<UpdatesRecord::Patch>
UpdatesRecord::Gather(FieldId field,
                      Timestamp timestamp,
                      const int64_t* offsets,
                      size_t count) const {
    return ReadBatch(field, timestamp, 0, count, offsets, {}, 0, nullptr);
}

template <typename Offset>
std::vector<UpdatesRecord::Patch>
UpdatesRecord::ReadBatch(FieldId field,
                         Timestamp timestamp,
                         int64_t start,
                         size_t count,
                         const Offset* offsets,
                         const TargetBitmapView& candidates,
                         size_t mask_start,
                         ReadStats* stats) const {
    AssertInfo(candidates.empty() || (mask_start <= candidates.size() &&
                                      count <= candidates.size() - mask_start),
               "update candidate mask does not cover batch");
    AssertInfo(
        offsets || (start >= 0 &&
                    count <= static_cast<size_t>(
                                 std::numeric_limits<int64_t>::max() - start)),
        "invalid update batch range");
    std::vector<Patch> patches;
    ReadStats local;
    std::shared_lock lock(impl_->mutex);
    auto it = impl_->fields.find(field.get());
    if (it != impl_->fields.end()) {
        std::visit(
            [&](const auto& log) {
                if (log.entries.empty() ||
                    log.entries.front().key.first > timestamp)
                    return;
                for (size_t i = 0; i < count; ++i) {
                    if (!candidates.empty() && !candidates[mask_start + i])
                        continue;
                    const int64_t row =
                        offsets ? offsets[i] : start + static_cast<int64_t>(i);
                    if (!log.Contains(row))
                        continue;
                    ++local.dictionary_probes;
                    if (auto value = log.LookupKnownRow(row, timestamp)) {
                        patches.push_back({i, std::move(*value)});
                    }
                }
            },
            it->second);
    }
    local.selected_offsets = patches.size();
    if (local.dictionary_probes)
        impl_->dictionary_probes.fetch_add(local.dictionary_probes,
                                           std::memory_order_relaxed);
    if (!patches.empty())
        impl_->overlay_batches.fetch_add(1, std::memory_order_relaxed);
    if (stats)
        *stats = local;
    return patches;
}

UpdatesRecord::StoreStats
UpdatesRecord::StoreStatistics() const {
    StoreStats stats;
    std::shared_lock lock(impl_->mutex);
    stats.fields = impl_->fields.size();
    for (const auto& [id, field] : impl_->fields) {
        std::visit(
            [&](const auto& log) {
                stats.versions += log.entries.size();
                stats.distinct_field_offsets += log.positions.size();
                stats.entry_and_membership_bytes +=
                    log.entries.capacity() * sizeof(log.entries.front()) +
                    (log.membership.capacity() + 7) / 8;
            },
            field);
    }
    return stats;
}

UpdatesRecord::FieldStats
UpdatesRecord::Stats(FieldId field) const {
    std::shared_lock lock(impl_->mutex);
    auto it = impl_->fields.find(field.get());
    if (it == impl_->fields.end())
        return {};
    return std::visit(
        [](const auto& log) {
            return FieldStats{log.entries.size(),
                              log.positions.size(),
                              log.membership.size()};
        },
        it->second);
}
}  // namespace milvus::segcore
