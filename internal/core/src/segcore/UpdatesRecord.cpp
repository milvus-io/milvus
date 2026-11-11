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

#include "segcore/UpdatesRecord.h"

#include <absl/container/btree_map.h>
#include <absl/container/inlined_vector.h>
#include <algorithm>
#include <deque>
#include <iterator>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::segcore::experimental {
namespace {

constexpr uint64_t kInfinity = std::numeric_limits<uint64_t>::max();

// These APIs consume internal, already typed POC input. A violated contract is
// a caller bug, not an API client's InvalidParameter error.
void
Require(bool condition, const char* message) {
    if (!condition) {
        throw SegcoreError(UnexpectedError, message);
    }
}

struct Version {
    std::shared_ptr<const UpdateValue> value;
    uint64_t begin_ts;
    uint64_t end_ts = kInfinity;
};

// Most initial cells need one ref: avoid a heap allocation per history. Growth
// remains append-only, with random access for binary search. GC is out of scope.
using History = absl::InlinedVector<Version*, 1>;

const Version*
Visible(const History& history, uint64_t query_ts) {
    auto it = std::upper_bound(history.begin(),
                               history.end(),
                               query_ts,
                               [](uint64_t ts, const Version* version) {
                                   return ts < version->begin_ts;
                               });
    if (it == history.begin()) {
        return nullptr;
    }
    auto* version = *std::prev(it);
    return version->end_ts == kInfinity || query_ts < version->end_ts ? version
                                                                      : nullptr;
}

using Postings = std::unordered_map<int64_t, History>;

// variant ordering puts the dedicated NULL key before INT64/VARCHAR. A field
// contains only one non-null type. Strings use byte lexicographic ordering.
// B-tree mutations invalidate iterators: retain neither them nor key addresses.
// Heap-owned posting groups and deque-owned versions have stable addresses.
using AllDataVersionedIndex =
    absl::btree_map<UpdateValue, std::unique_ptr<Postings>>;

struct Field {
    UpdateFieldType type;
    bool nullable;
    std::deque<Version> versions;
    std::vector<History> offsets;
    std::unique_ptr<AllDataVersionedIndex> index;

    void
    Validate(const UpdateValue& value) const {
        if (std::holds_alternative<std::monostate>(value)) {
            Require(nullable, "NULL assignment to a non-nullable POC field");
        } else {
            Require(type == UpdateFieldType::Int64
                        ? std::holds_alternative<int64_t>(value)
                        : std::holds_alternative<std::string>(value),
                    "POC field value type mismatch");
        }
    }

    History&
    IndexHistory(const UpdateValue& value, int64_t offset) {
        auto it = index->find(value);
        if (it == index->end()) {
            it = index->emplace(value, std::make_unique<Postings>()).first;
        }
        return (*it->second)[offset];
    }
};

void
Collect(const Postings& postings,
        uint64_t query_ts,
        std::vector<int64_t>& result) {
    for (const auto& [offset, history] : postings) {
        if (Visible(history, query_ts)) {
            result.push_back(offset);
        }
    }
}

void
SortUnique(std::vector<int64_t>& result) {
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
}

}  // namespace

class UpdatesRecord::Impl {
 public:
    Impl(size_t row_count, bool enable_temp_index)
        : row_count(row_count), enable_temp_index(enable_temp_index) {
        Require(row_count <=
                    static_cast<size_t>(std::numeric_limits<int64_t>::max()),
                "POC row count exceeds offset range");
    }

    Field&
    GetField(int64_t field_id) const {
        auto it = fields.find(field_id);
        Require(it != fields.end(), "POC field is not initialized");
        return *it->second;
    }

    Field&
    IndexedField(int64_t field_id) const {
        auto& field = GetField(field_id);
        if (!field.index) {
            throw SegcoreError(Unsupported, "POC temp index is disabled");
        }
        return field;
    }

    void
    ValidateOffset(int64_t offset) const {
        Require(offset >= 0 && static_cast<size_t>(offset) < row_count,
                "POC row offset is outside the physical segment");
    }

    const size_t row_count;
    const bool enable_temp_index;
    mutable std::shared_mutex mutex;
    std::unordered_map<int64_t, std::unique_ptr<Field>> fields;
    std::optional<uint64_t> last_mutation_ts;
};

UpdatesRecord::UpdatesRecord(size_t row_count, bool enable_temp_index)
    : impl_(std::make_shared<Impl>(row_count, enable_temp_index)) {
}

UpdatesRecord::UpdatesRecord(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {
}

UpdatesRecord::~UpdatesRecord() = default;

detail::UpdateReadBinding
UpdatesRecord::CaptureReadBinding(int64_t field_id, uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->GetField(field_id);
    return {std::shared_ptr<const UpdatesRecord>(new UpdatesRecord(impl_)),
            field_id,
            query_ts,
            static_cast<int64_t>(impl_->row_count),
            field.type,
            field.index != nullptr};
}

void
UpdatesRecord::InitializeField(int64_t field_id,
                               UpdateFieldType type,
                               bool nullable,
                               const std::vector<UpdateValue>& values,
                               const std::vector<uint64_t>& begin_timestamps) {
    std::unique_lock lock(impl_->mutex);
    Require(!impl_->last_mutation_ts,
            "POC fields must be initialized before update apply");
    Require(impl_->fields.count(field_id) == 0,
            "POC field is already initialized");
    Require(type == UpdateFieldType::Int64 || type == UpdateFieldType::VarChar,
            "Unsupported POC field type");
    Require(values.size() == impl_->row_count &&
                begin_timestamps.size() == impl_->row_count,
            "POC initial values must cover the physical segment");

    auto field = std::make_unique<Field>();
    field->type = type;
    field->nullable = nullable;
    field->offsets.resize(impl_->row_count);
    if (impl_->enable_temp_index) {
        field->index = std::make_unique<AllDataVersionedIndex>();
    }
    for (size_t offset = 0; offset < values.size(); ++offset) {
        field->Validate(values[offset]);
        Require(begin_timestamps[offset] < kInfinity,
                "Infinity is reserved for an open version end");
        field->versions.push_back(
            {std::make_shared<const UpdateValue>(values[offset]),
             begin_timestamps[offset],
             kInfinity});
        auto* version = &field->versions.back();
        field->offsets[offset].push_back(version);
        if (field->index) {
            field->IndexHistory(values[offset], offset).push_back(version);
        }
    }
    impl_->fields.emplace(field_id, std::move(field));
}

size_t
UpdatesRecord::Apply(const std::vector<UpdateValue>& primary_keys,
                     uint64_t mutation_ts,
                     const std::vector<UpdateAssignment>& assignments,
                     const ResolveRows& resolve_rows) {
    std::unique_lock lock(impl_->mutex);
    Require(mutation_ts < kInfinity,
            "Infinity is reserved for an open version end");
    Require(!impl_->last_mutation_ts || mutation_ts >= *impl_->last_mutation_ts,
            "POC mutations must arrive in timestamp order");
    Require(!assignments.empty(), "POC update has no assignments");
    Require(static_cast<bool>(resolve_rows), "POC PK resolver is missing");
    for (const auto& pk : primary_keys) {
        Require(!std::holds_alternative<std::monostate>(pk),
                "POC primary keys cannot be NULL");
    }

    std::unordered_set<int64_t> assigned_fields;
    for (const auto& assignment : assignments) {
        Require(assigned_fields.insert(assignment.field_id).second,
                "POC update assigns a field more than once");
        impl_->GetField(assignment.field_id).Validate(assignment.value);
    }
    auto offsets = resolve_rows(primary_keys, mutation_ts);
    SortUnique(offsets);
    for (auto offset : offsets) {
        impl_->ValidateOffset(offset);
        for (const auto& assignment : assignments) {
            const auto& history =
                impl_->GetField(assignment.field_id).offsets[offset];
            Require(history.back()->begin_ts < mutation_ts,
                    "POC cell timestamps must be strictly increasing");
        }
    }

    struct Pending {
        Field* field;
        History* history;
        History* index_history;
        Version* version;
    };
    std::vector<Pending> pending;
    Require(offsets.empty() ||
                assignments.size() <= pending.max_size() / offsets.size(),
            "POC update batch is too large");
    pending.reserve(assignments.size() * offsets.size());
    try {
        for (const auto& assignment : assignments) {
            auto& field = impl_->GetField(assignment.field_id);
            // One immutable value payload for every row in this assignment.
            auto value = std::make_shared<const UpdateValue>(assignment.value);
            for (auto offset : offsets) {
                auto& history = field.offsets[offset];
                history.reserve(history.size() + 1);
                History* index_history = nullptr;
                if (field.index) {
                    index_history = &field.IndexHistory(*value, offset);
                    index_history->reserve(index_history->size() + 1);
                }
                field.versions.push_back({value, mutation_ts, kInfinity});
                pending.push_back(
                    {&field, &history, index_history, &field.versions.back()});
            }
        }
    } catch (...) {
        // No history refs or closed intervals were published yet. Empty posting
        // groups left by failed allocation contain no visible version.
        for (auto it = pending.rbegin(); it != pending.rend(); ++it) {
            it->field->versions.pop_back();
        }
        throw;
    }

    // All allocations finished. Both ref paths are installed under the same
    // write lock before a reader can observe the newly closed/open intervals.
    for (auto& item : pending) {
        item.history->back()->end_ts = mutation_ts;
        item.history->push_back(item.version);
        if (item.index_history) {
            item.index_history->push_back(item.version);
        }
    }
    impl_->last_mutation_ts = mutation_ts;
    return offsets.size();
}

std::shared_ptr<const UpdateValue>
UpdatesRecord::Lookup(int64_t field_id,
                      int64_t offset,
                      uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    impl_->ValidateOffset(offset);
    const auto* version =
        Visible(impl_->GetField(field_id).offsets[offset], query_ts);
    return version ? version->value : nullptr;
}

std::vector<std::shared_ptr<const UpdateValue>>
UpdatesRecord::Gather(int64_t field_id,
                      const std::vector<int64_t>& offsets,
                      uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->GetField(field_id);
    std::vector<std::shared_ptr<const UpdateValue>> result;
    result.reserve(offsets.size());
    for (auto offset : offsets) {
        impl_->ValidateOffset(offset);
        const auto* version = Visible(field.offsets[offset], query_ts);
        result.push_back(version ? version->value : nullptr);
    }
    return result;
}

std::vector<std::shared_ptr<const UpdateValue>>
UpdatesRecord::Scan(int64_t field_id,
                    int64_t begin_offset,
                    size_t count,
                    uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    Require(begin_offset >= 0 &&
                static_cast<size_t>(begin_offset) <= impl_->row_count &&
                count <= impl_->row_count - static_cast<size_t>(begin_offset),
            "POC scan range is outside the physical segment");
    const auto& field = impl_->GetField(field_id);
    std::vector<std::shared_ptr<const UpdateValue>> result;
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        const auto* version =
            Visible(field.offsets[begin_offset + i], query_ts);
        result.push_back(version ? version->value : nullptr);
    }
    return result;
}

std::vector<int64_t>
UpdatesRecord::In(int64_t field_id,
                  const std::vector<UpdateValue>& values,
                  uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->IndexedField(field_id);
    std::vector<int64_t> result;
    for (const auto& value : values) {
        // NULL in an IN list never contributes a TRUE result.
        if (std::holds_alternative<std::monostate>(value)) {
            continue;
        }
        field.Validate(value);
        auto it = field.index->find(value);
        if (it != field.index->end()) {
            Collect(*it->second, query_ts, result);
        }
    }
    SortUnique(result);
    return result;
}

std::vector<int64_t>
UpdatesRecord::Range(int64_t field_id,
                     const std::optional<UpdateRangeBound>& lower,
                     const std::optional<UpdateRangeBound>& upper,
                     uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->IndexedField(field_id);
    for (const auto* bound : {&lower, &upper}) {
        if (*bound) {
            Require(!std::holds_alternative<std::monostate>((*bound)->value),
                    "POC range bounds cannot be NULL");
            field.Validate((*bound)->value);
        }
    }
    if (lower && upper &&
        (upper->value < lower->value ||
         (upper->value == lower->value &&
          (!lower->inclusive || !upper->inclusive)))) {
        return {};
    }
    const auto& tree = *field.index;
    auto begin = !lower ? tree.upper_bound(UpdateValue{std::monostate{}})
                 : lower->inclusive ? tree.lower_bound(lower->value)
                                    : tree.upper_bound(lower->value);
    auto end = !upper             ? tree.end()
               : upper->inclusive ? tree.upper_bound(upper->value)
                                  : tree.lower_bound(upper->value);
    std::vector<int64_t> result;
    for (auto it = begin; it != end; ++it) {
        Collect(*it->second, query_ts, result);
    }
    SortUnique(result);
    return result;
}

std::vector<int64_t>
UpdatesRecord::Prefix(int64_t field_id,
                      const std::string& prefix,
                      uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->IndexedField(field_id);
    Require(field.type == UpdateFieldType::VarChar,
            "POC prefix lookup requires a VARCHAR field");
    std::vector<int64_t> result;
    const auto& tree = *field.index;
    for (auto it = tree.lower_bound(UpdateValue{prefix}); it != tree.end();
         ++it) {
        const auto& value = std::get<std::string>(it->first);
        if (value.compare(0, prefix.size(), prefix) != 0) {
            break;
        }
        Collect(*it->second, query_ts, result);
    }
    SortUnique(result);
    return result;
}

std::vector<int64_t>
UpdatesRecord::IsNull(int64_t field_id, uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    const auto& tree = *impl_->IndexedField(field_id).index;
    std::vector<int64_t> result;
    auto it = tree.find(UpdateValue{std::monostate{}});
    if (it != tree.end()) {
        Collect(*it->second, query_ts, result);
    }
    SortUnique(result);
    return result;
}

std::vector<int64_t>
UpdatesRecord::IsNotNull(int64_t field_id, uint64_t query_ts) const {
    return Range(field_id, std::nullopt, std::nullopt, query_ts);
}

}  // namespace milvus::segcore::experimental
