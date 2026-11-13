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
#include <atomic>

#include <absl/container/btree_map.h>
#include <absl/container/inlined_vector.h>
#include <algorithm>
#include <deque>
#include <iterator>
#include <limits>
#include <mutex>
#include <numeric>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/EasyAssert.h"
#include "segcore/PostingVisibility.h"

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

// Sparse update histories share stable Version records. Growth
// remains append-only, with random access for binary search. GC is out of scope.
using History = absl::InlinedVector<Version*, 1>;

// Sparse lookup remains indexed, while correction scans compact entries.
// Fixed blocks keep histories stable across append and pending publication.
class OffsetHistories {
 public:
    struct Entry {
        int64_t offset = 0;
        History history;
    };
    static constexpr size_t kBlockSize = 1024;

    History&
    operator[](int64_t offset) {
        auto found = positions_.find(offset);
        if (found != positions_.end()) {
            return At(found->second).history;
        }
        if (size_ == blocks_.size() * kBlockSize) {
            blocks_.push_back(std::make_unique<Entry[]>(kBlockSize));
        }
        // A failed insertion may leave an empty block, but no visible entry.
        positions_.emplace(offset, size_);
        auto& entry = At(size_++);
        entry.offset = offset;
        return entry.history;
    }

    const History*
    Find(int64_t offset) const {
        auto found = positions_.find(offset);
        return found == positions_.end() ? nullptr : &At(found->second).history;
    }

    template <typename Visitor>
    void
    ForEach(Visitor&& visit) const {
        size_t remaining = size_;
        for (const auto& block : blocks_) {
            const auto count = std::min(remaining, kBlockSize);
            for (size_t i = 0; i < count; ++i) {
                visit(block[i].offset, block[i].history);
            }
            remaining -= count;
        }
    }

 private:
    Entry&
    At(size_t position) const {
        return blocks_[position / kBlockSize][position % kBlockSize];
    }
    std::unordered_map<int64_t, size_t> positions_;
    std::vector<std::unique_ptr<Entry[]>> blocks_;
    size_t size_ = 0;
};

const Version*
Visible(const History& history, uint64_t query_ts) {
    if (history.empty()) {
        return nullptr;
    }
    const auto* latest = history.back();
    if (query_ts >= latest->begin_ts) {
        // A value-specific posting history may already have ended. Earlier
        // intervals cannot become visible again after its latest interval.
        return latest->end_ts == kInfinity || query_ts < latest->end_ts
                   ? latest
                   : nullptr;
    }
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

constexpr size_t kPostingBlockSize = 256;
constexpr size_t kMinLatestBitmapPostings = 4096;
constexpr size_t kLatestBitmapBudgetPerField = 8 * 1024 * 1024;

struct Postings;

struct PostingBlock {
    PostingBlock(size_t capacity, Postings* owner)
        : offsets(new int64_t[capacity]),
          last_begin(new uint64_t[capacity]),
          last_end(new uint64_t[capacity]),
          histories(new History[capacity]),
          capacity(capacity),
          owner(owner) {
    }

    // Scan these arrays without following history/version pointers.
    std::unique_ptr<int64_t[]> offsets;
    std::unique_ptr<uint64_t[]> last_begin;
    std::unique_ptr<uint64_t[]> last_end;
    std::unique_ptr<History[]> histories;
    const size_t capacity;
    Postings* const owner;
    size_t size = 0;
};

struct PostingPosition {
    PostingBlock* block;
    size_t slot;
};

struct Postings {
    explicit Postings(bool is_null) : is_null(is_null) {
    }

    // Writer lookup only; readers walk dense blocks.
    std::unordered_map<int64_t, PostingPosition> slots;
    std::vector<std::unique_ptr<PostingBlock>> blocks;
    size_t size = 0;
    const bool is_null;
    TargetBitmap latest;
    uint64_t latest_since = 0;

    PostingPosition
    AppendSlot(int64_t offset) {
        if (blocks.empty() || blocks.back()->size == blocks.back()->capacity) {
            // Rare values should not pay for a full 256-slot block.
            const size_t capacity =
                blocks.empty()
                    ? 1
                    : std::min(kPostingBlockSize, blocks.back()->capacity * 2);
            blocks.push_back(std::make_unique<PostingBlock>(capacity, this));
        }
        auto& block = *blocks.back();
        const size_t slot = block.size;
        PostingPosition position{&block, slot};
        block.offsets[slot] = offset;
        block.last_begin[slot] = kInfinity;
        block.last_end[slot] = 0;  // reserved slot, no published version
        ++block.size;
        ++size;
        return position;
    }

    PostingPosition
    EnsureSlot(int64_t offset) {
        auto [it, inserted] = slots.try_emplace(offset, PostingPosition{});
        if (!it->second.block)
            it->second = AppendSlot(offset);
        return it->second;
    }
};

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
    // Index-only base intervals are created only on first update. They carry
    // no copied base payload and are never exposed by the value reader.
    std::deque<Version> base_intervals;
    // Writer-only current posting location, indexed by row offset. Base slots
    // enter the per-value lookup map lazily when their row is first updated.
    std::vector<PostingPosition> current_postings;
    OffsetHistories offsets;
    UpdatesRecord::BaseReader base_reader;
    uint64_t base_index_rows = 0, updated_cells = 0;
    std::unique_ptr<AllDataVersionedIndex> index;
    // Only indexed nullable fields need this bitmap. Reads clone it under the
    // store lock; returned snapshots never alias subsequent writer changes.
    TargetBitmap latest_validity;
    uint64_t latest_validity_since = 0;
    size_t latest_posting_bytes = 0, latest_posting_count = 0;

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

    Postings&
    IndexPostings(const UpdateValue& value) {
        auto it = index->find(value);
        if (it == index->end()) {
            it = index
                     ->emplace(
                         value,
                         std::make_unique<Postings>(
                             std::holds_alternative<std::monostate>(value)))
                     .first;
        }
        return *it->second;
    }

    void
    MaybeCacheLatest(Postings& postings, size_t row_count, uint64_t since) {
        const auto words = (row_count + 63) / 64;
        const auto bytes = words * sizeof(uint64_t);
        // POC admission policy: bound dense bitmap memory and avoid paying a
        // whole-column bitmap for sparse values. NULL already has validity.
        if (postings.is_null || postings.latest.size() ||
            postings.size < std::max(kMinLatestBitmapPostings, words) ||
            bytes > kLatestBitmapBudgetPerField - latest_posting_bytes)
            return;
        TargetBitmap bitmap(row_count, false);
        for (const auto& block : postings.blocks) {
            for (size_t slot = 0; slot < block->size; ++slot) {
                if (block->last_begin[slot] != kInfinity &&
                    block->last_end[slot] == kInfinity)
                    bitmap.set(block->offsets[slot]);
            }
        }
        // Only published intervals were read; pending reserved slots are empty.
        // A failed Apply may retain this valid optimization of the old prefix.
        postings.latest = std::move(bitmap);
        postings.latest_since = since;
        latest_posting_bytes += bytes;
        ++latest_posting_count;
    }
};

void
Collect(const Postings& postings, uint64_t query_ts, TargetBitmap& result) {
    for (const auto& block_ptr : postings.blocks) {
        const auto& block = *block_ptr;
        static const auto kernel = posting::SelectKernel();
        for (size_t first = 0; first < block.size; first += 8) {
            const auto count = std::min(size_t{8}, block.size - first);
            auto masks = count == 8
                             ? kernel(block.last_begin.get() + first,
                                      block.last_end.get() + first,
                                      query_ts)
                             : posting::Scalar(block.last_begin.get() + first,
                                               block.last_end.get() + first,
                                               query_ts,
                                               count);
            // Only old snapshots touch histories. Ended latest intervals
            // cannot hide an earlier version visible at the same timestamp.
            while (masks.history) {
                const auto lane = __builtin_ctz(unsigned(masks.history));
                if (Visible(block.histories[first + lane], query_ts))
                    masks.visible |= uint8_t(1u << lane);
                masks.history &= masks.history - 1;
            }
            while (masks.visible) {
                const auto lane = __builtin_ctz(unsigned(masks.visible));
                result.set(block.offsets[first + lane]);
                masks.visible &= masks.visible - 1;
            }
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

    void
    CollectPostings(const Postings& postings,
                    uint64_t query_ts,
                    TargetBitmap& result) const {
        if (postings.latest.size() && query_ts >= postings.latest_since) {
            if (result.size())
                result |= postings.latest;
            else
                result = postings.latest.clone();
            latest_posting_reads.fetch_add(1, std::memory_order_relaxed);
        } else {
            if (!result.size())
                result = TargetBitmap(row_count, false);
            index_postings.fetch_add(postings.size, std::memory_order_relaxed);
            Collect(postings, query_ts, result);
        }
    }

    const size_t row_count;
    const bool enable_temp_index;
    mutable std::shared_mutex mutex;
    std::unordered_map<int64_t, std::unique_ptr<Field>> fields;
    std::optional<uint64_t> last_mutation_ts;
    std::atomic<uint64_t> index_reads{0}, value_reads{0};
    std::atomic<uint64_t> value_rows{0}, base_rows{0}, correction_rows{0};
    std::atomic<uint64_t> old_index_reads{0};
    mutable std::atomic<uint64_t> index_postings{0}, latest_posting_reads{0};
    std::atomic<uint64_t> all_valid_reads{0}, latest_validity_reads{0},
        history_validity_reads{0};
};

UpdatesRecord::UpdatesRecord(size_t row_count, bool enable_temp_index)
    : impl_(std::make_shared<Impl>(row_count, enable_temp_index)) {
}

UpdatesRecord::UpdatesRecord(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {
}

UpdatesRecord::~UpdatesRecord() = default;
void
UpdatesRecord::RecordRead(bool index) const {
    (index ? impl_->index_reads : impl_->value_reads)
        .fetch_add(1, std::memory_order_relaxed);
}
std::pair<uint64_t, uint64_t>
UpdatesRecord::ReadCounts() const {
    return {impl_->index_reads.load(std::memory_order_relaxed),
            impl_->value_reads.load(std::memory_order_relaxed)};
}

bool
UpdatesRecord::HasField(int64_t field_id) const {
    std::shared_lock lock(impl_->mutex);
    return impl_->fields.count(field_id) != 0;
}

size_t
UpdatesRecord::RowCount() const {
    return impl_->row_count;
}

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
UpdatesRecord::RegisterField(int64_t field_id,
                             UpdateFieldType type,
                             bool nullable,
                             BaseReader base_reader) {
    std::unique_lock lock(impl_->mutex);
    Require(!impl_->last_mutation_ts && !impl_->fields.count(field_id),
            "POC field registration must precede updates");
    Require(bool(base_reader), "Missing POC base reader");
    auto field = std::make_unique<Field>();
    field->type = type;
    field->nullable = nullable;
    field->base_reader = std::move(base_reader);
    if (impl_->enable_temp_index) {
        field->index = std::make_unique<AllDataVersionedIndex>();
        field->current_postings.reserve(impl_->row_count);
        if (nullable)
            field->latest_validity = TargetBitmap(impl_->row_count, true);
    }
    impl_->fields.emplace(field_id, std::move(field));
}

void
UpdatesRecord::IndexBaseBatch(int64_t field_id,
                              int64_t start,
                              const std::vector<UpdateValue>& values) {
    std::unique_lock lock(impl_->mutex);
    auto& field = impl_->IndexedField(field_id);
    Require(!impl_->last_mutation_ts && start == field.base_index_rows &&
                start + values.size() <= impl_->row_count,
            "POC index base batches must be contiguous and precede updates");
    for (size_t i = 0; i < values.size(); ++i) {
        field.Validate(values[i]);
        auto& postings = (*field.index)[values[i]];
        if (!postings)
            postings = std::make_unique<Postings>(
                std::holds_alternative<std::monostate>(values[i]));
        auto position = postings->AppendSlot(start + i);
        position.block->last_begin[position.slot] = 0;
        position.block->last_end[position.slot] = kInfinity;
        field.current_postings.push_back(position);
        if (field.nullable)
            field.latest_validity.set(
                start + i, !std::holds_alternative<std::monostate>(values[i]));
    }
    field.base_index_rows += values.size();
}

void
UpdatesRecord::VisitUpdates(
    int64_t field_id,
    uint64_t query_ts,
    const std::function<void(int64_t, const UpdateValue&)>& visit) const {
    std::shared_lock lock(impl_->mutex);
    uint64_t count = 0;
    impl_->GetField(field_id).offsets.ForEach(
        [&](int64_t offset, const History& history) {
            if (auto* version = Visible(history, query_ts)) {
                visit(offset, *version->value);
                ++count;
            }
        });
    impl_->correction_rows.fetch_add(count, std::memory_order_relaxed);
}

void
UpdatesRecord::CorrectEquality(int64_t field_id,
                               uint64_t query_ts,
                               const UpdateValue& wanted,
                               bool equal,
                               TargetBitmap& result,
                               TargetBitmap* validity) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->GetField(field_id);
    uint64_t count = 0;
    const auto correct = [&]<typename T>(const T& needle) {
        field.offsets.ForEach([&](int64_t offset, const History& history) {
            if (const auto* version = Visible(history, query_ts)) {
                const auto* value = std::get_if<T>(version->value.get());
                result[offset] = value && ((*value == needle) == equal);
                if (validity) {
                    (*validity)[offset] = value != nullptr;
                }
                ++count;
            }
        });
    };
    if (field.type == UpdateFieldType::Int64) {
        correct(std::get<int64_t>(wanted));
    } else {
        correct(std::get<std::string>(wanted));
    }
    impl_->correction_rows.fetch_add(count, std::memory_order_relaxed);
}

UpdatesRecord::Stats
UpdatesRecord::GetStats() const {
    std::shared_lock lock(impl_->mutex);
    Stats stats;
    for (const auto& [id, field] : impl_->fields) {
        stats.updated_cells += field->updated_cells;
        stats.update_versions += field->versions.size();
        stats.base_index_rows += field->base_index_rows;
        stats.latest_validity_bytes += field->latest_validity.size_in_bytes();
        stats.latest_posting_bitmap_bytes += field->latest_posting_bytes;
        stats.latest_posting_bitmap_count += field->latest_posting_count;
    }
    stats.value_rows = impl_->value_rows.load();
    stats.base_rows = impl_->base_rows.load();
    stats.correction_rows = impl_->correction_rows.load();
    stats.old_index_reads = impl_->old_index_reads.load();
    stats.index_postings = impl_->index_postings.load();
    stats.latest_posting_reads = impl_->latest_posting_reads.load();
    stats.all_valid_reads = impl_->all_valid_reads.load();
    stats.latest_validity_reads = impl_->latest_validity_reads.load();
    stats.history_validity_reads = impl_->history_validity_reads.load();
    return stats;
}
void
UpdatesRecord::RecordOldIndexRead() const {
    impl_->old_index_reads.fetch_add(1, std::memory_order_relaxed);
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
        auto& field = impl_->GetField(assignment.field_id);
        field.Validate(assignment.value);
        Require(!field.index || field.base_index_rows == impl_->row_count,
                "POC base index must be complete before updates");
    }
    auto offsets = resolve_rows(primary_keys, mutation_ts);
    SortUnique(offsets);
    for (auto offset : offsets) {
        impl_->ValidateOffset(offset);
        for (const auto& assignment : assignments) {
            const auto& history =
                impl_->GetField(assignment.field_id).offsets[offset];
            Require(history.empty() || history.back()->begin_ts < mutation_ts,
                    "POC cell timestamps must be strictly increasing");
        }
    }

    struct Pending {
        Field* field;
        History* history;
        History* index_history;
        Version* version;
        PostingBlock* block;
        size_t slot;
        uint64_t* previous_end;
        History* base_history;
        Version* base_interval;
        int64_t offset;
        Postings* previous_postings;
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
                PostingBlock* block = nullptr;
                size_t slot = 0;
                uint64_t* previous_end = nullptr;
                History* base_history = nullptr;
                Version* base_interval = nullptr;
                Postings* previous_postings = nullptr;
                if (field.index) {
                    auto previous = field.current_postings[offset];
                    previous_postings = previous.block->owner;
                    previous_end = &previous.block->last_end[previous.slot];
                    if (history.empty()) {
                        // Register the base slot before finding the destination:
                        // SET to the same value must reuse that slot.
                        previous.block->owner->slots.emplace(offset, previous);
                        base_history =
                            &previous.block->histories[previous.slot];
                        base_history->reserve(base_history->size() + 1);
                    }
                    auto position =
                        field.IndexPostings(*value).EnsureSlot(offset);
                    block = position.block;
                    slot = position.slot;
                    index_history = &block->histories[slot];
                    index_history->reserve(
                        index_history->size() +
                        (base_history == index_history ? 2 : 1));
                    const auto since = impl_->last_mutation_ts.value_or(0);
                    field.MaybeCacheLatest(
                        *previous_postings, impl_->row_count, since);
                    field.MaybeCacheLatest(
                        *block->owner, impl_->row_count, since);
                }
                field.versions.push_back({value, mutation_ts, kInfinity});
                if (base_history) {
                    try {
                        field.base_intervals.push_back(
                            {nullptr, 0, mutation_ts});
                        base_interval = &field.base_intervals.back();
                    } catch (...) {
                        field.versions.pop_back();
                        throw;
                    }
                }
                pending.push_back({&field,
                                   &history,
                                   index_history,
                                   &field.versions.back(),
                                   block,
                                   slot,
                                   previous_end,
                                   base_history,
                                   base_interval,
                                   offset,
                                   previous_postings});
            }
        }
    } catch (...) {
        // No history refs or closed intervals were published yet. Empty posting
        // groups left by failed allocation contain no visible version.
        for (auto it = pending.rbegin(); it != pending.rend(); ++it) {
            if (it->base_interval)
                it->field->base_intervals.pop_back();
            it->field->versions.pop_back();
        }
        throw;
    }

    // All allocations finished. Both ref paths are installed under the same
    // write lock before a reader can observe the newly closed/open intervals.
    for (auto& item : pending) {
        if (!item.history->empty()) {
            item.history->back()->end_ts = mutation_ts;
        } else
            ++item.field->updated_cells;
        if (item.previous_end)
            *item.previous_end = mutation_ts;
        if (item.base_history)
            item.base_history->push_back(item.base_interval);
        item.history->push_back(item.version);
        if (item.index_history) {
            item.index_history->push_back(item.version);
            item.block->last_begin[item.slot] = mutation_ts;
            item.block->last_end[item.slot] = kInfinity;
            item.field->current_postings[item.offset] = {item.block, item.slot};
            auto& destination = *item.block->owner;
            if (item.previous_postings != &destination) {
                if (item.previous_postings->latest.size()) {
                    item.previous_postings->latest.set(item.offset, false);
                    item.previous_postings->latest_since = mutation_ts;
                }
                if (destination.latest.size()) {
                    destination.latest.set(item.offset);
                    destination.latest_since = mutation_ts;
                }
            }
            if (item.field->nullable) {
                const bool valid = !std::holds_alternative<std::monostate>(
                    *item.version->value);
                auto& bitmap = item.field->latest_validity;
                if (bitmap[item.offset] != valid) {
                    bitmap.set(item.offset, valid);
                    item.field->latest_validity_since = mutation_ts;
                }
            }
        }
    }
    impl_->last_mutation_ts = mutation_ts;
    return offsets.size();
}

std::shared_ptr<const UpdateValue>
UpdatesRecord::Lookup(int64_t field_id,
                      int64_t offset,
                      uint64_t query_ts) const {
    return Gather(field_id, {offset}, query_ts).front();
}

UpdatesRecord::Values
UpdatesRecord::Gather(int64_t field_id,
                      const std::vector<int64_t>& offsets,
                      uint64_t query_ts) const {
    Values result(offsets.size());
    std::vector<int64_t> missing;
    std::vector<size_t> positions;
    BaseReader base_reader;
    {
        std::shared_lock lock(impl_->mutex);
        const auto& field = impl_->GetField(field_id);
        base_reader = field.base_reader;
        for (size_t i = 0; i < offsets.size(); ++i) {
            impl_->ValidateOffset(offsets[i]);
            const auto* history = field.offsets.Find(offsets[i]);
            const auto* version =
                history ? Visible(*history, query_ts) : nullptr;
            if (version)
                result[i] = version->value;
            else {
                missing.push_back(offsets[i]);
                positions.push_back(i);
            }
        }
    }
    if (!missing.empty()) {
        auto base = base_reader(missing);
        Require(base.size() == missing.size(),
                "POC base reader returned wrong row count");
        for (size_t i = 0; i < base.size(); ++i)
            result[positions[i]] = std::move(base[i]);
    }
    impl_->value_rows.fetch_add(offsets.size(), std::memory_order_relaxed);
    impl_->base_rows.fetch_add(missing.size(), std::memory_order_relaxed);
    return result;
}

UpdatesRecord::Values
UpdatesRecord::Scan(int64_t field_id,
                    int64_t begin_offset,
                    size_t count,
                    uint64_t query_ts) const {
    Require(begin_offset >= 0 &&
                static_cast<size_t>(begin_offset) <= impl_->row_count &&
                count <= impl_->row_count - begin_offset,
            "Invalid POC scan range");
    std::vector<int64_t> offsets(count);
    std::iota(offsets.begin(), offsets.end(), begin_offset);
    return Gather(field_id, offsets, query_ts);
}

TargetBitmap
UpdatesRecord::InBitmap(int64_t field_id,
                        const std::vector<UpdateValue>& values,
                        uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->IndexedField(field_id);
    TargetBitmap result;
    for (const auto& value : values) {
        // NULL in an IN list never contributes a TRUE result.
        if (std::holds_alternative<std::monostate>(value)) {
            continue;
        }
        field.Validate(value);
        auto it = field.index->find(value);
        if (it != field.index->end()) {
            impl_->CollectPostings(*it->second, query_ts, result);
        }
    }
    if (!result.size())
        result = TargetBitmap(impl_->row_count, false);
    return result;
}

TargetBitmap
UpdatesRecord::RangeBitmap(int64_t field_id,
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
        return TargetBitmap(impl_->row_count, false);
    }
    const auto& tree = *field.index;
    auto begin = !lower ? tree.upper_bound(UpdateValue{std::monostate{}})
                 : lower->inclusive ? tree.lower_bound(lower->value)
                                    : tree.upper_bound(lower->value);
    auto end = !upper             ? tree.end()
               : upper->inclusive ? tree.upper_bound(upper->value)
                                  : tree.lower_bound(upper->value);
    TargetBitmap result;
    for (auto it = begin; it != end; ++it) {
        impl_->CollectPostings(*it->second, query_ts, result);
    }
    if (!result.size())
        result = TargetBitmap(impl_->row_count, false);
    return result;
}

TargetBitmap
UpdatesRecord::PrefixBitmap(int64_t field_id,
                            const std::string& prefix,
                            uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->IndexedField(field_id);
    Require(field.type == UpdateFieldType::VarChar,
            "POC prefix lookup requires a VARCHAR field");
    TargetBitmap result;
    const auto& tree = *field.index;
    for (auto it = tree.lower_bound(UpdateValue{prefix}); it != tree.end();
         ++it) {
        const auto& value = std::get<std::string>(it->first);
        if (value.compare(0, prefix.size(), prefix) != 0) {
            break;
        }
        impl_->CollectPostings(*it->second, query_ts, result);
    }
    if (!result.size())
        result = TargetBitmap(impl_->row_count, false);
    return result;
}

TargetBitmap
UpdatesRecord::NullBitmap(int64_t field_id, uint64_t query_ts) const {
    auto result = ValidityBitmap(field_id, query_ts);
    result.flip();
    return result;
}

TargetBitmap
UpdatesRecord::ValidityBitmap(int64_t field_id, uint64_t query_ts) const {
    std::shared_lock lock(impl_->mutex);
    const auto& field = impl_->IndexedField(field_id);
    if (!field.nullable) {
        impl_->all_valid_reads.fetch_add(1, std::memory_order_relaxed);
        return TargetBitmap(impl_->row_count, true);
    }
    if (field.base_index_rows == impl_->row_count &&
        query_ts >= field.latest_validity_since) {
        // The caller's existing apply-frontier contract covers query_ts. Later
        // non-NULL-to-non-NULL updates do not shorten this bitmap's interval.
        impl_->latest_validity_reads.fetch_add(1, std::memory_order_relaxed);
        return field.latest_validity.clone();
    }
    impl_->history_validity_reads.fetch_add(1, std::memory_order_relaxed);
    const auto& tree = *field.index;
    TargetBitmap result(impl_->row_count, false);
    auto it = tree.find(UpdateValue{std::monostate{}});
    if (it != tree.end()) {
        impl_->index_postings.fetch_add(it->second->size,
                                        std::memory_order_relaxed);
        Collect(*it->second, query_ts, result);
    }
    result.flip();
    return result;
}

namespace {
std::vector<int64_t>
BitmapOffsets(const TargetBitmap& bitmap) {
    std::vector<int64_t> result;
    for (auto offset = bitmap.find_first(); offset;
         offset = bitmap.find_next(*offset)) {
        result.push_back(*offset);
    }
    return result;
}
}  // namespace

std::vector<int64_t>
UpdatesRecord::In(int64_t field_id,
                  const std::vector<UpdateValue>& values,
                  uint64_t query_ts) const {
    return BitmapOffsets(InBitmap(field_id, values, query_ts));
}

std::vector<int64_t>
UpdatesRecord::Range(int64_t field_id,
                     const std::optional<UpdateRangeBound>& lower,
                     const std::optional<UpdateRangeBound>& upper,
                     uint64_t query_ts) const {
    return BitmapOffsets(RangeBitmap(field_id, lower, upper, query_ts));
}

std::vector<int64_t>
UpdatesRecord::Prefix(int64_t field_id,
                      const std::string& prefix,
                      uint64_t query_ts) const {
    return BitmapOffsets(PrefixBitmap(field_id, prefix, query_ts));
}

std::vector<int64_t>
UpdatesRecord::IsNull(int64_t field_id, uint64_t query_ts) const {
    return BitmapOffsets(NullBitmap(field_id, query_ts));
}

}  // namespace milvus::segcore::experimental
