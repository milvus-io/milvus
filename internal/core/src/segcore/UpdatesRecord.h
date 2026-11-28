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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include "common/Types.h"

namespace milvus::segcore::experimental {

using UpdateValue = std::variant<std::monostate, int64_t, std::string>;
using UpdateValueView = std::variant<std::monostate, int64_t, std::string_view>;
enum class UpdateFieldType { Int64, VarChar };
struct UpdateAssignment {
    int64_t field_id;
    UpdateValue value;  // monostate is SET NULL, not an absent update.
};
class ValueReader;
namespace detail {
struct UpdateReadBinding;
}

// Query-side phase 1: one typed, TS-ordered log per field. The sparse offset
// directory contains log positions only; it never duplicates value payloads.
// Base values remain in the pinned segment reader. No GC or value index.
class UpdatesRecord {
 public:
    using Values = std::vector<std::shared_ptr<const UpdateValue>>;
    using BaseReader = std::function<Values(const std::vector<int64_t>&)>;
    using ResolveRows = std::function<std::vector<int64_t>(
        const std::vector<UpdateValue>&, uint64_t)>;
    explicit UpdatesRecord(size_t row_count);
    ~UpdatesRecord();
    UpdatesRecord(const UpdatesRecord&) = delete;
    UpdatesRecord&
    operator=(const UpdatesRecord&) = delete;
    void
    RegisterField(int64_t field_id,
                  UpdateFieldType type,
                  bool nullable,
                  BaseReader base_reader);
    bool
    HasField(int64_t field_id) const;
    size_t
    RowCount() const;

    // Nondecreasing timestamps across calls; strictly increasing per cell.
    // PK resolution must exclude rows not yet inserted or already deleted.
    // All validation and allocation finish before publishing the mutation.
    size_t
    Apply(const std::vector<UpdateValue>& primary_keys,
          uint64_t mutation_ts,
          const std::vector<UpdateAssignment>& assignments,
          const ResolveRows& resolve_rows);

    // Reverse-scan the visible prefix, selecting the latest value per offset.
    // Views are borrowed under the read lock and must not escape the callback.
    void
    VisitUpdates(int64_t field_id,
                 uint64_t query_ts,
                 const std::function<void(int64_t, const UpdateValueView&)>&
                     visit) const;
    void
    CorrectEquality(int64_t field_id,
                    uint64_t query_ts,
                    const UpdateValue& wanted,
                    bool equal,
                    TargetBitmap& result,
                    TargetBitmap* validity) const;

    // Directory lookup: last first, otherwise binary-search this offset's
    // ordered log positions. Absence falls back to base; NULL does not.
    // Returned values own their storage, surviving writes and store teardown.
    Values
    Gather(int64_t field_id,
           const std::vector<int64_t>& offsets,
           uint64_t query_ts) const;
    std::shared_ptr<const UpdateValue>
    Lookup(int64_t field_id, int64_t offset, uint64_t query_ts) const;
    Values
    Scan(int64_t field_id,
         int64_t begin_offset,
         size_t count,
         uint64_t query_ts) const;
    struct RangeUpdate {
        size_t position;  // Relative to the requested batch.
        std::shared_ptr<const UpdateValue> value;
    };
    // Only updated candidate offsets are looked up. Never reads base data.
    std::vector<RangeUpdate>
    ReadRangeUpdates(int64_t field_id,
                     uint64_t query_ts,
                     int64_t begin_offset,
                     size_t count,
                     const TargetBitmapView* candidates = nullptr,
                     size_t candidate_offset = 0) const;

    struct Stats {
        uint64_t updated_cells = 0, update_versions = 0, directory_refs = 0;
        uint64_t value_rows = 0, base_rows = 0, correction_rows = 0;
        uint64_t old_index_reads = 0, value_reads = 0;
        uint64_t ts_entries_visited = 0, ts_selected_rows = 0;
        uint64_t ts_unique_reads = 0, ts_dedup_reads = 0;
        uint64_t ts_visited_bitmap_bytes = 0;
        uint64_t lookup_latest = 0, lookup_history = 0;
        uint64_t overlay_batches = 0, overlay_history_rows = 0,
                 overlay_rows = 0;
        uint64_t updated_offset_bitmap_bytes = 0;
    };
    Stats
    GetStats() const;
    void
    RecordOldIndexRead() const;
    void
    RecordRead() const;

    // Caller admits only snapshots covered by its complete applied prefix.
    // Both predicate correction and output lookup share this Q_TS and store.
    std::unique_ptr<ValueReader>
    CreateReader(int64_t field_id, uint64_t query_ts) const;

 private:
    detail::UpdateReadBinding
    CaptureReadBinding(int64_t field_id, uint64_t query_ts) const;
    class Impl;
    explicit UpdatesRecord(std::shared_ptr<Impl> impl);
    std::shared_ptr<Impl> impl_;
};

namespace detail {
struct UpdateReadBinding {
    std::shared_ptr<const UpdatesRecord> records;
    int64_t field_id;
    uint64_t query_ts;
    int64_t row_count;
    UpdateFieldType type;
};
}  // namespace detail
}  // namespace milvus::segcore::experimental
