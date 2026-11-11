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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace milvus::segcore::experimental {

// Query-side POC version storage; snapshot-bound readers are defined separately.
// monostate denotes SQL NULL; it is distinct from an absent visible version.
using UpdateValue = std::variant<std::monostate, int64_t, std::string>;

enum class UpdateFieldType { Int64, VarChar };

struct UpdateReaders;
namespace detail {
struct UpdateReadBinding;
}

struct UpdateAssignment {
    int64_t field_id;
    UpdateValue value;
};

struct UpdateRangeBound {
    UpdateValue value;
    bool inclusive = true;
};

// One instance belongs to one physical segment. Each initialized field contains
// ALL rows, including their original values. The offset directory and optional
// all-data B-tree share version records. No version GC is performed in this POC.
//
// Methods serialize mutation against readers. Callers must still supply one
// fixed query timestamp across filter/output calls and publish QueryView progress
// only after Apply succeeds. Segment liveness/row visibility remains the caller's
// responsibility; index results contain field-value matches, not a delete mask.
class UpdatesRecord {
 public:
    // The segment adapter resolves PKs to rows eligible for this mutation:
    // insertion timestamp < mutation timestamp, and not already deleted.
    // A PK may resolve to several offsets. Duplicate offsets are applied once.
    // The adapter must not call back into this UpdatesRecord.
    using ResolveRows = std::function<std::vector<int64_t>(
        const std::vector<UpdateValue>&, uint64_t)>;

    explicit UpdatesRecord(size_t row_count, bool enable_temp_index);
    ~UpdatesRecord();
    UpdatesRecord(const UpdatesRecord&) = delete;
    UpdatesRecord&
    operator=(const UpdatesRecord&) = delete;

    // Offsets are [0, row_count). begin_timestamps describe the loaded values'
    // visibility, not load time. Initialization completes before the first Apply.
    // Field type is explicit so an all-NULL column is unambiguous.
    void
    InitializeField(int64_t field_id,
                    UpdateFieldType type,
                    bool nullable,
                    const std::vector<UpdateValue>& values,
                    const std::vector<uint64_t>& begin_timestamps);

    // Apply one ordered PK mutation to all assignments. Timestamps must be
    // nondecreasing across calls and strictly increasing for each affected cell.
    // Same-timestamp fragments may touch disjoint cells. Replay deduplication
    // belongs to the caller. Returns the number of distinct affected rows.
    // Validation/allocation failure leaves visible versions unchanged.
    size_t
    Apply(const std::vector<UpdateValue>& primary_keys,
          uint64_t mutation_ts,
          const std::vector<UpdateAssignment>& assignments,
          const ResolveRows& resolve_rows);

    // An owning immutable value survives subsequent updates and store teardown.
    // nullptr means no version visible at query_ts; monostate means SET NULL.
    std::shared_ptr<const UpdateValue>
    Lookup(int64_t field_id, int64_t offset, uint64_t query_ts) const;

    // One read lock and one timestamp for the entire batch. Results preserve
    // input order (including duplicate offsets), with the same NULL/absence
    // distinction as Lookup. Values retain ownership without copying strings.
    std::vector<std::shared_ptr<const UpdateValue>>
    Gather(int64_t field_id,
           const std::vector<int64_t>& offsets,
           uint64_t query_ts) const;

    // Contiguous batch for a raw evaluator, without building an offsets array.
    std::vector<std::shared_ptr<const UpdateValue>>
    Scan(int64_t field_id,
         int64_t begin_offset,
         size_t count,
         uint64_t query_ts) const;

    // Exact, sorted unique offset results. Ordinary predicates exclude NULL.
    // A future expression adapter supplies SQL validity and segment visibility.
    // Calling an index operation with enable_temp_index=false is an error,
    // rather than silently measuring a scan as an index lookup.
    std::vector<int64_t>
    In(int64_t field_id,
       const std::vector<UpdateValue>& values,
       uint64_t query_ts) const;

    std::vector<int64_t>
    Range(int64_t field_id,
          const std::optional<UpdateRangeBound>& lower,
          const std::optional<UpdateRangeBound>& upper,
          uint64_t query_ts) const;

    // Literal byte prefix, without LIKE wildcards or collation conversion.
    std::vector<int64_t>
    Prefix(int64_t field_id,
           const std::string& prefix,
           uint64_t query_ts) const;

    std::vector<int64_t>
    IsNull(int64_t field_id, uint64_t query_ts) const;

    std::vector<int64_t>
    IsNotNull(int64_t field_id, uint64_t query_ts) const;

    // Creates both access paths with one field/timestamp binding. The caller
    // must have completed the input prefix through query_ts before opening it;
    // binding does not advance or replace the QueryView apply frontier.
    // Include UpdatesRecordReaders.h to use the returned reader pair.
    UpdateReaders
    CreateReaders(int64_t field_id, uint64_t query_ts) const;

 private:
    detail::UpdateReadBinding
    CaptureReadBinding(int64_t field_id, uint64_t query_ts) const;

    class Impl;
    explicit UpdatesRecord(std::shared_ptr<Impl> impl);
    std::shared_ptr<Impl> impl_;
};

namespace detail {
// Shared by the two readers; only UpdatesRecord creates a binding. The retained
// facade owns the same Impl as the writer, not a copy of the version records.
struct UpdateReadBinding {
    std::shared_ptr<const UpdatesRecord> records;
    int64_t field_id;
    uint64_t query_ts;
    int64_t row_count;
    UpdateFieldType type;
    bool has_index;
};
}  // namespace detail

}  // namespace milvus::segcore::experimental
