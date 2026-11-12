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

#include "segcore/UpdatesRecord.h"

namespace milvus {
struct OpContext;
}

namespace milvus::segcore {
class ChunkedSegmentSealedImpl;
}

namespace milvus::segcore::experimental {

// Preparation adapter for the static, sealed-segment POC. Pins the original
// physical base/index state, registers sparse histories and optionally builds
// the all-data index. OFF does not copy base column values.
// Expression routing is activated explicitly after preparation.
class SealedUpdatesRecord {
 public:
    static std::unique_ptr<SealedUpdatesRecord>
    Build(std::shared_ptr<ChunkedSegmentSealedImpl> source,
          const std::vector<int64_t>& field_ids,
          bool enable_temp_index,
          OpContext* op_ctx = nullptr);

    ~SealedUpdatesRecord();

    // Uses the segment's PK index and effective insertion timestamps, then
    // excludes rows deleted at mutation_ts. No second PK map is constructed.
    size_t
    Apply(const std::vector<UpdateValue>& primary_keys,
          uint64_t mutation_ts,
          const std::vector<UpdateAssignment>& assignments,
          OpContext* op_ctx = nullptr);

    // Call before starting queries to enable versioned filtering and output fill.
    void
    EnableExprEvaluation();

    uint64_t
    LatestInsertionTimestamp() const;

    const UpdatesRecord&
    Records() const {
        return *records_;
    }

 private:
    explicit SealedUpdatesRecord(
        std::shared_ptr<ChunkedSegmentSealedImpl> source);

    // The pinned read facade shares delete state with source; source must
    // outlive the facade. Reverse member destruction preserves that order.
    std::shared_ptr<ChunkedSegmentSealedImpl> source_;
    std::shared_ptr<ChunkedSegmentSealedImpl> read_state_;
    std::shared_ptr<UpdatesRecord> records_;
    std::vector<uint64_t> insertion_timestamps_;
};

}  // namespace milvus::segcore::experimental
