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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "index/contracts/growing/IGrowingIndex.h"
#include "index/contracts/query/ReaderCaps.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/indexing/FieldIndexCapability.h"

// Segment teardown waits for in-flight calls; acquired pins may outlive this
// set. Schema reopen stages and backfills new owners before RegisterBatch makes
// them visible. Text, configured R-Tree and supported interim-vector owners use
// this same ownership and publication path.
namespace milvus::segcore {

// Threads one growing interim index build/add may use, resolved from
// queryNode.segcore.interimIndex.growingBuildThreadRate (#53030) against the
// live knowhere build thread pool and clamped to [1, pool size]. Exposed so
// the resolution rule is testable without a per-field build-params accessor.
int64_t
ResolveGrowingBuildThreadNum(const SegcoreConfig& segcore_config);

class InsertRecordGrowing;
class VectorBase;

class GrowingIndexSet {
 public:
    struct Appender {
        Appender(FieldId field_id,
                 index::ReaderCaps reader_caps,
                 std::unique_ptr<index::IGrowingIndex> owner,
                 bool source_backed = false);

        index::ReaderCaps caps;
        std::unique_ptr<index::IGrowingIndex> owner;
        // SCANN_DVR refines through the growing column; other connected
        // families can release the column after a covering reader owns raw.
        bool source_backed{false};
        FieldIndexCapability capability;
    };

    using AppenderMap = std::map<FieldId, Appender>;

    void
    Initialize(const Schema& schema,
               const IndexMetaPtr& index_meta,
               const SegcoreConfig& segcore_config,
               int64_t segment_id,
               const InsertRecordGrowing& insert_record);

    // Construct without registering. Reopen backfills and flushes this owner
    // through the segment's stable raw prefix before registering the complete
    // batch; any reader it publishes remains private until registration.
    std::optional<Appender>
    StageAppender(const FieldMeta& field_meta,
                  const IndexMetaPtr& index_meta,
                  const SegcoreConfig& segcore_config,
                  int64_t segment_id,
                  const VectorBase* field_raw_data = nullptr) const;

    // All nodes and rollback bookkeeping are allocated before appenders_ is
    // changed. A failure leaves the shared map unchanged.
    void
    RegisterBatch(AppenderMap staged);

    bool
    Has(FieldId field_id) const;

    // True only when a fixed reader covers the query-visible prefix and can
    // reproduce original values without borrowing the growing column.
    bool
    CanReleaseVectorColumn(FieldId field_id, int64_t visible_row_end) const;

    // The insert path binds the checked IAppendable<Batch> capability from
    // owner using field schema. It must not retain an owning reader pointer.
    template <typename Batch>
    void
    Append(FieldId field_id, int64_t row_begin, const Batch& batch) {
        std::shared_lock lock(mutex_);
        auto it = appenders_.find(field_id);
        if (it == appenders_.end()) {
            return;
        }
        AppendTo(it->second, field_id, row_begin, batch);
    }

    template <typename Batch>
    static void
    AppendTo(Appender& appender,
             FieldId field_id,
             int64_t row_begin,
             const Batch& batch) {
        AssertInfo(appender.owner != nullptr,
                   "growing field {} has no index owner",
                   field_id.get());
        auto* typed =
            dynamic_cast<index::IAppendable<Batch>*>(appender.owner.get());
        AssertInfo(typed != nullptr,
                   "growing field {} does not accept the requested batch",
                   field_id.get());
        typed->Append(row_begin, batch);
    }

    // Load/reopen boundary: an already-built owner returns only after all
    // accepted rows are represented by fixed published records. A vector cold
    // Build failure may return with an empty pin only while complete raw data
    // remains the safe fallback. Built Add failures still propagate.
    void
    FlushAll();

    static void
    Flush(Appender& appender, FieldId field_id);

    // One generation, one coverage boundary; no family-specific read getters.
    index::GrowingIndexSnapshotPin
    PinSnapshot(FieldId field_id) const {
        std::shared_lock lock(mutex_);
        auto it = appenders_.find(field_id);
        if (it == appenders_.end() || !it->second.owner) {
            return {};
        }
        it->second.owner->CommitIfNeeded();
        return it->second.owner->PinSnapshot();
    }

    FieldIndexCapability
    Capability(FieldId field_id) const;

 private:
    mutable std::shared_mutex mutex_;
    AppenderMap appenders_;
};

}  // namespace milvus::segcore
