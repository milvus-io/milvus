// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <chrono>
#include <cstddef>

namespace milvus::monitor {

// Fixed instrumentation sites only. Never use request, field or file IDs as labels.
enum class QueryStage {
    ManifestReaderOpen,
    ManifestTranslator,
    ManifestCacheSlot,
    ManifestGroupWait,
    FieldPrefetchPrepare,
    FieldPrefetchLoad,
    VectorPrefetchQueue,
    VectorPrefetchRun,
    VectorPrefetchWait,
    MvccPrefetchQueue,
    MvccPrefetchRun,
    MvccPrefetchWait,
    ManifestLoadCells,
    ManifestReadBatch,
    ManifestBuildChunk,
    LoadBatchBudgetWait,
    LoadBatchQueue,
    SearchPrepare,
    SearchExecute,
    FillPrimaryKeys,
    Count,
};

using QueryStageClock = std::chrono::steady_clock;

void
ObserveQueryStage(QueryStage stage,
                  QueryStageClock::duration elapsed,
                  bool failed = false);

// Wall time, including waits. Nested stages and parallel batches overlap.
// Records exceptional exits as errors and always releases the inflight gauge.
class QueryStageTimer {
 public:
    explicit QueryStageTimer(QueryStage stage);
    ~QueryStageTimer();
    QueryStageTimer(const QueryStageTimer&) = delete;
    QueryStageTimer&
    operator=(const QueryStageTimer&) = delete;

    // Idempotent. Pass failed=true for failures returned without throwing.
    void
    End(bool failed = false);

 private:
    QueryStage stage_;
    QueryStageClock::time_point start_;
    int uncaught_exceptions_;
    bool active_ = true;
};

}  // namespace milvus::monitor
