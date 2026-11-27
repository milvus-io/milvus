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

#include <array>
#include <chrono>
#include <cstddef>

namespace milvus::monitor {

enum class SegmentLoadPhase {
    LockWait,
    Prepare,
    CloneState,
    Indexes,
    ReloadColumns,
    ColumnGroups,
    TextLob,
    FieldData,
    TextIndexes,
    JsonStats,
    DefaultFields,
    CreateTextIndexes,
    Finalize,
    Publish,
    Count,
};

// One histogram observation per phase per Load attempt, including zero for
// skipped phases. Phases partition total wall time and share its final outcome.
// Owned only by the calling Load thread; parallel task metrics are separate.
class SegmentLoadTiming {
 public:
    using Clock = std::chrono::steady_clock;
    SegmentLoadTiming();
    ~SegmentLoadTiming();
    SegmentLoadTiming(const SegmentLoadTiming&) = delete;
    SegmentLoadTiming&
    operator=(const SegmentLoadTiming&) = delete;

    static void
    SwitchTo(SegmentLoadTiming* timing, SegmentLoadPhase phase);
    void
    End(bool failed = false);

 private:
    std::array<Clock::duration,
               static_cast<std::size_t>(SegmentLoadPhase::Count)>
        durations_{};
    Clock::time_point started_;
    Clock::time_point phase_started_;
    SegmentLoadPhase phase_ = SegmentLoadPhase::LockWait;
    int uncaught_exceptions_;
    bool active_ = true;
};

}  // namespace milvus::monitor
