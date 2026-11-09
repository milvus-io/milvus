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

#include <chrono>
#include <cstdint>

namespace milvus::index {

// Writer-local cadence only. The serialized writer calls NoteCommitted after
// publication succeeds. Reader coverage belongs exclusively to GrowingIndexSnapshotPin.
class GrowingCommitPolicy {
 public:
    using Clock = std::chrono::steady_clock;

    explicit GrowingCommitPolicy(int64_t commit_interval_in_ms)
        : interval_(commit_interval_in_ms), last_commit_time_(Clock::now()) {
    }

    bool
    ShouldCommit() const {
        return Clock::now() - last_commit_time_ >= interval_;
    }

    void
    NoteCommitted() {
        last_commit_time_ = Clock::now();
    }

 private:
    const std::chrono::milliseconds interval_;
    Clock::time_point last_commit_time_;
};

}  // namespace milvus::index
