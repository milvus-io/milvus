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

#include <chrono>
#include <utility>

namespace milvus::exec {

// Attribute waits to the synchronous consumer, including lazy preparation when
// prefetch is disabled. Background workers and driver cleanup have no collector.
class PrefetchWaitStats {
 public:
    PrefetchWaitStats() : previous_(std::exchange(current_, this)) {
    }

    ~PrefetchWaitStats() {
        current_ = previous_;
    }

    PrefetchWaitStats(const PrefetchWaitStats&) = delete;
    PrefetchWaitStats&
    operator=(const PrefetchWaitStats&) = delete;

    double
    ElapsedMicros() const {
        return std::chrono::duration<double, std::micro>(elapsed_).count();
    }

 private:
    friend class PrefetchWaitTimer;
    using Clock = std::chrono::steady_clock;
    inline static thread_local PrefetchWaitStats* current_ = nullptr;
    PrefetchWaitStats* previous_;
    Clock::duration elapsed_{};
};

class PrefetchWaitTimer {
 public:
    // Suspend the collector so a wrapper and its child do not double-count
    // the same wait. No clock reads are needed outside a consumer's scope.
    PrefetchWaitTimer()
        : stats_(std::exchange(PrefetchWaitStats::current_, nullptr)),
          start_(stats_ ? Clock::now() : Clock::time_point{}) {
    }

    ~PrefetchWaitTimer() {
        if (stats_) {
            stats_->elapsed_ += Clock::now() - start_;
            PrefetchWaitStats::current_ = stats_;
        }
    }

    PrefetchWaitTimer(const PrefetchWaitTimer&) = delete;
    PrefetchWaitTimer&
    operator=(const PrefetchWaitTimer&) = delete;

 private:
    using Clock = std::chrono::steady_clock;
    PrefetchWaitStats* stats_;
    Clock::time_point start_;
};

}  // namespace milvus::exec
