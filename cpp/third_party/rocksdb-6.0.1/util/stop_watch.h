//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/statistics.h"
#include "rocksdb/env.h"

namespace rocksdb {
// Auto-scoped.
// Records the measure time into the corresponding histogram if statistics
// is not nullptr. It is also saved into *elapsed if the pointer is not nullptr
// and overwrite is true, it will be added to *elapsed if overwrite is false.
class StopWatch {
 public:
  StopWatch(Env* const env, Statistics* statistics, const uint32_t hist_type,
            uint64_t* elapsed = nullptr, bool overwrite = true,
            bool delay_enabled = false)
      : env_(env),
        statistics_(statistics),
        hist_type_(hist_type),
        elapsed_(elapsed),
        overwrite_(overwrite),
        stats_enabled_(statistics && statistics->HistEnabledForType(hist_type)),
        delay_enabled_(delay_enabled),
        total_delay_(0),
        delay_start_time_(0),
        start_time_((stats_enabled_ || elapsed != nullptr) ? env->NowMicros()
                                                           : 0) {}

  ~StopWatch() {
    if (elapsed_) {
      if (overwrite_) {
        *elapsed_ = env_->NowMicros() - start_time_;
      } else {
        *elapsed_ += env_->NowMicros() - start_time_;
      }
    }
    if (elapsed_ && delay_enabled_) {
      *elapsed_ -= total_delay_;
    }
    if (stats_enabled_) {
      statistics_->measureTime(hist_type_,
          (elapsed_ != nullptr) ? *elapsed_ :
                                  (env_->NowMicros() - start_time_));
    }
  }

  void DelayStart() {
    // if delay_start_time_ is not 0, it means we are already tracking delay,
    // so delay_start_time_ should not be overwritten
    if (elapsed_ && delay_enabled_ && delay_start_time_ == 0) {
      delay_start_time_ = env_->NowMicros();
    }
  }

  void DelayStop() {
    if (elapsed_ && delay_enabled_ && delay_start_time_ != 0) {
      total_delay_ += env_->NowMicros() - delay_start_time_;
    }
    // reset to 0 means currently no delay is being tracked, so two consecutive
    // calls to DelayStop will not increase total_delay_
    delay_start_time_ = 0;
  }

  uint64_t GetDelay() const { return delay_enabled_ ? total_delay_ : 0; }

  uint64_t start_time() const { return start_time_; }

 private:
  Env* const env_;
  Statistics* statistics_;
  const uint32_t hist_type_;
  uint64_t* elapsed_;
  bool overwrite_;
  bool stats_enabled_;
  bool delay_enabled_;
  uint64_t total_delay_;
  uint64_t delay_start_time_;
  const uint64_t start_time_;
};

// a nano second precision stopwatch
class StopWatchNano {
 public:
  explicit StopWatchNano(Env* const env, bool auto_start = false)
      : env_(env), start_(0) {
    if (auto_start) {
      Start();
    }
  }

  void Start() { start_ = env_->NowNanos(); }

  uint64_t ElapsedNanos(bool reset = false) {
    auto now = env_->NowNanos();
    auto elapsed = now - start_;
    if (reset) {
      start_ = now;
    }
    return elapsed;
  }

  uint64_t ElapsedNanosSafe(bool reset = false) {
    return (env_ != nullptr) ? ElapsedNanos(reset) : 0U;
  }

 private:
  Env* const env_;
  uint64_t start_;
};

} // namespace rocksdb
