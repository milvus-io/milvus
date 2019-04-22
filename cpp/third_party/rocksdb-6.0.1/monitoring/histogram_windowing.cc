//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "monitoring/histogram_windowing.h"
#include "monitoring/histogram.h"
#include "util/cast_util.h"

#include <algorithm>

namespace rocksdb {

HistogramWindowingImpl::HistogramWindowingImpl() {
  env_ = Env::Default();
  window_stats_.reset(new HistogramStat[static_cast<size_t>(num_windows_)]);
  Clear();
}

HistogramWindowingImpl::HistogramWindowingImpl(
    uint64_t num_windows,
    uint64_t micros_per_window,
    uint64_t min_num_per_window) :
      num_windows_(num_windows),
      micros_per_window_(micros_per_window),
      min_num_per_window_(min_num_per_window) {
  env_ = Env::Default();
  window_stats_.reset(new HistogramStat[static_cast<size_t>(num_windows_)]);
  Clear();
}

HistogramWindowingImpl::~HistogramWindowingImpl() {
}

void HistogramWindowingImpl::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);

  stats_.Clear();
  for (size_t i = 0; i < num_windows_; i++) {
    window_stats_[i].Clear();
  }
  current_window_.store(0, std::memory_order_relaxed);
  last_swap_time_.store(env_->NowMicros(), std::memory_order_relaxed);
}

bool HistogramWindowingImpl::Empty() const { return stats_.Empty(); }

// This function is designed to be lock free, as it's in the critical path
// of any operation.
// Each individual value is atomic, it is just that some samples can go
// in the older bucket which is tolerable.
void HistogramWindowingImpl::Add(uint64_t value){
  TimerTick();

  // Parent (global) member update
  stats_.Add(value);

  // Current window update
  window_stats_[static_cast<size_t>(current_window())].Add(value);
}

void HistogramWindowingImpl::Merge(const Histogram& other) {
  if (strcmp(Name(), other.Name()) == 0) {
    Merge(
        *static_cast_with_check<const HistogramWindowingImpl, const Histogram>(
            &other));
  }
}

void HistogramWindowingImpl::Merge(const HistogramWindowingImpl& other) {
  std::lock_guard<std::mutex> lock(mutex_);
  stats_.Merge(other.stats_);

  if (stats_.num_buckets_ != other.stats_.num_buckets_ ||
      micros_per_window_ != other.micros_per_window_) {
    return;
  }

  uint64_t cur_window = current_window();
  uint64_t other_cur_window = other.current_window();
  // going backwards for alignment
  for (unsigned int i = 0;
                    i < std::min(num_windows_, other.num_windows_); i++) {
    uint64_t window_index =
        (cur_window + num_windows_ - i) % num_windows_;
    uint64_t other_window_index =
        (other_cur_window + other.num_windows_ - i) % other.num_windows_;
    size_t windex = static_cast<size_t>(window_index);
    size_t other_windex = static_cast<size_t>(other_window_index);

    window_stats_[windex].Merge(
      other.window_stats_[other_windex]);
  }
}

std::string HistogramWindowingImpl::ToString() const {
  return stats_.ToString();
}

double HistogramWindowingImpl::Median() const {
  return Percentile(50.0);
}

double HistogramWindowingImpl::Percentile(double p) const {
  // Retry 3 times in total
  for (int retry = 0; retry < 3; retry++) {
    uint64_t start_num = stats_.num();
    double result = stats_.Percentile(p);
    // Detect if swap buckets or Clear() was called during calculation
    if (stats_.num() >= start_num) {
      return result;
    }
  }
  return 0.0;
}

double HistogramWindowingImpl::Average() const {
  return stats_.Average();
}

double HistogramWindowingImpl::StandardDeviation() const {
  return stats_.StandardDeviation();
}

void HistogramWindowingImpl::Data(HistogramData * const data) const {
  stats_.Data(data);
}

void HistogramWindowingImpl::TimerTick() {
  uint64_t curr_time = env_->NowMicros();
  size_t curr_window_ = static_cast<size_t>(current_window());
  if (curr_time - last_swap_time() > micros_per_window_ &&
      window_stats_[curr_window_].num() >= min_num_per_window_) {
    SwapHistoryBucket();
  }
}

void HistogramWindowingImpl::SwapHistoryBucket() {
  // Threads executing Add() would be competing for this mutex, the first one
  // who got the metex would take care of the bucket swap, other threads
  // can skip this.
  // If mutex is held by Merge() or Clear(), next Add() will take care of the
  // swap, if needed.
  if (mutex_.try_lock()) {
    last_swap_time_.store(env_->NowMicros(), std::memory_order_relaxed);

    uint64_t curr_window = current_window();
    uint64_t next_window = (curr_window == num_windows_ - 1) ?
                                                    0 : curr_window + 1;

    // subtract next buckets from totals and swap to next buckets
    HistogramStat& stats_to_drop = 
      window_stats_[static_cast<size_t>(next_window)];

    if (!stats_to_drop.Empty()) {
      for (size_t b = 0; b < stats_.num_buckets_; b++){
        stats_.buckets_[b].fetch_sub(
            stats_to_drop.bucket_at(b), std::memory_order_relaxed);
      }

      if (stats_.min() == stats_to_drop.min()) {
        uint64_t new_min = std::numeric_limits<uint64_t>::max();
        for (unsigned int i = 0; i < num_windows_; i++) {
          if (i != next_window) {
            uint64_t m = window_stats_[i].min();
            if (m < new_min) new_min = m;
          }
        }
        stats_.min_.store(new_min, std::memory_order_relaxed);
      }

      if (stats_.max() == stats_to_drop.max()) {
        uint64_t new_max = 0;
        for (unsigned int i = 0; i < num_windows_; i++) {
          if (i != next_window) {
            uint64_t m = window_stats_[i].max();
            if (m > new_max) new_max = m;
          }
        }
        stats_.max_.store(new_max, std::memory_order_relaxed);
      }

      stats_.num_.fetch_sub(stats_to_drop.num(), std::memory_order_relaxed);
      stats_.sum_.fetch_sub(stats_to_drop.sum(), std::memory_order_relaxed);
      stats_.sum_squares_.fetch_sub(
                  stats_to_drop.sum_squares(), std::memory_order_relaxed);

      stats_to_drop.Clear();
    }

    // advance to next window bucket
    current_window_.store(next_window, std::memory_order_relaxed);

    mutex_.unlock();
  }
}

}  // namespace rocksdb
