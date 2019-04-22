//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "monitoring/histogram.h"

#include <inttypes.h>
#include <cassert>
#include <math.h>
#include <stdio.h>

#include "port/port.h"
#include "util/cast_util.h"

namespace rocksdb {

HistogramBucketMapper::HistogramBucketMapper() {
  // If you change this, you also need to change
  // size of array buckets_ in HistogramImpl
  bucketValues_ = {1, 2};
  valueIndexMap_ = {{1, 0}, {2, 1}};
  double bucket_val = static_cast<double>(bucketValues_.back());
  while ((bucket_val = 1.5 * bucket_val) <= static_cast<double>(port::kMaxUint64)) {
    bucketValues_.push_back(static_cast<uint64_t>(bucket_val));
    // Extracts two most significant digits to make histogram buckets more
    // human-readable. E.g., 172 becomes 170.
    uint64_t pow_of_ten = 1;
    while (bucketValues_.back() / 10 > 10) {
      bucketValues_.back() /= 10;
      pow_of_ten *= 10;
    }
    bucketValues_.back() *= pow_of_ten;
    valueIndexMap_[bucketValues_.back()] = bucketValues_.size() - 1;
  }
  maxBucketValue_ = bucketValues_.back();
  minBucketValue_ = bucketValues_.front();
}

size_t HistogramBucketMapper::IndexForValue(const uint64_t value) const {
  if (value >= maxBucketValue_) {
    return bucketValues_.size() - 1;
  } else if ( value >= minBucketValue_ ) {
    std::map<uint64_t, uint64_t>::const_iterator lowerBound =
      valueIndexMap_.lower_bound(value);
    if (lowerBound != valueIndexMap_.end()) {
      return static_cast<size_t>(lowerBound->second);
    } else {
      return 0;
    }
  } else {
    return 0;
  }
}

namespace {
  const HistogramBucketMapper bucketMapper;
}

HistogramStat::HistogramStat()
  : num_buckets_(bucketMapper.BucketCount()) {
  assert(num_buckets_ == sizeof(buckets_) / sizeof(*buckets_));
  Clear();
}

void HistogramStat::Clear() {
  min_.store(bucketMapper.LastValue(), std::memory_order_relaxed);
  max_.store(0, std::memory_order_relaxed);
  num_.store(0, std::memory_order_relaxed);
  sum_.store(0, std::memory_order_relaxed);
  sum_squares_.store(0, std::memory_order_relaxed);
  for (unsigned int b = 0; b < num_buckets_; b++) {
    buckets_[b].store(0, std::memory_order_relaxed);
  }
};

bool HistogramStat::Empty() const { return num() == 0; }

void HistogramStat::Add(uint64_t value) {
  // This function is designed to be lock free, as it's in the critical path
  // of any operation. Each individual value is atomic and the order of updates
  // by concurrent threads is tolerable.
  const size_t index = bucketMapper.IndexForValue(value);
  assert(index < num_buckets_);
  buckets_[index].store(buckets_[index].load(std::memory_order_relaxed) + 1,
                        std::memory_order_relaxed);

  uint64_t old_min = min();
  if (value < old_min) {
    min_.store(value, std::memory_order_relaxed);
  }

  uint64_t old_max = max();
  if (value > old_max) {
    max_.store(value, std::memory_order_relaxed);
  }

  num_.store(num_.load(std::memory_order_relaxed) + 1,
             std::memory_order_relaxed);
  sum_.store(sum_.load(std::memory_order_relaxed) + value,
             std::memory_order_relaxed);
  sum_squares_.store(
      sum_squares_.load(std::memory_order_relaxed) + value * value,
      std::memory_order_relaxed);
}

void HistogramStat::Merge(const HistogramStat& other) {
  // This function needs to be performned with the outer lock acquired
  // However, atomic operation on every member is still need, since Add()
  // requires no lock and value update can still happen concurrently
  uint64_t old_min = min();
  uint64_t other_min = other.min();
  while (other_min < old_min &&
         !min_.compare_exchange_weak(old_min, other_min)) {}

  uint64_t old_max = max();
  uint64_t other_max = other.max();
  while (other_max > old_max &&
         !max_.compare_exchange_weak(old_max, other_max)) {}

  num_.fetch_add(other.num(), std::memory_order_relaxed);
  sum_.fetch_add(other.sum(), std::memory_order_relaxed);
  sum_squares_.fetch_add(other.sum_squares(), std::memory_order_relaxed);
  for (unsigned int b = 0; b < num_buckets_; b++) {
    buckets_[b].fetch_add(other.bucket_at(b), std::memory_order_relaxed);
  }
}

double HistogramStat::Median() const {
  return Percentile(50.0);
}

double HistogramStat::Percentile(double p) const {
  double threshold = num() * (p / 100.0);
  uint64_t cumulative_sum = 0;
  for (unsigned int b = 0; b < num_buckets_; b++) {
    uint64_t bucket_value = bucket_at(b);
    cumulative_sum += bucket_value;
    if (cumulative_sum >= threshold) {
      // Scale linearly within this bucket
      uint64_t left_point = (b == 0) ? 0 : bucketMapper.BucketLimit(b-1);
      uint64_t right_point = bucketMapper.BucketLimit(b);
      uint64_t left_sum = cumulative_sum - bucket_value;
      uint64_t right_sum = cumulative_sum;
      double pos = 0;
      uint64_t right_left_diff = right_sum - left_sum;
      if (right_left_diff != 0) {
       pos = (threshold - left_sum) / right_left_diff;
      }
      double r = left_point + (right_point - left_point) * pos;
      uint64_t cur_min = min();
      uint64_t cur_max = max();
      if (r < cur_min) r = static_cast<double>(cur_min);
      if (r > cur_max) r = static_cast<double>(cur_max);
      return r;
    }
  }
  return static_cast<double>(max());
}

double HistogramStat::Average() const {
  uint64_t cur_num = num();
  uint64_t cur_sum = sum();
  if (cur_num == 0) return 0;
  return static_cast<double>(cur_sum) / static_cast<double>(cur_num);
}

double HistogramStat::StandardDeviation() const {
  uint64_t cur_num = num();
  uint64_t cur_sum = sum();
  uint64_t cur_sum_squares = sum_squares();
  if (cur_num == 0) return 0;
  double variance =
      static_cast<double>(cur_sum_squares * cur_num - cur_sum * cur_sum) /
      static_cast<double>(cur_num * cur_num);
  return sqrt(variance);
}
std::string HistogramStat::ToString() const {
  uint64_t cur_num = num();
  std::string r;
  char buf[1650];
  snprintf(buf, sizeof(buf),
           "Count: %" PRIu64 " Average: %.4f  StdDev: %.2f\n",
           cur_num, Average(), StandardDeviation());
  r.append(buf);
  snprintf(buf, sizeof(buf),
           "Min: %" PRIu64 "  Median: %.4f  Max: %" PRIu64 "\n",
           (cur_num == 0 ? 0 : min()), Median(), (cur_num == 0 ? 0 : max()));
  r.append(buf);
  snprintf(buf, sizeof(buf),
           "Percentiles: "
           "P50: %.2f P75: %.2f P99: %.2f P99.9: %.2f P99.99: %.2f\n",
           Percentile(50), Percentile(75), Percentile(99), Percentile(99.9),
           Percentile(99.99));
  r.append(buf);
  r.append("------------------------------------------------------\n");
  if (cur_num == 0) return r;   // all buckets are empty
  const double mult = 100.0 / cur_num;
  uint64_t cumulative_sum = 0;
  for (unsigned int b = 0; b < num_buckets_; b++) {
    uint64_t bucket_value = bucket_at(b);
    if (bucket_value <= 0.0) continue;
    cumulative_sum += bucket_value;
    snprintf(buf, sizeof(buf),
             "%c %7" PRIu64 ", %7" PRIu64 " ] %8" PRIu64 " %7.3f%% %7.3f%% ",
             (b == 0) ? '[' : '(',
             (b == 0) ? 0 : bucketMapper.BucketLimit(b-1),  // left
              bucketMapper.BucketLimit(b),  // right
              bucket_value,                   // count
             (mult * bucket_value),           // percentage
             (mult * cumulative_sum));       // cumulative percentage
    r.append(buf);

    // Add hash marks based on percentage; 20 marks for 100%.
    size_t marks = static_cast<size_t>(mult * bucket_value / 5 + 0.5);
    r.append(marks, '#');
    r.push_back('\n');
  }
  return r;
}

void HistogramStat::Data(HistogramData * const data) const {
  assert(data);
  data->median = Median();
  data->percentile95 = Percentile(95);
  data->percentile99 = Percentile(99);
  data->max = static_cast<double>(max());
  data->average = Average();
  data->standard_deviation = StandardDeviation();
  data->count = num();
  data->sum = sum();
  data->min = static_cast<double>(min());
}

void HistogramImpl::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  stats_.Clear();
}

bool HistogramImpl::Empty() const {
  return stats_.Empty();
}

void HistogramImpl::Add(uint64_t value) {
  stats_.Add(value);
}

void HistogramImpl::Merge(const Histogram& other) {
  if (strcmp(Name(), other.Name()) == 0) {
    Merge(
        *static_cast_with_check<const HistogramImpl, const Histogram>(&other));
  }
}

void HistogramImpl::Merge(const HistogramImpl& other) {
  std::lock_guard<std::mutex> lock(mutex_);
  stats_.Merge(other.stats_);
}

double HistogramImpl::Median() const {
  return stats_.Median();
}

double HistogramImpl::Percentile(double p) const {
  return stats_.Percentile(p);
}

double HistogramImpl::Average() const {
  return stats_.Average();
}

double HistogramImpl::StandardDeviation() const {
 return stats_.StandardDeviation();
}

std::string HistogramImpl::ToString() const {
  return stats_.ToString();
}

void HistogramImpl::Data(HistogramData * const data) const {
  stats_.Data(data);
}

} // namespace levedb
