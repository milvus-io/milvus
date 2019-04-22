//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/iostats_context.h"

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
namespace rocksdb {
extern __thread IOStatsContext iostats_context;
}  // namespace rocksdb

// increment a specific counter by the specified value
#define IOSTATS_ADD(metric, value) (iostats_context.metric += value)

// Increase metric value only when it is positive
#define IOSTATS_ADD_IF_POSITIVE(metric, value)   \
  if (value > 0) { IOSTATS_ADD(metric, value); }

// reset a specific counter to zero
#define IOSTATS_RESET(metric) (iostats_context.metric = 0)

// reset all counters to zero
#define IOSTATS_RESET_ALL() (iostats_context.Reset())

#define IOSTATS_SET_THREAD_POOL_ID(value) \
  (iostats_context.thread_pool_id = value)

#define IOSTATS_THREAD_POOL_ID() (iostats_context.thread_pool_id)

#define IOSTATS(metric) (iostats_context.metric)

// Declare and set start time of the timer
#define IOSTATS_TIMER_GUARD(metric)                                     \
  PerfStepTimer iostats_step_timer_##metric(&(iostats_context.metric)); \
  iostats_step_timer_##metric.Start();

// Declare and set start time of the timer
#define IOSTATS_CPU_TIMER_GUARD(metric, env)           \
  PerfStepTimer iostats_step_timer_##metric(           \
      &(iostats_context.metric), env, true,            \
      PerfLevel::kEnableTimeAndCPUTimeExceptForMutex); \
  iostats_step_timer_##metric.Start();

#else  // ROCKSDB_SUPPORT_THREAD_LOCAL

#define IOSTATS_ADD(metric, value)
#define IOSTATS_ADD_IF_POSITIVE(metric, value)
#define IOSTATS_RESET(metric)
#define IOSTATS_RESET_ALL()
#define IOSTATS_SET_THREAD_POOL_ID(value)
#define IOSTATS_THREAD_POOL_ID()
#define IOSTATS(metric) 0

#define IOSTATS_TIMER_GUARD(metric)

#endif  // ROCKSDB_SUPPORT_THREAD_LOCAL
