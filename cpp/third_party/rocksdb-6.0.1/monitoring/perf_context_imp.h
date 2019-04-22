//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/perf_context.h"
#include "util/stop_watch.h"

namespace rocksdb {
#if defined(NPERF_CONTEXT) || !defined(ROCKSDB_SUPPORT_THREAD_LOCAL)
extern PerfContext perf_context;
#else
#if defined(OS_SOLARIS)
extern __thread PerfContext perf_context_;
#define perf_context (*get_perf_context())
#else
extern thread_local PerfContext perf_context;
#endif
#endif

#if defined(NPERF_CONTEXT)

#define PERF_TIMER_GUARD(metric)
#define PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(metric, condition)
#define PERF_TIMER_MEASURE(metric)
#define PERF_TIMER_STOP(metric)
#define PERF_TIMER_START(metric)
#define PERF_COUNTER_ADD(metric, value)

#else

// Stop the timer and update the metric
#define PERF_TIMER_STOP(metric) perf_step_timer_##metric.Stop();

#define PERF_TIMER_START(metric) perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define PERF_TIMER_GUARD(metric)                                  \
  PerfStepTimer perf_step_timer_##metric(&(perf_context.metric)); \
  perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define PERF_TIMER_GUARD_WITH_ENV(metric, env)                         \
  PerfStepTimer perf_step_timer_##metric(&(perf_context.metric), env); \
  perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define PERF_CPU_TIMER_GUARD(metric, env)              \
  PerfStepTimer perf_step_timer_##metric(              \
      &(perf_context.metric), env, true,               \
      PerfLevel::kEnableTimeAndCPUTimeExceptForMutex); \
  perf_step_timer_##metric.Start();

#define PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(metric, condition, stats,       \
                                               ticker_type)                    \
  PerfStepTimer perf_step_timer_##metric(&(perf_context.metric), nullptr,      \
                                         false, PerfLevel::kEnableTime, stats, \
                                         ticker_type);                         \
  if (condition) {                                                             \
    perf_step_timer_##metric.Start();                                          \
  }

// Update metric with time elapsed since last START. start time is reset
// to current timestamp.
#define PERF_TIMER_MEASURE(metric) perf_step_timer_##metric.Measure();

// Increase metric value
#define PERF_COUNTER_ADD(metric, value)        \
  if (perf_level >= PerfLevel::kEnableCount) { \
    perf_context.metric += value;              \
  }

// Increase metric value
#define PERF_COUNTER_BY_LEVEL_ADD(metric, value, level)                      \
  if (perf_level >= PerfLevel::kEnableCount &&                               \
      perf_context.per_level_perf_context_enabled &&                         \
      perf_context.level_to_perf_context) {                                  \
    if ((*(perf_context.level_to_perf_context)).find(level) !=               \
        (*(perf_context.level_to_perf_context)).end()) {                     \
      (*(perf_context.level_to_perf_context))[level].metric += value;        \
    }                                                                        \
    else {                                                                   \
      PerfContextByLevel empty_context;                                      \
      (*(perf_context.level_to_perf_context))[level] = empty_context;        \
      (*(perf_context.level_to_perf_context))[level].metric += value;       \
    }                                                                        \
  }                                                                          \

#endif

}
