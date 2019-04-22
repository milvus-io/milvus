//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "rocksdb/statistics.h"

#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "monitoring/histogram.h"
#include "port/likely.h"
#include "port/port.h"
#include "util/core_local.h"
#include "util/mutexlock.h"

#ifdef __clang__
#define ROCKSDB_FIELD_UNUSED __attribute__((__unused__))
#else
#define ROCKSDB_FIELD_UNUSED
#endif  // __clang__

#ifndef STRINGIFY
#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#endif

namespace rocksdb {

enum TickersInternal : uint32_t {
  INTERNAL_TICKER_ENUM_START = TICKER_ENUM_MAX,
  INTERNAL_TICKER_ENUM_MAX
};

enum HistogramsInternal : uint32_t {
  INTERNAL_HISTOGRAM_START = HISTOGRAM_ENUM_MAX,
  INTERNAL_HISTOGRAM_ENUM_MAX
};

class StatisticsImpl : public Statistics {
 public:
  StatisticsImpl(std::shared_ptr<Statistics> stats);
  virtual ~StatisticsImpl();

  virtual uint64_t getTickerCount(uint32_t ticker_type) const override;
  virtual void histogramData(uint32_t histogram_type,
                             HistogramData* const data) const override;
  std::string getHistogramString(uint32_t histogram_type) const override;

  virtual void setTickerCount(uint32_t ticker_type, uint64_t count) override;
  virtual uint64_t getAndResetTickerCount(uint32_t ticker_type) override;
  virtual void recordTick(uint32_t ticker_type, uint64_t count) override;
  virtual void measureTime(uint32_t histogram_type, uint64_t value) override;

  virtual Status Reset() override;
  virtual std::string ToString() const override;
  virtual bool getTickerMap(std::map<std::string, uint64_t>*) const override;
  virtual bool HistEnabledForType(uint32_t type) const override;

 private:
  // If non-nullptr, forwards updates to the object pointed to by `stats_`.
  std::shared_ptr<Statistics> stats_;
  // Synchronizes anything that operates across other cores' local data,
  // such that operations like Reset() can be performed atomically.
  mutable port::Mutex aggregate_lock_;

  // The ticker/histogram data are stored in this structure, which we will store
  // per-core. It is cache-aligned, so tickers/histograms belonging to different
  // cores can never share the same cache line.
  //
  // Alignment attributes expand to nothing depending on the platform
  struct ALIGN_AS(CACHE_LINE_SIZE) StatisticsData {
    std::atomic_uint_fast64_t tickers_[INTERNAL_TICKER_ENUM_MAX] = {{0}};
    HistogramImpl histograms_[INTERNAL_HISTOGRAM_ENUM_MAX];
#ifndef HAVE_ALIGNED_NEW
    char
        padding[(CACHE_LINE_SIZE -
                 (INTERNAL_TICKER_ENUM_MAX * sizeof(std::atomic_uint_fast64_t) +
                  INTERNAL_HISTOGRAM_ENUM_MAX * sizeof(HistogramImpl)) %
                     CACHE_LINE_SIZE)] ROCKSDB_FIELD_UNUSED;
#endif
    void *operator new(size_t s) { return port::cacheline_aligned_alloc(s); }
    void *operator new[](size_t s) { return port::cacheline_aligned_alloc(s); }
    void operator delete(void *p) { port::cacheline_aligned_free(p); }
    void operator delete[](void *p) { port::cacheline_aligned_free(p); }
  };

  static_assert(sizeof(StatisticsData) % CACHE_LINE_SIZE == 0, "Expected " TOSTRING(CACHE_LINE_SIZE) "-byte aligned");

  CoreLocalArray<StatisticsData> per_core_stats_;

  uint64_t getTickerCountLocked(uint32_t ticker_type) const;
  std::unique_ptr<HistogramImpl> getHistogramImplLocked(
      uint32_t histogram_type) const;
  void setTickerCountLocked(uint32_t ticker_type, uint64_t count);
};

// Utility functions
inline void MeasureTime(Statistics* statistics, uint32_t histogram_type,
                        uint64_t value) {
  if (statistics) {
    statistics->measureTime(histogram_type, value);
  }
}

inline void RecordTick(Statistics* statistics, uint32_t ticker_type,
                       uint64_t count = 1) {
  if (statistics) {
    statistics->recordTick(ticker_type, count);
  }
}

inline void SetTickerCount(Statistics* statistics, uint32_t ticker_type,
                           uint64_t count) {
  if (statistics) {
    statistics->setTickerCount(ticker_type, count);
  }
}

}
