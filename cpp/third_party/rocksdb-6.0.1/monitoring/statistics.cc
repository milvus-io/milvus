//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "monitoring/statistics.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include "rocksdb/statistics.h"
#include "port/likely.h"
#include <algorithm>
#include <cstdio>

namespace rocksdb {

// The order of items listed in  Tickers should be the same as
// the order listed in TickersNameMap
const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
    {BLOCK_CACHE_MISS, "rocksdb.block.cache.miss"},
    {BLOCK_CACHE_HIT, "rocksdb.block.cache.hit"},
    {BLOCK_CACHE_ADD, "rocksdb.block.cache.add"},
    {BLOCK_CACHE_ADD_FAILURES, "rocksdb.block.cache.add.failures"},
    {BLOCK_CACHE_INDEX_MISS, "rocksdb.block.cache.index.miss"},
    {BLOCK_CACHE_INDEX_HIT, "rocksdb.block.cache.index.hit"},
    {BLOCK_CACHE_INDEX_ADD, "rocksdb.block.cache.index.add"},
    {BLOCK_CACHE_INDEX_BYTES_INSERT, "rocksdb.block.cache.index.bytes.insert"},
    {BLOCK_CACHE_INDEX_BYTES_EVICT, "rocksdb.block.cache.index.bytes.evict"},
    {BLOCK_CACHE_FILTER_MISS, "rocksdb.block.cache.filter.miss"},
    {BLOCK_CACHE_FILTER_HIT, "rocksdb.block.cache.filter.hit"},
    {BLOCK_CACHE_FILTER_ADD, "rocksdb.block.cache.filter.add"},
    {BLOCK_CACHE_FILTER_BYTES_INSERT,
     "rocksdb.block.cache.filter.bytes.insert"},
    {BLOCK_CACHE_FILTER_BYTES_EVICT, "rocksdb.block.cache.filter.bytes.evict"},
    {BLOCK_CACHE_DATA_MISS, "rocksdb.block.cache.data.miss"},
    {BLOCK_CACHE_DATA_HIT, "rocksdb.block.cache.data.hit"},
    {BLOCK_CACHE_DATA_ADD, "rocksdb.block.cache.data.add"},
    {BLOCK_CACHE_DATA_BYTES_INSERT, "rocksdb.block.cache.data.bytes.insert"},
    {BLOCK_CACHE_BYTES_READ, "rocksdb.block.cache.bytes.read"},
    {BLOCK_CACHE_BYTES_WRITE, "rocksdb.block.cache.bytes.write"},
    {BLOOM_FILTER_USEFUL, "rocksdb.bloom.filter.useful"},
    {BLOOM_FILTER_FULL_POSITIVE, "rocksdb.bloom.filter.full.positive"},
    {BLOOM_FILTER_FULL_TRUE_POSITIVE,
     "rocksdb.bloom.filter.full.true.positive"},
    {PERSISTENT_CACHE_HIT, "rocksdb.persistent.cache.hit"},
    {PERSISTENT_CACHE_MISS, "rocksdb.persistent.cache.miss"},
    {SIM_BLOCK_CACHE_HIT, "rocksdb.sim.block.cache.hit"},
    {SIM_BLOCK_CACHE_MISS, "rocksdb.sim.block.cache.miss"},
    {MEMTABLE_HIT, "rocksdb.memtable.hit"},
    {MEMTABLE_MISS, "rocksdb.memtable.miss"},
    {GET_HIT_L0, "rocksdb.l0.hit"},
    {GET_HIT_L1, "rocksdb.l1.hit"},
    {GET_HIT_L2_AND_UP, "rocksdb.l2andup.hit"},
    {COMPACTION_KEY_DROP_NEWER_ENTRY, "rocksdb.compaction.key.drop.new"},
    {COMPACTION_KEY_DROP_OBSOLETE, "rocksdb.compaction.key.drop.obsolete"},
    {COMPACTION_KEY_DROP_RANGE_DEL, "rocksdb.compaction.key.drop.range_del"},
    {COMPACTION_KEY_DROP_USER, "rocksdb.compaction.key.drop.user"},
    {COMPACTION_RANGE_DEL_DROP_OBSOLETE,
     "rocksdb.compaction.range_del.drop.obsolete"},
    {COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
     "rocksdb.compaction.optimized.del.drop.obsolete"},
    {COMPACTION_CANCELLED, "rocksdb.compaction.cancelled"},
    {NUMBER_KEYS_WRITTEN, "rocksdb.number.keys.written"},
    {NUMBER_KEYS_READ, "rocksdb.number.keys.read"},
    {NUMBER_KEYS_UPDATED, "rocksdb.number.keys.updated"},
    {BYTES_WRITTEN, "rocksdb.bytes.written"},
    {BYTES_READ, "rocksdb.bytes.read"},
    {NUMBER_DB_SEEK, "rocksdb.number.db.seek"},
    {NUMBER_DB_NEXT, "rocksdb.number.db.next"},
    {NUMBER_DB_PREV, "rocksdb.number.db.prev"},
    {NUMBER_DB_SEEK_FOUND, "rocksdb.number.db.seek.found"},
    {NUMBER_DB_NEXT_FOUND, "rocksdb.number.db.next.found"},
    {NUMBER_DB_PREV_FOUND, "rocksdb.number.db.prev.found"},
    {ITER_BYTES_READ, "rocksdb.db.iter.bytes.read"},
    {NO_FILE_CLOSES, "rocksdb.no.file.closes"},
    {NO_FILE_OPENS, "rocksdb.no.file.opens"},
    {NO_FILE_ERRORS, "rocksdb.no.file.errors"},
    {STALL_L0_SLOWDOWN_MICROS, "rocksdb.l0.slowdown.micros"},
    {STALL_MEMTABLE_COMPACTION_MICROS, "rocksdb.memtable.compaction.micros"},
    {STALL_L0_NUM_FILES_MICROS, "rocksdb.l0.num.files.stall.micros"},
    {STALL_MICROS, "rocksdb.stall.micros"},
    {DB_MUTEX_WAIT_MICROS, "rocksdb.db.mutex.wait.micros"},
    {RATE_LIMIT_DELAY_MILLIS, "rocksdb.rate.limit.delay.millis"},
    {NO_ITERATORS, "rocksdb.num.iterators"},
    {NUMBER_MULTIGET_CALLS, "rocksdb.number.multiget.get"},
    {NUMBER_MULTIGET_KEYS_READ, "rocksdb.number.multiget.keys.read"},
    {NUMBER_MULTIGET_BYTES_READ, "rocksdb.number.multiget.bytes.read"},
    {NUMBER_FILTERED_DELETES, "rocksdb.number.deletes.filtered"},
    {NUMBER_MERGE_FAILURES, "rocksdb.number.merge.failures"},
    {BLOOM_FILTER_PREFIX_CHECKED, "rocksdb.bloom.filter.prefix.checked"},
    {BLOOM_FILTER_PREFIX_USEFUL, "rocksdb.bloom.filter.prefix.useful"},
    {NUMBER_OF_RESEEKS_IN_ITERATION, "rocksdb.number.reseeks.iteration"},
    {GET_UPDATES_SINCE_CALLS, "rocksdb.getupdatessince.calls"},
    {BLOCK_CACHE_COMPRESSED_MISS, "rocksdb.block.cachecompressed.miss"},
    {BLOCK_CACHE_COMPRESSED_HIT, "rocksdb.block.cachecompressed.hit"},
    {BLOCK_CACHE_COMPRESSED_ADD, "rocksdb.block.cachecompressed.add"},
    {BLOCK_CACHE_COMPRESSED_ADD_FAILURES,
     "rocksdb.block.cachecompressed.add.failures"},
    {WAL_FILE_SYNCED, "rocksdb.wal.synced"},
    {WAL_FILE_BYTES, "rocksdb.wal.bytes"},
    {WRITE_DONE_BY_SELF, "rocksdb.write.self"},
    {WRITE_DONE_BY_OTHER, "rocksdb.write.other"},
    {WRITE_TIMEDOUT, "rocksdb.write.timeout"},
    {WRITE_WITH_WAL, "rocksdb.write.wal"},
    {COMPACT_READ_BYTES, "rocksdb.compact.read.bytes"},
    {COMPACT_WRITE_BYTES, "rocksdb.compact.write.bytes"},
    {FLUSH_WRITE_BYTES, "rocksdb.flush.write.bytes"},
    {NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
     "rocksdb.number.direct.load.table.properties"},
    {NUMBER_SUPERVERSION_ACQUIRES, "rocksdb.number.superversion_acquires"},
    {NUMBER_SUPERVERSION_RELEASES, "rocksdb.number.superversion_releases"},
    {NUMBER_SUPERVERSION_CLEANUPS, "rocksdb.number.superversion_cleanups"},
    {NUMBER_BLOCK_COMPRESSED, "rocksdb.number.block.compressed"},
    {NUMBER_BLOCK_DECOMPRESSED, "rocksdb.number.block.decompressed"},
    {NUMBER_BLOCK_NOT_COMPRESSED, "rocksdb.number.block.not_compressed"},
    {MERGE_OPERATION_TOTAL_TIME, "rocksdb.merge.operation.time.nanos"},
    {FILTER_OPERATION_TOTAL_TIME, "rocksdb.filter.operation.time.nanos"},
    {ROW_CACHE_HIT, "rocksdb.row.cache.hit"},
    {ROW_CACHE_MISS, "rocksdb.row.cache.miss"},
    {READ_AMP_ESTIMATE_USEFUL_BYTES, "rocksdb.read.amp.estimate.useful.bytes"},
    {READ_AMP_TOTAL_READ_BYTES, "rocksdb.read.amp.total.read.bytes"},
    {NUMBER_RATE_LIMITER_DRAINS, "rocksdb.number.rate_limiter.drains"},
    {NUMBER_ITER_SKIP, "rocksdb.number.iter.skip"},
    {BLOB_DB_NUM_PUT, "rocksdb.blobdb.num.put"},
    {BLOB_DB_NUM_WRITE, "rocksdb.blobdb.num.write"},
    {BLOB_DB_NUM_GET, "rocksdb.blobdb.num.get"},
    {BLOB_DB_NUM_MULTIGET, "rocksdb.blobdb.num.multiget"},
    {BLOB_DB_NUM_SEEK, "rocksdb.blobdb.num.seek"},
    {BLOB_DB_NUM_NEXT, "rocksdb.blobdb.num.next"},
    {BLOB_DB_NUM_PREV, "rocksdb.blobdb.num.prev"},
    {BLOB_DB_NUM_KEYS_WRITTEN, "rocksdb.blobdb.num.keys.written"},
    {BLOB_DB_NUM_KEYS_READ, "rocksdb.blobdb.num.keys.read"},
    {BLOB_DB_BYTES_WRITTEN, "rocksdb.blobdb.bytes.written"},
    {BLOB_DB_BYTES_READ, "rocksdb.blobdb.bytes.read"},
    {BLOB_DB_WRITE_INLINED, "rocksdb.blobdb.write.inlined"},
    {BLOB_DB_WRITE_INLINED_TTL, "rocksdb.blobdb.write.inlined.ttl"},
    {BLOB_DB_WRITE_BLOB, "rocksdb.blobdb.write.blob"},
    {BLOB_DB_WRITE_BLOB_TTL, "rocksdb.blobdb.write.blob.ttl"},
    {BLOB_DB_BLOB_FILE_BYTES_WRITTEN, "rocksdb.blobdb.blob.file.bytes.written"},
    {BLOB_DB_BLOB_FILE_BYTES_READ, "rocksdb.blobdb.blob.file.bytes.read"},
    {BLOB_DB_BLOB_FILE_SYNCED, "rocksdb.blobdb.blob.file.synced"},
    {BLOB_DB_BLOB_INDEX_EXPIRED_COUNT,
     "rocksdb.blobdb.blob.index.expired.count"},
    {BLOB_DB_BLOB_INDEX_EXPIRED_SIZE, "rocksdb.blobdb.blob.index.expired.size"},
    {BLOB_DB_BLOB_INDEX_EVICTED_COUNT,
     "rocksdb.blobdb.blob.index.evicted.count"},
    {BLOB_DB_BLOB_INDEX_EVICTED_SIZE, "rocksdb.blobdb.blob.index.evicted.size"},
    {BLOB_DB_GC_NUM_FILES, "rocksdb.blobdb.gc.num.files"},
    {BLOB_DB_GC_NUM_NEW_FILES, "rocksdb.blobdb.gc.num.new.files"},
    {BLOB_DB_GC_FAILURES, "rocksdb.blobdb.gc.failures"},
    {BLOB_DB_GC_NUM_KEYS_OVERWRITTEN, "rocksdb.blobdb.gc.num.keys.overwritten"},
    {BLOB_DB_GC_NUM_KEYS_EXPIRED, "rocksdb.blobdb.gc.num.keys.expired"},
    {BLOB_DB_GC_NUM_KEYS_RELOCATED, "rocksdb.blobdb.gc.num.keys.relocated"},
    {BLOB_DB_GC_BYTES_OVERWRITTEN, "rocksdb.blobdb.gc.bytes.overwritten"},
    {BLOB_DB_GC_BYTES_EXPIRED, "rocksdb.blobdb.gc.bytes.expired"},
    {BLOB_DB_GC_BYTES_RELOCATED, "rocksdb.blobdb.gc.bytes.relocated"},
    {BLOB_DB_FIFO_NUM_FILES_EVICTED, "rocksdb.blobdb.fifo.num.files.evicted"},
    {BLOB_DB_FIFO_NUM_KEYS_EVICTED, "rocksdb.blobdb.fifo.num.keys.evicted"},
    {BLOB_DB_FIFO_BYTES_EVICTED, "rocksdb.blobdb.fifo.bytes.evicted"},
    {TXN_PREPARE_MUTEX_OVERHEAD, "rocksdb.txn.overhead.mutex.prepare"},
    {TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD,
     "rocksdb.txn.overhead.mutex.old.commit.map"},
    {TXN_DUPLICATE_KEY_OVERHEAD, "rocksdb.txn.overhead.duplicate.key"},
    {TXN_SNAPSHOT_MUTEX_OVERHEAD, "rocksdb.txn.overhead.mutex.snapshot"},
    {NUMBER_MULTIGET_KEYS_FOUND, "rocksdb.number.multiget.keys.found"},
    {NO_ITERATOR_CREATED, "rocksdb.num.iterator.created"},
    {NO_ITERATOR_DELETED, "rocksdb.num.iterator.deleted"},
    {BLOCK_CACHE_COMPRESSION_DICT_MISS,
     "rocksdb.block.cache.compression.dict.miss"},
    {BLOCK_CACHE_COMPRESSION_DICT_HIT,
     "rocksdb.block.cache.compression.dict.hit"},
    {BLOCK_CACHE_COMPRESSION_DICT_ADD,
     "rocksdb.block.cache.compression.dict.add"},
    {BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT,
     "rocksdb.block.cache.compression.dict.bytes.insert"},
    {BLOCK_CACHE_COMPRESSION_DICT_BYTES_EVICT,
     "rocksdb.block.cache.compression.dict.bytes.evict"},
};

const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap = {
    {DB_GET, "rocksdb.db.get.micros"},
    {DB_WRITE, "rocksdb.db.write.micros"},
    {COMPACTION_TIME, "rocksdb.compaction.times.micros"},
    {COMPACTION_CPU_TIME, "rocksdb.compaction.times.cpu_micros"},
    {SUBCOMPACTION_SETUP_TIME, "rocksdb.subcompaction.setup.times.micros"},
    {TABLE_SYNC_MICROS, "rocksdb.table.sync.micros"},
    {COMPACTION_OUTFILE_SYNC_MICROS, "rocksdb.compaction.outfile.sync.micros"},
    {WAL_FILE_SYNC_MICROS, "rocksdb.wal.file.sync.micros"},
    {MANIFEST_FILE_SYNC_MICROS, "rocksdb.manifest.file.sync.micros"},
    {TABLE_OPEN_IO_MICROS, "rocksdb.table.open.io.micros"},
    {DB_MULTIGET, "rocksdb.db.multiget.micros"},
    {READ_BLOCK_COMPACTION_MICROS, "rocksdb.read.block.compaction.micros"},
    {READ_BLOCK_GET_MICROS, "rocksdb.read.block.get.micros"},
    {WRITE_RAW_BLOCK_MICROS, "rocksdb.write.raw.block.micros"},
    {STALL_L0_SLOWDOWN_COUNT, "rocksdb.l0.slowdown.count"},
    {STALL_MEMTABLE_COMPACTION_COUNT, "rocksdb.memtable.compaction.count"},
    {STALL_L0_NUM_FILES_COUNT, "rocksdb.num.files.stall.count"},
    {HARD_RATE_LIMIT_DELAY_COUNT, "rocksdb.hard.rate.limit.delay.count"},
    {SOFT_RATE_LIMIT_DELAY_COUNT, "rocksdb.soft.rate.limit.delay.count"},
    {NUM_FILES_IN_SINGLE_COMPACTION, "rocksdb.numfiles.in.singlecompaction"},
    {DB_SEEK, "rocksdb.db.seek.micros"},
    {WRITE_STALL, "rocksdb.db.write.stall"},
    {SST_READ_MICROS, "rocksdb.sst.read.micros"},
    {NUM_SUBCOMPACTIONS_SCHEDULED, "rocksdb.num.subcompactions.scheduled"},
    {BYTES_PER_READ, "rocksdb.bytes.per.read"},
    {BYTES_PER_WRITE, "rocksdb.bytes.per.write"},
    {BYTES_PER_MULTIGET, "rocksdb.bytes.per.multiget"},
    {BYTES_COMPRESSED, "rocksdb.bytes.compressed"},
    {BYTES_DECOMPRESSED, "rocksdb.bytes.decompressed"},
    {COMPRESSION_TIMES_NANOS, "rocksdb.compression.times.nanos"},
    {DECOMPRESSION_TIMES_NANOS, "rocksdb.decompression.times.nanos"},
    {READ_NUM_MERGE_OPERANDS, "rocksdb.read.num.merge_operands"},
    {BLOB_DB_KEY_SIZE, "rocksdb.blobdb.key.size"},
    {BLOB_DB_VALUE_SIZE, "rocksdb.blobdb.value.size"},
    {BLOB_DB_WRITE_MICROS, "rocksdb.blobdb.write.micros"},
    {BLOB_DB_GET_MICROS, "rocksdb.blobdb.get.micros"},
    {BLOB_DB_MULTIGET_MICROS, "rocksdb.blobdb.multiget.micros"},
    {BLOB_DB_SEEK_MICROS, "rocksdb.blobdb.seek.micros"},
    {BLOB_DB_NEXT_MICROS, "rocksdb.blobdb.next.micros"},
    {BLOB_DB_PREV_MICROS, "rocksdb.blobdb.prev.micros"},
    {BLOB_DB_BLOB_FILE_WRITE_MICROS, "rocksdb.blobdb.blob.file.write.micros"},
    {BLOB_DB_BLOB_FILE_READ_MICROS, "rocksdb.blobdb.blob.file.read.micros"},
    {BLOB_DB_BLOB_FILE_SYNC_MICROS, "rocksdb.blobdb.blob.file.sync.micros"},
    {BLOB_DB_GC_MICROS, "rocksdb.blobdb.gc.micros"},
    {BLOB_DB_COMPRESSION_MICROS, "rocksdb.blobdb.compression.micros"},
    {BLOB_DB_DECOMPRESSION_MICROS, "rocksdb.blobdb.decompression.micros"},
    {FLUSH_TIME, "rocksdb.db.flush.micros"},
};

std::shared_ptr<Statistics> CreateDBStatistics() {
  return std::make_shared<StatisticsImpl>(nullptr);
}

StatisticsImpl::StatisticsImpl(std::shared_ptr<Statistics> stats)
    : stats_(std::move(stats)) {}

StatisticsImpl::~StatisticsImpl() {}

uint64_t StatisticsImpl::getTickerCount(uint32_t tickerType) const {
  MutexLock lock(&aggregate_lock_);
  return getTickerCountLocked(tickerType);
}

uint64_t StatisticsImpl::getTickerCountLocked(uint32_t tickerType) const {
  assert(tickerType < TICKER_ENUM_MAX);
  uint64_t res = 0;
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res += per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType];
  }
  return res;
}

void StatisticsImpl::histogramData(uint32_t histogramType,
                                   HistogramData* const data) const {
  MutexLock lock(&aggregate_lock_);
  getHistogramImplLocked(histogramType)->Data(data);
}

std::unique_ptr<HistogramImpl> StatisticsImpl::getHistogramImplLocked(
    uint32_t histogramType) const {
  assert(histogramType < HISTOGRAM_ENUM_MAX);
  std::unique_ptr<HistogramImpl> res_hist(new HistogramImpl());
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res_hist->Merge(
        per_core_stats_.AccessAtCore(core_idx)->histograms_[histogramType]);
  }
  return res_hist;
}

std::string StatisticsImpl::getHistogramString(uint32_t histogramType) const {
  MutexLock lock(&aggregate_lock_);
  return getHistogramImplLocked(histogramType)->ToString();
}

void StatisticsImpl::setTickerCount(uint32_t tickerType, uint64_t count) {
  {
    MutexLock lock(&aggregate_lock_);
    setTickerCountLocked(tickerType, count);
  }
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->setTickerCount(tickerType, count);
  }
}

void StatisticsImpl::setTickerCountLocked(uint32_t tickerType, uint64_t count) {
  assert(tickerType < TICKER_ENUM_MAX);
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    if (core_idx == 0) {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = count;
    } else {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = 0;
    }
  }
}

uint64_t StatisticsImpl::getAndResetTickerCount(uint32_t tickerType) {
  uint64_t sum = 0;
  {
    MutexLock lock(&aggregate_lock_);
    assert(tickerType < TICKER_ENUM_MAX);
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      sum +=
          per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType].exchange(
              0, std::memory_order_relaxed);
    }
  }
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->setTickerCount(tickerType, 0);
  }
  return sum;
}

void StatisticsImpl::recordTick(uint32_t tickerType, uint64_t count) {
  assert(tickerType < TICKER_ENUM_MAX);
  per_core_stats_.Access()->tickers_[tickerType].fetch_add(
      count, std::memory_order_relaxed);
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->recordTick(tickerType, count);
  }
}

void StatisticsImpl::measureTime(uint32_t histogramType, uint64_t value) {
  assert(histogramType < HISTOGRAM_ENUM_MAX);
  per_core_stats_.Access()->histograms_[histogramType].Add(value);
  if (stats_ && histogramType < HISTOGRAM_ENUM_MAX) {
    stats_->measureTime(histogramType, value);
  }
}

Status StatisticsImpl::Reset() {
  MutexLock lock(&aggregate_lock_);
  for (uint32_t i = 0; i < TICKER_ENUM_MAX; ++i) {
    setTickerCountLocked(i, 0);
  }
  for (uint32_t i = 0; i < HISTOGRAM_ENUM_MAX; ++i) {
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      per_core_stats_.AccessAtCore(core_idx)->histograms_[i].Clear();
    }
  }
  return Status::OK();
}

namespace {

// a buffer size used for temp string buffers
const int kTmpStrBufferSize = 200;

} // namespace

std::string StatisticsImpl::ToString() const {
  MutexLock lock(&aggregate_lock_);
  std::string res;
  res.reserve(20000);
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_ENUM_MAX);
    char buffer[kTmpStrBufferSize];
    snprintf(buffer, kTmpStrBufferSize, "%s COUNT : %" PRIu64 "\n",
             t.second.c_str(), getTickerCountLocked(t.first));
    res.append(buffer);
  }
  for (const auto& h : HistogramsNameMap) {
    assert(h.first < HISTOGRAM_ENUM_MAX);
    char buffer[kTmpStrBufferSize];
    HistogramData hData;
    getHistogramImplLocked(h.first)->Data(&hData);
    // don't handle failures - buffer should always be big enough and arguments
    // should be provided correctly
    int ret =
        snprintf(buffer, kTmpStrBufferSize,
                 "%s P50 : %f P95 : %f P99 : %f P100 : %f COUNT : %" PRIu64
                 " SUM : %" PRIu64 "\n",
                 h.second.c_str(), hData.median, hData.percentile95,
                 hData.percentile99, hData.max, hData.count, hData.sum);
    if (ret < 0 || ret >= kTmpStrBufferSize) {
      assert(false);
      continue;
    }
    res.append(buffer);
  }
  res.shrink_to_fit();
  return res;
}

bool StatisticsImpl::getTickerMap(
    std::map<std::string, uint64_t>* stats_map) const {
  assert(stats_map);
  if (!stats_map) return false;
  stats_map->clear();
  MutexLock lock(&aggregate_lock_);
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_ENUM_MAX);
    (*stats_map)[t.second.c_str()] = getTickerCountLocked(t.first);
  }
  return true;
}

bool StatisticsImpl::HistEnabledForType(uint32_t type) const {
  return type < HISTOGRAM_ENUM_MAX;
}

} // namespace rocksdb
