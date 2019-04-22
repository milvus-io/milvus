//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based_table_reader.h"

#include <algorithm>
#include <array>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"

#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"

#include "table/block.h"
#include "table/block_based_filter_block.h"
#include "table/block_based_table_factory.h"
#include "table/block_fetcher.h"
#include "table/block_prefix_index.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/full_filter_block.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/partitioned_filter_block.h"
#include "table/persistent_cache_helper.h"
#include "table/sst_file_writer_collectors.h"
#include "table/two_level_iterator.h"

#include "monitoring/perf_context_imp.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/xxhash.h"

namespace rocksdb {

extern const uint64_t kBlockBasedTableMagicNumber;
extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;
using std::unique_ptr;

typedef BlockBasedTable::IndexReader IndexReader;

BlockBasedTable::~BlockBasedTable() {
  Close();
  delete rep_;
}

std::atomic<uint64_t> BlockBasedTable::next_cache_key_id_(0);

namespace {
// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
// @param uncompression_dict Data for presetting the compression library's
//    dictionary.
Status ReadBlockFromFile(
    RandomAccessFileReader* file, FilePrefetchBuffer* prefetch_buffer,
    const Footer& footer, const ReadOptions& options, const BlockHandle& handle,
    std::unique_ptr<Block>* result, const ImmutableCFOptions& ioptions,
    bool do_uncompress, bool maybe_compressed,
    const UncompressionDict& uncompression_dict,
    const PersistentCacheOptions& cache_options, SequenceNumber global_seqno,
    size_t read_amp_bytes_per_bit, MemoryAllocator* memory_allocator) {
  BlockContents contents;
  BlockFetcher block_fetcher(file, prefetch_buffer, footer, options, handle,
                             &contents, ioptions, do_uncompress,
                             maybe_compressed, uncompression_dict,
                             cache_options, memory_allocator);
  Status s = block_fetcher.ReadBlockContents();
  if (s.ok()) {
    result->reset(new Block(std::move(contents), global_seqno,
                            read_amp_bytes_per_bit, ioptions.statistics));
  }

  return s;
}

inline MemoryAllocator* GetMemoryAllocator(
    const BlockBasedTableOptions& table_options) {
  return table_options.block_cache.get()
             ? table_options.block_cache->memory_allocator()
             : nullptr;
}

inline MemoryAllocator* GetMemoryAllocatorForCompressedBlock(
    const BlockBasedTableOptions& table_options) {
  return table_options.block_cache_compressed.get()
             ? table_options.block_cache_compressed->memory_allocator()
             : nullptr;
}

// Delete the resource that is held by the iterator.
template <class ResourceType>
void DeleteHeldResource(void* arg, void* /*ignored*/) {
  delete reinterpret_cast<ResourceType*>(arg);
}

// Delete the entry resided in the cache.
template <class Entry>
void DeleteCachedEntry(const Slice& /*key*/, void* value) {
  auto entry = reinterpret_cast<Entry*>(value);
  delete entry;
}

void DeleteCachedFilterEntry(const Slice& key, void* value);
void DeleteCachedIndexEntry(const Slice& key, void* value);
void DeleteCachedUncompressionDictEntry(const Slice& key, void* value);

// Release the cached entry and decrement its ref count.
void ReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Release the cached entry and decrement its ref count.
void ForceReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle, true /* force_erase */);
}

Slice GetCacheKeyFromOffset(const char* cache_key_prefix,
                            size_t cache_key_prefix_size, uint64_t offset,
                            char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= BlockBasedTable::kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end = EncodeVarint64(cache_key + cache_key_prefix_size, offset);
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Cache::Handle* GetEntryFromCache(Cache* block_cache, const Slice& key,
                                 int level,
                                 Tickers block_cache_miss_ticker,
                                 Tickers block_cache_hit_ticker,
                                 uint64_t* block_cache_miss_stats,
                                 uint64_t* block_cache_hit_stats,
                                 Statistics* statistics,
                                 GetContext* get_context) {
  auto cache_handle = block_cache->Lookup(key, statistics);
  if (cache_handle != nullptr) {
    PERF_COUNTER_ADD(block_cache_hit_count, 1);
    PERF_COUNTER_BY_LEVEL_ADD(block_cache_hit_count, 1,
      static_cast<uint32_t>(level));
    if (get_context != nullptr) {
      // overall cache hit
      get_context->get_context_stats_.num_cache_hit++;
      // total bytes read from cache
      get_context->get_context_stats_.num_cache_bytes_read +=
          block_cache->GetUsage(cache_handle);
      // block-type specific cache hit
      (*block_cache_hit_stats)++;
    } else {
      // overall cache hit
      RecordTick(statistics, BLOCK_CACHE_HIT);
      // total bytes read from cache
      RecordTick(statistics, BLOCK_CACHE_BYTES_READ,
                 block_cache->GetUsage(cache_handle));
      RecordTick(statistics, block_cache_hit_ticker);
    }
  } else {
    PERF_COUNTER_BY_LEVEL_ADD(block_cache_miss_count, 1,
      static_cast<uint32_t>(level));
    if (get_context != nullptr) {
      // overall cache miss
      get_context->get_context_stats_.num_cache_miss++;
      // block-type specific cache miss
      (*block_cache_miss_stats)++;
    } else {
      RecordTick(statistics, BLOCK_CACHE_MISS);
      RecordTick(statistics, block_cache_miss_ticker);
    }
  }

  return cache_handle;
}

// For hash based index, return true if prefix_extractor and
// prefix_extractor_block mismatch, false otherwise. This flag will be used
// as total_order_seek via NewIndexIterator
bool PrefixExtractorChanged(const TableProperties* table_properties,
                            const SliceTransform* prefix_extractor) {
  // BlockBasedTableOptions::kHashSearch requires prefix_extractor to be set.
  // Turn off hash index in prefix_extractor is not set; if  prefix_extractor
  // is set but prefix_extractor_block is not set, also disable hash index
  if (prefix_extractor == nullptr || table_properties == nullptr ||
      table_properties->prefix_extractor_name.empty()) {
    return true;
  }

  // prefix_extractor and prefix_extractor_block are both non-empty
  if (table_properties->prefix_extractor_name.compare(
          prefix_extractor->Name()) != 0) {
    return true;
  } else {
    return false;
  }
}

}  // namespace

// Index that allows binary search lookup in a two-level index structure.
class PartitionIndexReader : public IndexReader, public Cleanable {
 public:
  // Read the partition index from the file and create an instance for
  // `PartitionIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(BlockBasedTable* table, RandomAccessFileReader* file,
                       FilePrefetchBuffer* prefetch_buffer,
                       const Footer& footer, const BlockHandle& index_handle,
                       const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icomparator,
                       IndexReader** index_reader,
                       const PersistentCacheOptions& cache_options,
                       const int level, const bool index_key_includes_seq,
                       const bool index_value_is_full,
                       MemoryAllocator* memory_allocator) {
    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(
        file, prefetch_buffer, footer, ReadOptions(), index_handle,
        &index_block, ioptions, true /* decompress */,
        true /*maybe_compressed*/, UncompressionDict::GetEmptyDict(),
        cache_options, kDisableGlobalSequenceNumber,
        0 /* read_amp_bytes_per_bit */, memory_allocator);

    if (s.ok()) {
      *index_reader = new PartitionIndexReader(
          table, icomparator, std::move(index_block), ioptions.statistics,
          level, index_key_includes_seq, index_value_is_full);
    }

    return s;
  }

  // return a two-level iterator: first level is on the partition index
  InternalIteratorBase<BlockHandle>* NewIterator(
      IndexBlockIter* /*iter*/ = nullptr, bool /*dont_care*/ = true,
      bool fill_cache = true) override {
    Statistics* kNullStats = nullptr;
    // Filters are already checked before seeking the index
    if (!partition_map_.empty()) {
      // We don't return pinned datat from index blocks, so no need
      // to set `block_contents_pinned`.
      return NewTwoLevelIterator(
          new BlockBasedTable::PartitionedIndexIteratorState(
              table_, &partition_map_, index_key_includes_seq_,
              index_value_is_full_),
          index_block_->NewIterator<IndexBlockIter>(
              icomparator_, icomparator_->user_comparator(), nullptr,
              kNullStats, true, index_key_includes_seq_, index_value_is_full_));
    } else {
      auto ro = ReadOptions();
      ro.fill_cache = fill_cache;
      bool kIsIndex = true;
      // We don't return pinned datat from index blocks, so no need
      // to set `block_contents_pinned`.
      return new BlockBasedTableIterator<IndexBlockIter, BlockHandle>(
          table_, ro, *icomparator_,
          index_block_->NewIterator<IndexBlockIter>(
              icomparator_, icomparator_->user_comparator(), nullptr,
              kNullStats, true, index_key_includes_seq_, index_value_is_full_),
          false, true, /* prefix_extractor */ nullptr, kIsIndex,
          index_key_includes_seq_, index_value_is_full_);
    }
    // TODO(myabandeh): Update TwoLevelIterator to be able to make use of
    // on-stack BlockIter while the state is on heap. Currentlly it assumes
    // the first level iter is always on heap and will attempt to delete it
    // in its destructor.
  }

  void CacheDependencies(bool pin) override {
    // Before read partitions, prefetch them to avoid lots of IOs
    auto rep = table_->rep_;
    IndexBlockIter biter;
    BlockHandle handle;
    Statistics* kNullStats = nullptr;
    // We don't return pinned datat from index blocks, so no need
    // to set `block_contents_pinned`.
    index_block_->NewIterator<IndexBlockIter>(
        icomparator_, icomparator_->user_comparator(), &biter, kNullStats, true,
        index_key_includes_seq_, index_value_is_full_);
    // Index partitions are assumed to be consecuitive. Prefetch them all.
    // Read the first block offset
    biter.SeekToFirst();
    if (!biter.Valid()) {
      // Empty index.
      return;
    }
    handle = biter.value();
    uint64_t prefetch_off = handle.offset();

    // Read the last block's offset
    biter.SeekToLast();
    if (!biter.Valid()) {
      // Empty index.
      return;
    }
    handle = biter.value();
    uint64_t last_off = handle.offset() + handle.size() + kBlockTrailerSize;
    uint64_t prefetch_len = last_off - prefetch_off;
    std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;
    auto& file = table_->rep_->file;
    prefetch_buffer.reset(new FilePrefetchBuffer());
    Status s = prefetch_buffer->Prefetch(file.get(), prefetch_off,
                                         static_cast<size_t>(prefetch_len));

    // After prefetch, read the partitions one by one
    biter.SeekToFirst();
    auto ro = ReadOptions();
    Cache* block_cache = rep->table_options.block_cache.get();
    for (; biter.Valid(); biter.Next()) {
      handle = biter.value();
      BlockBasedTable::CachableEntry<Block> block;
      const bool is_index = true;
      // TODO: Support counter batch update for partitioned index and
      // filter blocks
      s = table_->MaybeReadBlockAndLoadToCache(
          prefetch_buffer.get(), rep, ro, handle,
          UncompressionDict::GetEmptyDict(), &block, is_index,
          nullptr /* get_context */);

      assert(s.ok() || block.value == nullptr);
      if (s.ok() && block.value != nullptr) {
        if (block.cache_handle != nullptr) {
          if (pin) {
            partition_map_[handle.offset()] = block;
            RegisterCleanup(&ReleaseCachedEntry, block_cache,
                            block.cache_handle);
          } else {
            block_cache->Release(block.cache_handle);
          }
        } else {
          delete block.value;
        }
      }
    }
  }

  size_t size() const override { return index_block_->size(); }
  size_t usable_size() const override { return index_block_->usable_size(); }

  size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    size_t usage = index_block_->ApproximateMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size((void*)this);
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    // TODO(myabandeh): more accurate estimate of partition_map_ mem usage
    return usage;
  }

 private:
  PartitionIndexReader(BlockBasedTable* table,
                       const InternalKeyComparator* icomparator,
                       std::unique_ptr<Block>&& index_block, Statistics* stats,
                       const int /*level*/, const bool index_key_includes_seq,
                       const bool index_value_is_full)
      : IndexReader(icomparator, stats),
        table_(table),
        index_block_(std::move(index_block)),
        index_key_includes_seq_(index_key_includes_seq),
        index_value_is_full_(index_value_is_full) {
    assert(index_block_ != nullptr);
  }
  BlockBasedTable* table_;
  std::unique_ptr<Block> index_block_;
  std::unordered_map<uint64_t, BlockBasedTable::CachableEntry<Block>>
      partition_map_;
  const bool index_key_includes_seq_;
  const bool index_value_is_full_;
};

// Index that allows binary search lookup for the first key of each block.
// This class can be viewed as a thin wrapper for `Block` class which already
// supports binary search.
class BinarySearchIndexReader : public IndexReader {
 public:
  // Read index from the file and create an intance for
  // `BinarySearchIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(RandomAccessFileReader* file,
                       FilePrefetchBuffer* prefetch_buffer,
                       const Footer& footer, const BlockHandle& index_handle,
                       const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icomparator,
                       IndexReader** index_reader,
                       const PersistentCacheOptions& cache_options,
                       const bool index_key_includes_seq,
                       const bool index_value_is_full,
                       MemoryAllocator* memory_allocator) {
    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(
        file, prefetch_buffer, footer, ReadOptions(), index_handle,
        &index_block, ioptions, true /* decompress */,
        true /*maybe_compressed*/, UncompressionDict::GetEmptyDict(),
        cache_options, kDisableGlobalSequenceNumber,
        0 /* read_amp_bytes_per_bit */, memory_allocator);

    if (s.ok()) {
      *index_reader = new BinarySearchIndexReader(
          icomparator, std::move(index_block), ioptions.statistics,
          index_key_includes_seq, index_value_is_full);
    }

    return s;
  }

  InternalIteratorBase<BlockHandle>* NewIterator(
      IndexBlockIter* iter = nullptr, bool /*dont_care*/ = true,
      bool /*dont_care*/ = true) override {
    Statistics* kNullStats = nullptr;
    // We don't return pinned datat from index blocks, so no need
    // to set `block_contents_pinned`.
    return index_block_->NewIterator<IndexBlockIter>(
        icomparator_, icomparator_->user_comparator(), iter, kNullStats, true,
        index_key_includes_seq_, index_value_is_full_);
  }

  size_t size() const override { return index_block_->size(); }
  size_t usable_size() const override { return index_block_->usable_size(); }

  size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    size_t usage = index_block_->ApproximateMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size((void*)this);
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return usage;
  }

 private:
  BinarySearchIndexReader(const InternalKeyComparator* icomparator,
                          std::unique_ptr<Block>&& index_block,
                          Statistics* stats, const bool index_key_includes_seq,
                          const bool index_value_is_full)
      : IndexReader(icomparator, stats),
        index_block_(std::move(index_block)),
        index_key_includes_seq_(index_key_includes_seq),
        index_value_is_full_(index_value_is_full) {
    assert(index_block_ != nullptr);
  }
  std::unique_ptr<Block> index_block_;
  const bool index_key_includes_seq_;
  const bool index_value_is_full_;
};

// Index that leverages an internal hash table to quicken the lookup for a given
// key.
class HashIndexReader : public IndexReader {
 public:
  static Status Create(
      const SliceTransform* hash_key_extractor, const Footer& footer,
      RandomAccessFileReader* file, FilePrefetchBuffer* prefetch_buffer,
      const ImmutableCFOptions& ioptions,
      const InternalKeyComparator* icomparator, const BlockHandle& index_handle,
      InternalIterator* meta_index_iter, IndexReader** index_reader,
      bool /*hash_index_allow_collision*/,
      const PersistentCacheOptions& cache_options,
      const bool index_key_includes_seq, const bool index_value_is_full,
      MemoryAllocator* memory_allocator) {
    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(
        file, prefetch_buffer, footer, ReadOptions(), index_handle,
        &index_block, ioptions, true /* decompress */,
        true /*maybe_compressed*/, UncompressionDict::GetEmptyDict(),
        cache_options, kDisableGlobalSequenceNumber,
        0 /* read_amp_bytes_per_bit */, memory_allocator);

    if (!s.ok()) {
      return s;
    }

    // Note, failure to create prefix hash index does not need to be a
    // hard error. We can still fall back to the original binary search index.
    // So, Create will succeed regardless, from this point on.

    auto new_index_reader = new HashIndexReader(
        icomparator, std::move(index_block), ioptions.statistics,
        index_key_includes_seq, index_value_is_full);
    *index_reader = new_index_reader;

    // Get prefixes block
    BlockHandle prefixes_handle;
    s = FindMetaBlock(meta_index_iter, kHashIndexPrefixesBlock,
                      &prefixes_handle);
    if (!s.ok()) {
      // TODO: log error
      return Status::OK();
    }

    // Get index metadata block
    BlockHandle prefixes_meta_handle;
    s = FindMetaBlock(meta_index_iter, kHashIndexPrefixesMetadataBlock,
                      &prefixes_meta_handle);
    if (!s.ok()) {
      // TODO: log error
      return Status::OK();
    }

    // Read contents for the blocks
    BlockContents prefixes_contents;
    BlockFetcher prefixes_block_fetcher(
        file, prefetch_buffer, footer, ReadOptions(), prefixes_handle,
        &prefixes_contents, ioptions, true /*decompress*/,
        true /*maybe_compressed*/, UncompressionDict::GetEmptyDict(),
        cache_options, memory_allocator);
    s = prefixes_block_fetcher.ReadBlockContents();
    if (!s.ok()) {
      return s;
    }
    BlockContents prefixes_meta_contents;
    BlockFetcher prefixes_meta_block_fetcher(
        file, prefetch_buffer, footer, ReadOptions(), prefixes_meta_handle,
        &prefixes_meta_contents, ioptions, true /*decompress*/,
        true /*maybe_compressed*/, UncompressionDict::GetEmptyDict(),
        cache_options, memory_allocator);
    s = prefixes_meta_block_fetcher.ReadBlockContents();
    if (!s.ok()) {
      // TODO: log error
      return Status::OK();
    }

    BlockPrefixIndex* prefix_index = nullptr;
    s = BlockPrefixIndex::Create(hash_key_extractor, prefixes_contents.data,
                                 prefixes_meta_contents.data, &prefix_index);
    // TODO: log error
    if (s.ok()) {
      new_index_reader->prefix_index_.reset(prefix_index);
    }

    return Status::OK();
  }

  InternalIteratorBase<BlockHandle>* NewIterator(
      IndexBlockIter* iter = nullptr, bool total_order_seek = true,
      bool /*dont_care*/ = true) override {
    Statistics* kNullStats = nullptr;
    // We don't return pinned datat from index blocks, so no need
    // to set `block_contents_pinned`.
    return index_block_->NewIterator<IndexBlockIter>(
        icomparator_, icomparator_->user_comparator(), iter, kNullStats,
        total_order_seek, index_key_includes_seq_, index_value_is_full_,
        false /* block_contents_pinned */, prefix_index_.get());
  }

  size_t size() const override { return index_block_->size(); }
  size_t usable_size() const override { return index_block_->usable_size(); }

  size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    size_t usage = index_block_->ApproximateMemoryUsage();
    usage += prefixes_contents_.usable_size();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size((void*)this);
#else
    if (prefix_index_) {
      usage += prefix_index_->ApproximateMemoryUsage();
    }
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return usage;
  }

 private:
  HashIndexReader(const InternalKeyComparator* icomparator,
                  std::unique_ptr<Block>&& index_block, Statistics* stats,
                  const bool index_key_includes_seq,
                  const bool index_value_is_full)
      : IndexReader(icomparator, stats),
        index_block_(std::move(index_block)),
        index_key_includes_seq_(index_key_includes_seq),
        index_value_is_full_(index_value_is_full) {
    assert(index_block_ != nullptr);
  }

  ~HashIndexReader() override {}

  std::unique_ptr<Block> index_block_;
  std::unique_ptr<BlockPrefixIndex> prefix_index_;
  BlockContents prefixes_contents_;
  const bool index_key_includes_seq_;
  const bool index_value_is_full_;
};

// Helper function to setup the cache key's prefix for the Table.
void BlockBasedTable::SetupCacheKeyPrefix(Rep* rep, uint64_t file_size) {
  assert(kMaxCacheKeyPrefixSize >= 10);
  rep->cache_key_prefix_size = 0;
  rep->compressed_cache_key_prefix_size = 0;
  if (rep->table_options.block_cache != nullptr) {
    GenerateCachePrefix(rep->table_options.block_cache.get(), rep->file->file(),
                        &rep->cache_key_prefix[0], &rep->cache_key_prefix_size);
    // Create dummy offset of index reader which is beyond the file size.
    rep->dummy_index_reader_offset =
        file_size + rep->table_options.block_cache->NewId();
  }
  if (rep->table_options.persistent_cache != nullptr) {
    GenerateCachePrefix(/*cache=*/nullptr, rep->file->file(),
                        &rep->persistent_cache_key_prefix[0],
                        &rep->persistent_cache_key_prefix_size);
  }
  if (rep->table_options.block_cache_compressed != nullptr) {
    GenerateCachePrefix(rep->table_options.block_cache_compressed.get(),
                        rep->file->file(), &rep->compressed_cache_key_prefix[0],
                        &rep->compressed_cache_key_prefix_size);
  }
}

void BlockBasedTable::GenerateCachePrefix(Cache* cc,
    RandomAccessFile* file, char* buffer, size_t* size) {

  // generate an id from the file
  *size = file->GetUniqueId(buffer, kMaxCacheKeyPrefixSize);

  // If the prefix wasn't generated or was too long,
  // create one from the cache.
  if (cc && *size == 0) {
    char* end = EncodeVarint64(buffer, cc->NewId());
    *size = static_cast<size_t>(end - buffer);
  }
}

void BlockBasedTable::GenerateCachePrefix(Cache* cc,
    WritableFile* file, char* buffer, size_t* size) {

  // generate an id from the file
  *size = file->GetUniqueId(buffer, kMaxCacheKeyPrefixSize);

  // If the prefix wasn't generated or was too long,
  // create one from the cache.
  if (*size == 0) {
    char* end = EncodeVarint64(buffer, cc->NewId());
    *size = static_cast<size_t>(end - buffer);
  }
}

namespace {
// Return True if table_properties has `user_prop_name` has a `true` value
// or it doesn't contain this property (for backward compatible).
bool IsFeatureSupported(const TableProperties& table_properties,
                        const std::string& user_prop_name, Logger* info_log) {
  auto& props = table_properties.user_collected_properties;
  auto pos = props.find(user_prop_name);
  // Older version doesn't have this value set. Skip this check.
  if (pos != props.end()) {
    if (pos->second == kPropFalse) {
      return false;
    } else if (pos->second != kPropTrue) {
      ROCKS_LOG_WARN(info_log, "Property %s has invalidate value %s",
                     user_prop_name.c_str(), pos->second.c_str());
    }
  }
  return true;
}

// Caller has to ensure seqno is not nullptr.
Status GetGlobalSequenceNumber(const TableProperties& table_properties,
                               SequenceNumber largest_seqno,
                               SequenceNumber* seqno) {
  const auto& props = table_properties.user_collected_properties;
  const auto version_pos = props.find(ExternalSstFilePropertyNames::kVersion);
  const auto seqno_pos = props.find(ExternalSstFilePropertyNames::kGlobalSeqno);

  *seqno = kDisableGlobalSequenceNumber;
  if (version_pos == props.end()) {
    if (seqno_pos != props.end()) {
      std::array<char, 200> msg_buf;
      // This is not an external sst file, global_seqno is not supported.
      snprintf(
          msg_buf.data(), msg_buf.max_size(),
          "A non-external sst file have global seqno property with value %s",
          seqno_pos->second.c_str());
      return Status::Corruption(msg_buf.data());
    }
    return Status::OK();
  }

  uint32_t version = DecodeFixed32(version_pos->second.c_str());
  if (version < 2) {
    if (seqno_pos != props.end() || version != 1) {
      std::array<char, 200> msg_buf;
      // This is a v1 external sst file, global_seqno is not supported.
      snprintf(msg_buf.data(), msg_buf.max_size(),
               "An external sst file with version %u have global seqno "
               "property with value %s",
               version, seqno_pos->second.c_str());
      return Status::Corruption(msg_buf.data());
    }
    return Status::OK();
  }

  // Since we have a plan to deprecate global_seqno, we do not return failure
  // if seqno_pos == props.end(). We rely on version_pos to detect whether the
  // SST is external.
  SequenceNumber global_seqno(0);
  if (seqno_pos != props.end()) {
    global_seqno = DecodeFixed64(seqno_pos->second.c_str());
  }
  if (global_seqno != 0 && global_seqno != largest_seqno) {
    std::array<char, 200> msg_buf;
    snprintf(msg_buf.data(), msg_buf.max_size(),
             "An external sst file with version %u have global seqno property "
             "with value %s, while largest seqno in the file is %llu",
             version, seqno_pos->second.c_str(),
             static_cast<unsigned long long>(largest_seqno));
    return Status::Corruption(msg_buf.data());
  }
  global_seqno = largest_seqno;
  *seqno = largest_seqno;

  if (global_seqno > kMaxSequenceNumber) {
    std::array<char, 200> msg_buf;
    snprintf(msg_buf.data(), msg_buf.max_size(),
             "An external sst file with version %u have global seqno property "
             "with value %llu, which is greater than kMaxSequenceNumber",
             version, static_cast<unsigned long long>(global_seqno));
    return Status::Corruption(msg_buf.data());
  }

  return Status::OK();
}
}  // namespace

Slice BlockBasedTable::GetCacheKey(const char* cache_key_prefix,
                                   size_t cache_key_prefix_size,
                                   const BlockHandle& handle, char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end =
      EncodeVarint64(cache_key + cache_key_prefix_size, handle.offset());
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Status BlockBasedTable::Open(const ImmutableCFOptions& ioptions,
                             const EnvOptions& env_options,
                             const BlockBasedTableOptions& table_options,
                             const InternalKeyComparator& internal_comparator,
                             std::unique_ptr<RandomAccessFileReader>&& file,
                             uint64_t file_size,
                             std::unique_ptr<TableReader>* table_reader,
                             const SliceTransform* prefix_extractor,
                             const bool prefetch_index_and_filter_in_cache,
                             const bool skip_filters, const int level,
                             const bool immortal_table,
                             const SequenceNumber largest_seqno,
                             TailPrefetchStats* tail_prefetch_stats) {
  table_reader->reset();

  Status s;
  Footer footer;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;

  // prefetch both index and filters, down to all partitions
  const bool prefetch_all = prefetch_index_and_filter_in_cache || level == 0;
  const bool preload_all = !table_options.cache_index_and_filter_blocks;

  s = PrefetchTail(file.get(), file_size, tail_prefetch_stats, prefetch_all,
                   preload_all, &prefetch_buffer);

  // Read in the following order:
  //    1. Footer
  //    2. [metaindex block]
  //    3. [meta block: properties]
  //    4. [meta block: range deletion tombstone]
  //    5. [meta block: compression dictionary]
  //    6. [meta block: index]
  //    7. [meta block: filter]
  s = ReadFooterFromFile(file.get(), prefetch_buffer.get(), file_size, &footer,
                         kBlockBasedTableMagicNumber);
  if (!s.ok()) {
    return s;
  }
  if (!BlockBasedTableSupportedVersion(footer.version())) {
    return Status::Corruption(
        "Unknown Footer version. Maybe this file was created with newer "
        "version of RocksDB?");
  }

  // We've successfully read the footer. We are ready to serve requests.
  // Better not mutate rep_ after the creation. eg. internal_prefix_transform
  // raw pointer will be used to create HashIndexReader, whose reset may
  // access a dangling pointer.
  Rep* rep = new BlockBasedTable::Rep(ioptions, env_options, table_options,
                                      internal_comparator, skip_filters, level,
                                      immortal_table);
  rep->file = std::move(file);
  rep->footer = footer;
  rep->index_type = table_options.index_type;
  rep->hash_index_allow_collision = table_options.hash_index_allow_collision;
  // We need to wrap data with internal_prefix_transform to make sure it can
  // handle prefix correctly.
  rep->internal_prefix_transform.reset(
      new InternalKeySliceTransform(prefix_extractor));
  SetupCacheKeyPrefix(rep, file_size);
  std::unique_ptr<BlockBasedTable> new_table(new BlockBasedTable(rep));

  // page cache options
  rep->persistent_cache_options =
      PersistentCacheOptions(rep->table_options.persistent_cache,
                             std::string(rep->persistent_cache_key_prefix,
                                         rep->persistent_cache_key_prefix_size),
                             rep->ioptions.statistics);

  // Read metaindex
  std::unique_ptr<Block> meta;
  std::unique_ptr<InternalIterator> meta_iter;
  s = ReadMetaBlock(rep, prefetch_buffer.get(), &meta, &meta_iter);
  if (!s.ok()) {
    return s;
  }

  s = ReadPropertiesBlock(rep, prefetch_buffer.get(), meta_iter.get(),
                          largest_seqno);
  if (!s.ok()) {
    return s;
  }
  s = ReadRangeDelBlock(rep, prefetch_buffer.get(), meta_iter.get(),
                        internal_comparator);
  if (!s.ok()) {
    return s;
  }
  s = PrefetchIndexAndFilterBlocks(rep, prefetch_buffer.get(), meta_iter.get(),
                                   new_table.get(), prefix_extractor,
                                   prefetch_all, table_options, level,
                                   prefetch_index_and_filter_in_cache);

  if (s.ok()) {
    // Update tail prefetch stats
    assert(prefetch_buffer.get() != nullptr);
    if (tail_prefetch_stats != nullptr) {
      assert(prefetch_buffer->min_offset_read() < file_size);
      tail_prefetch_stats->RecordEffectiveSize(
          static_cast<size_t>(file_size) - prefetch_buffer->min_offset_read());
    }

    *table_reader = std::move(new_table);
  }

  return s;
}

Status BlockBasedTable::PrefetchTail(
    RandomAccessFileReader* file, uint64_t file_size,
    TailPrefetchStats* tail_prefetch_stats, const bool prefetch_all,
    const bool preload_all,
    std::unique_ptr<FilePrefetchBuffer>* prefetch_buffer) {
  size_t tail_prefetch_size = 0;
  if (tail_prefetch_stats != nullptr) {
    // Multiple threads may get a 0 (no history) when running in parallel,
    // but it will get cleared after the first of them finishes.
    tail_prefetch_size = tail_prefetch_stats->GetSuggestedPrefetchSize();
  }
  if (tail_prefetch_size == 0) {
    // Before read footer, readahead backwards to prefetch data. Do more
    // readahead if we're going to read index/filter.
    // TODO: This may incorrectly select small readahead in case partitioned
    // index/filter is enabled and top-level partition pinning is enabled.
    // That's because we need to issue readahead before we read the properties,
    // at which point we don't yet know the index type.
    tail_prefetch_size = prefetch_all || preload_all ? 512 * 1024 : 4 * 1024;
  }
  size_t prefetch_off;
  size_t prefetch_len;
  if (file_size < tail_prefetch_size) {
    prefetch_off = 0;
    prefetch_len = static_cast<size_t>(file_size);
  } else {
    prefetch_off = static_cast<size_t>(file_size - tail_prefetch_size);
    prefetch_len = tail_prefetch_size;
  }
  TEST_SYNC_POINT_CALLBACK("BlockBasedTable::Open::TailPrefetchLen",
                           &tail_prefetch_size);
  Status s;
  // TODO should not have this special logic in the future.
  if (!file->use_direct_io()) {
    prefetch_buffer->reset(new FilePrefetchBuffer(nullptr, 0, 0, false, true));
    s = file->Prefetch(prefetch_off, prefetch_len);
  } else {
    prefetch_buffer->reset(new FilePrefetchBuffer(nullptr, 0, 0, true, true));
    s = (*prefetch_buffer)->Prefetch(file, prefetch_off, prefetch_len);
  }
  return s;
}

Status VerifyChecksum(const ChecksumType type, const char* buf, size_t len,
                      uint32_t expected) {
  Status s;
  uint32_t actual = 0;
  switch (type) {
    case kNoChecksum:
      break;
    case kCRC32c:
      expected = crc32c::Unmask(expected);
      actual = crc32c::Value(buf, len);
      break;
    case kxxHash:
      actual = XXH32(buf, static_cast<int>(len), 0);
      break;
    case kxxHash64:
      actual = static_cast<uint32_t>(XXH64(buf, static_cast<int>(len), 0) &
                                     uint64_t{0xffffffff});
      break;
    default:
      s = Status::Corruption("unknown checksum type");
  }
  if (s.ok() && actual != expected) {
    s = Status::Corruption("properties block checksum mismatched");
  }
  return s;
}

Status BlockBasedTable::ReadPropertiesBlock(
    Rep* rep, FilePrefetchBuffer* prefetch_buffer, InternalIterator* meta_iter,
    const SequenceNumber largest_seqno) {
  bool found_properties_block = true;
  Status s;
  s = SeekToPropertiesBlock(meta_iter, &found_properties_block);

  if (!s.ok()) {
    ROCKS_LOG_WARN(rep->ioptions.info_log,
                   "Error when seeking to properties block from file: %s",
                   s.ToString().c_str());
  } else if (found_properties_block) {
    s = meta_iter->status();
    TableProperties* table_properties = nullptr;
    if (s.ok()) {
      s = ReadProperties(
          meta_iter->value(), rep->file.get(), prefetch_buffer, rep->footer,
          rep->ioptions, &table_properties, true /* verify_checksum */,
          nullptr /* ret_block_handle */, nullptr /* ret_block_contents */,
          false /* compression_type_missing */, nullptr /* memory_allocator */);
    }

    if (s.IsCorruption()) {
      // If this is an external SST file ingested with write_global_seqno set to
      // true, then we expect the checksum mismatch because checksum was written
      // by SstFileWriter, but its global seqno in the properties block may have
      // been changed during ingestion. In this case, we read the properties
      // block, copy it to a memory buffer, change the global seqno to its
      // original value, i.e. 0, and verify the checksum again.
      BlockHandle props_block_handle;
      CacheAllocationPtr tmp_buf;
      s = ReadProperties(meta_iter->value(), rep->file.get(), prefetch_buffer,
                         rep->footer, rep->ioptions, &table_properties,
                         false /* verify_checksum */, &props_block_handle,
                         &tmp_buf, false /* compression_type_missing */,
                         nullptr /* memory_allocator */);
      if (s.ok() && tmp_buf) {
        const auto seqno_pos_iter = table_properties->properties_offsets.find(
            ExternalSstFilePropertyNames::kGlobalSeqno);
        size_t block_size = props_block_handle.size();
        if (seqno_pos_iter != table_properties->properties_offsets.end()) {
          uint64_t global_seqno_offset = seqno_pos_iter->second;
          EncodeFixed64(
              tmp_buf.get() + global_seqno_offset - props_block_handle.offset(),
              0);
        }
        uint32_t value = DecodeFixed32(tmp_buf.get() + block_size + 1);
        s = rocksdb::VerifyChecksum(rep->footer.checksum(), tmp_buf.get(),
                                    block_size + 1, value);
      }
    }
    std::unique_ptr<TableProperties> props_guard;
    if (table_properties != nullptr) {
      props_guard.reset(table_properties);
    }

    if (!s.ok()) {
      ROCKS_LOG_WARN(rep->ioptions.info_log,
                     "Encountered error while reading data from properties "
                     "block %s",
                     s.ToString().c_str());
    } else {
      assert(table_properties != nullptr);
      rep->table_properties.reset(props_guard.release());
      rep->blocks_maybe_compressed = rep->table_properties->compression_name !=
                                     CompressionTypeToString(kNoCompression);
      rep->blocks_definitely_zstd_compressed =
          (rep->table_properties->compression_name ==
               CompressionTypeToString(kZSTD) ||
           rep->table_properties->compression_name ==
               CompressionTypeToString(kZSTDNotFinalCompression));
    }
  } else {
    ROCKS_LOG_ERROR(rep->ioptions.info_log,
                    "Cannot find Properties block from file.");
  }
#ifndef ROCKSDB_LITE
  if (rep->table_properties) {
    ParseSliceTransform(rep->table_properties->prefix_extractor_name,
                        &(rep->table_prefix_extractor));
  }
#endif  // ROCKSDB_LITE

  // Read the table properties, if provided.
  if (rep->table_properties) {
    rep->whole_key_filtering &=
        IsFeatureSupported(*(rep->table_properties),
                           BlockBasedTablePropertyNames::kWholeKeyFiltering,
                           rep->ioptions.info_log);
    rep->prefix_filtering &= IsFeatureSupported(
        *(rep->table_properties),
        BlockBasedTablePropertyNames::kPrefixFiltering, rep->ioptions.info_log);

    s = GetGlobalSequenceNumber(*(rep->table_properties), largest_seqno,
                                &(rep->global_seqno));
    if (!s.ok()) {
      ROCKS_LOG_ERROR(rep->ioptions.info_log, "%s", s.ToString().c_str());
    }
  }
  return s;
}

Status BlockBasedTable::ReadRangeDelBlock(
    Rep* rep, FilePrefetchBuffer* prefetch_buffer, InternalIterator* meta_iter,
    const InternalKeyComparator& internal_comparator) {
  Status s;
  bool found_range_del_block;
  BlockHandle range_del_handle;
  s = SeekToRangeDelBlock(meta_iter, &found_range_del_block, &range_del_handle);
  if (!s.ok()) {
    ROCKS_LOG_WARN(
        rep->ioptions.info_log,
        "Error when seeking to range delete tombstones block from file: %s",
        s.ToString().c_str());
  } else if (found_range_del_block && !range_del_handle.IsNull()) {
    ReadOptions read_options;
    std::unique_ptr<InternalIterator> iter(NewDataBlockIterator<DataBlockIter>(
        rep, read_options, range_del_handle, nullptr /* input_iter */,
        false /* is_index */, true /* key_includes_seq */,
        true /* index_key_is_full */, nullptr /* get_context */, Status(),
        prefetch_buffer));
    assert(iter != nullptr);
    s = iter->status();
    if (!s.ok()) {
      ROCKS_LOG_WARN(
          rep->ioptions.info_log,
          "Encountered error while reading data from range del block %s",
          s.ToString().c_str());
    } else {
      rep->fragmented_range_dels =
          std::make_shared<FragmentedRangeTombstoneList>(std::move(iter),
                                                         internal_comparator);
    }
  }
  return s;
}

Status BlockBasedTable::ReadCompressionDictBlock(
    Rep* rep, FilePrefetchBuffer* prefetch_buffer,
    std::unique_ptr<const BlockContents>* compression_dict_block) {
  assert(compression_dict_block != nullptr);
  Status s;
  if (!rep->compression_dict_handle.IsNull()) {
    std::unique_ptr<BlockContents> compression_dict_cont{new BlockContents()};
    PersistentCacheOptions cache_options;
    ReadOptions read_options;
    read_options.verify_checksums = true;
    BlockFetcher compression_block_fetcher(
        rep->file.get(), prefetch_buffer, rep->footer, read_options,
        rep->compression_dict_handle, compression_dict_cont.get(),
        rep->ioptions, false /* decompress */, false /*maybe_compressed*/,
        UncompressionDict::GetEmptyDict(), cache_options);
    s = compression_block_fetcher.ReadBlockContents();

    if (!s.ok()) {
      ROCKS_LOG_WARN(
          rep->ioptions.info_log,
          "Encountered error while reading data from compression dictionary "
          "block %s",
          s.ToString().c_str());
    } else {
      *compression_dict_block = std::move(compression_dict_cont);
    }
  }
  return s;
}

Status BlockBasedTable::PrefetchIndexAndFilterBlocks(
    Rep* rep, FilePrefetchBuffer* prefetch_buffer, InternalIterator* meta_iter,
    BlockBasedTable* new_table, const SliceTransform* prefix_extractor,
    bool prefetch_all, const BlockBasedTableOptions& table_options,
    const int level, const bool prefetch_index_and_filter_in_cache) {
  Status s;

  // Find filter handle and filter type
  if (rep->filter_policy) {
    for (auto filter_type :
         {Rep::FilterType::kFullFilter, Rep::FilterType::kPartitionedFilter,
          Rep::FilterType::kBlockFilter}) {
      std::string prefix;
      switch (filter_type) {
        case Rep::FilterType::kFullFilter:
          prefix = kFullFilterBlockPrefix;
          break;
        case Rep::FilterType::kPartitionedFilter:
          prefix = kPartitionedFilterBlockPrefix;
          break;
        case Rep::FilterType::kBlockFilter:
          prefix = kFilterBlockPrefix;
          break;
        default:
          assert(0);
      }
      std::string filter_block_key = prefix;
      filter_block_key.append(rep->filter_policy->Name());
      if (FindMetaBlock(meta_iter, filter_block_key, &rep->filter_handle)
              .ok()) {
        rep->filter_type = filter_type;
        break;
      }
    }
  }

  {
    // Find compression dictionary handle
    bool found_compression_dict;
    s = SeekToCompressionDictBlock(meta_iter, &found_compression_dict,
                                   &rep->compression_dict_handle);
  }

  bool need_upper_bound_check =
      PrefixExtractorChanged(rep->table_properties.get(), prefix_extractor);

  BlockBasedTableOptions::IndexType index_type = new_table->UpdateIndexType();
  // prefetch the first level of index
  const bool prefetch_index =
      prefetch_all ||
      (table_options.pin_top_level_index_and_filter &&
       index_type == BlockBasedTableOptions::kTwoLevelIndexSearch);
  // prefetch the first level of filter
  const bool prefetch_filter =
      prefetch_all || (table_options.pin_top_level_index_and_filter &&
                       rep->filter_type == Rep::FilterType::kPartitionedFilter);
  // Partition fitlers cannot be enabled without partition indexes
  assert(!prefetch_filter || prefetch_index);
  // pin both index and filters, down to all partitions
  const bool pin_all =
      rep->table_options.pin_l0_filter_and_index_blocks_in_cache && level == 0;
  // pin the first level of index
  const bool pin_index =
      pin_all || (table_options.pin_top_level_index_and_filter &&
                  index_type == BlockBasedTableOptions::kTwoLevelIndexSearch);
  // pin the first level of filter
  const bool pin_filter =
      pin_all || (table_options.pin_top_level_index_and_filter &&
                  rep->filter_type == Rep::FilterType::kPartitionedFilter);
  // pre-fetching of blocks is turned on
  // Will use block cache for meta-blocks access
  // Always prefetch index and filter for level 0
  // TODO(ajkr): also prefetch compression dictionary block
  if (table_options.cache_index_and_filter_blocks) {
    assert(table_options.block_cache != nullptr);
    if (prefetch_index) {
      // Hack: Call NewIndexIterator() to implicitly add index to the
      // block_cache
      CachableEntry<IndexReader> index_entry;
      // check prefix_extractor match only if hash based index is used
      bool disable_prefix_seek =
          rep->index_type == BlockBasedTableOptions::kHashSearch &&
          need_upper_bound_check;
      if (s.ok()) {
        std::unique_ptr<InternalIteratorBase<BlockHandle>> iter(
            new_table->NewIndexIterator(ReadOptions(), disable_prefix_seek,
                                        nullptr, &index_entry));
        s = iter->status();
      }
      if (s.ok()) {
        // This is the first call to NewIndexIterator() since we're in Open().
        // On success it should give us ownership of the `CachableEntry` by
        // populating `index_entry`.
        assert(index_entry.value != nullptr);
        if (prefetch_all) {
          index_entry.value->CacheDependencies(pin_all);
        }
        if (pin_index) {
          rep->index_entry = std::move(index_entry);
        } else {
          index_entry.Release(table_options.block_cache.get());
        }
      }
    }
    if (s.ok() && prefetch_filter) {
      // Hack: Call GetFilter() to implicitly add filter to the block_cache
      auto filter_entry =
          new_table->GetFilter(rep->table_prefix_extractor.get());
      if (filter_entry.value != nullptr && prefetch_all) {
        filter_entry.value->CacheDependencies(
            pin_all, rep->table_prefix_extractor.get());
      }
      // if pin_filter is true then save it in rep_->filter_entry; it will be
      // released in the destructor only, hence it will be pinned in the
      // cache while this reader is alive
      if (pin_filter) {
        rep->filter_entry = filter_entry;
      } else {
        filter_entry.Release(table_options.block_cache.get());
      }
    }
  } else {
    // If we don't use block cache for meta-block access, we'll pre-load these
    // blocks, which will kept in member variables in Rep and with a same life-
    // time as this table object.
    IndexReader* index_reader = nullptr;
    if (s.ok()) {
      s = new_table->CreateIndexReader(prefetch_buffer, &index_reader,
                                       meta_iter, level);
    }
    std::unique_ptr<const BlockContents> compression_dict_block;
    if (s.ok()) {
      rep->index_reader.reset(index_reader);
      // The partitions of partitioned index are always stored in cache. They
      // are hence follow the configuration for pin and prefetch regardless of
      // the value of cache_index_and_filter_blocks
      if (prefetch_index_and_filter_in_cache || level == 0) {
        rep->index_reader->CacheDependencies(pin_all);
      }

      // Set filter block
      if (rep->filter_policy) {
        const bool is_a_filter_partition = true;
        auto filter = new_table->ReadFilter(prefetch_buffer, rep->filter_handle,
                                            !is_a_filter_partition,
                                            rep->table_prefix_extractor.get());
        rep->filter.reset(filter);
        // Refer to the comment above about paritioned indexes always being
        // cached
        if (filter && (prefetch_index_and_filter_in_cache || level == 0)) {
          filter->CacheDependencies(pin_all, rep->table_prefix_extractor.get());
        }
      }
      s = ReadCompressionDictBlock(rep, prefetch_buffer,
                                   &compression_dict_block);
    } else {
      delete index_reader;
    }
    if (s.ok() && !rep->compression_dict_handle.IsNull()) {
      assert(compression_dict_block != nullptr);
      // TODO(ajkr): find a way to avoid the `compression_dict_block` data copy
      rep->uncompression_dict.reset(new UncompressionDict(
          compression_dict_block->data.ToString(),
          rep->blocks_definitely_zstd_compressed, rep->ioptions.statistics));
    }
  }
  return s;
}

void BlockBasedTable::SetupForCompaction() {
  switch (rep_->ioptions.access_hint_on_compaction_start) {
    case Options::NONE:
      break;
    case Options::NORMAL:
      rep_->file->file()->Hint(RandomAccessFile::NORMAL);
      break;
    case Options::SEQUENTIAL:
      rep_->file->file()->Hint(RandomAccessFile::SEQUENTIAL);
      break;
    case Options::WILLNEED:
      rep_->file->file()->Hint(RandomAccessFile::WILLNEED);
      break;
    default:
      assert(false);
  }
}

std::shared_ptr<const TableProperties> BlockBasedTable::GetTableProperties()
    const {
  return rep_->table_properties;
}

size_t BlockBasedTable::ApproximateMemoryUsage() const {
  size_t usage = 0;
  if (rep_->filter) {
    usage += rep_->filter->ApproximateMemoryUsage();
  }
  if (rep_->index_reader) {
    usage += rep_->index_reader->ApproximateMemoryUsage();
  }
  if (rep_->uncompression_dict) {
    usage += rep_->uncompression_dict->ApproximateMemoryUsage();
  }
  return usage;
}

// Load the meta-block from the file. On success, return the loaded meta block
// and its iterator.
Status BlockBasedTable::ReadMetaBlock(Rep* rep,
                                      FilePrefetchBuffer* prefetch_buffer,
                                      std::unique_ptr<Block>* meta_block,
                                      std::unique_ptr<InternalIterator>* iter) {
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  std::unique_ptr<Block> meta;
  Status s = ReadBlockFromFile(
      rep->file.get(), prefetch_buffer, rep->footer, ReadOptions(),
      rep->footer.metaindex_handle(), &meta, rep->ioptions,
      true /* decompress */, true /*maybe_compressed*/,
      UncompressionDict::GetEmptyDict(), rep->persistent_cache_options,
      kDisableGlobalSequenceNumber, 0 /* read_amp_bytes_per_bit */,
      GetMemoryAllocator(rep->table_options));

  if (!s.ok()) {
    ROCKS_LOG_ERROR(rep->ioptions.info_log,
                    "Encountered error while reading data from properties"
                    " block %s",
                    s.ToString().c_str());
    return s;
  }

  *meta_block = std::move(meta);
  // meta block uses bytewise comparator.
  iter->reset(meta_block->get()->NewIterator<DataBlockIter>(
      BytewiseComparator(), BytewiseComparator()));
  return Status::OK();
}

Status BlockBasedTable::GetDataBlockFromCache(
    const Slice& block_cache_key, const Slice& compressed_block_cache_key,
    Cache* block_cache, Cache* block_cache_compressed, Rep* rep,
    const ReadOptions& read_options,
    BlockBasedTable::CachableEntry<Block>* block,
    const UncompressionDict& uncompression_dict, size_t read_amp_bytes_per_bit,
    bool is_index, GetContext* get_context) {
  Status s;
  BlockContents* compressed_block = nullptr;
  Cache::Handle* block_cache_compressed_handle = nullptr;
  Statistics* statistics = rep->ioptions.statistics;

  // Lookup uncompressed cache first
  if (block_cache != nullptr) {
    block->cache_handle = GetEntryFromCache(
        block_cache, block_cache_key, rep->level,
        is_index ? BLOCK_CACHE_INDEX_MISS : BLOCK_CACHE_DATA_MISS,
        is_index ? BLOCK_CACHE_INDEX_HIT : BLOCK_CACHE_DATA_HIT,
        get_context
            ? (is_index ? &get_context->get_context_stats_.num_cache_index_miss
                        : &get_context->get_context_stats_.num_cache_data_miss)
            : nullptr,
        get_context
            ? (is_index ? &get_context->get_context_stats_.num_cache_index_hit
                        : &get_context->get_context_stats_.num_cache_data_hit)
            : nullptr,
        statistics, get_context);
    if (block->cache_handle != nullptr) {
      block->value =
          reinterpret_cast<Block*>(block_cache->Value(block->cache_handle));
      return s;
    }
  }

  // If not found, search from the compressed block cache.
  assert(block->cache_handle == nullptr && block->value == nullptr);

  if (block_cache_compressed == nullptr) {
    return s;
  }

  assert(!compressed_block_cache_key.empty());
  block_cache_compressed_handle =
      block_cache_compressed->Lookup(compressed_block_cache_key);
  // if we found in the compressed cache, then uncompress and insert into
  // uncompressed cache
  if (block_cache_compressed_handle == nullptr) {
    RecordTick(statistics, BLOCK_CACHE_COMPRESSED_MISS);
    return s;
  }

  // found compressed block
  RecordTick(statistics, BLOCK_CACHE_COMPRESSED_HIT);
  compressed_block = reinterpret_cast<BlockContents*>(
      block_cache_compressed->Value(block_cache_compressed_handle));
  CompressionType compression_type = compressed_block->get_compression_type();
  assert(compression_type != kNoCompression);

  // Retrieve the uncompressed contents into a new buffer
  BlockContents contents;
  UncompressionContext context(compression_type);
  UncompressionInfo info(context, uncompression_dict, compression_type);
  s = UncompressBlockContents(info, compressed_block->data.data(),
                              compressed_block->data.size(), &contents,
                              rep->table_options.format_version, rep->ioptions,
                              GetMemoryAllocator(rep->table_options));

  // Insert uncompressed block into block cache
  if (s.ok()) {
    block->value =
        new Block(std::move(contents), rep->get_global_seqno(is_index),
                  read_amp_bytes_per_bit,
                  statistics);  // uncompressed block
    if (block_cache != nullptr && block->value->own_bytes() &&
        read_options.fill_cache) {
      size_t charge = block->value->ApproximateMemoryUsage();
      s = block_cache->Insert(block_cache_key, block->value, charge,
                              &DeleteCachedEntry<Block>,
                              &(block->cache_handle));
#ifndef NDEBUG
      block_cache->TEST_mark_as_data_block(block_cache_key, charge);
#endif  // NDEBUG
      if (s.ok()) {
        if (get_context != nullptr) {
          get_context->get_context_stats_.num_cache_add++;
          get_context->get_context_stats_.num_cache_bytes_write += charge;
        } else {
          RecordTick(statistics, BLOCK_CACHE_ADD);
          RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, charge);
        }
        if (is_index) {
          if (get_context != nullptr) {
            get_context->get_context_stats_.num_cache_index_add++;
            get_context->get_context_stats_.num_cache_index_bytes_insert +=
                charge;
          } else {
            RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
            RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT, charge);
          }
        } else {
          if (get_context != nullptr) {
            get_context->get_context_stats_.num_cache_data_add++;
            get_context->get_context_stats_.num_cache_data_bytes_insert +=
                charge;
          } else {
            RecordTick(statistics, BLOCK_CACHE_DATA_ADD);
            RecordTick(statistics, BLOCK_CACHE_DATA_BYTES_INSERT, charge);
          }
        }
      } else {
        RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
        delete block->value;
        block->value = nullptr;
      }
    }
  }

  // Release hold on compressed cache entry
  block_cache_compressed->Release(block_cache_compressed_handle);
  return s;
}

Status BlockBasedTable::PutDataBlockToCache(
    const Slice& block_cache_key, const Slice& compressed_block_cache_key,
    Cache* block_cache, Cache* block_cache_compressed,
    const ReadOptions& /*read_options*/, const ImmutableCFOptions& ioptions,
    CachableEntry<Block>* cached_block, BlockContents* raw_block_contents,
    CompressionType raw_block_comp_type, uint32_t format_version,
    const UncompressionDict& uncompression_dict, SequenceNumber seq_no,
    size_t read_amp_bytes_per_bit, MemoryAllocator* memory_allocator,
    bool is_index, Cache::Priority priority, GetContext* get_context) {
  assert(raw_block_comp_type == kNoCompression ||
         block_cache_compressed != nullptr);

  Status s;
  // Retrieve the uncompressed contents into a new buffer
  BlockContents uncompressed_block_contents;
  Statistics* statistics = ioptions.statistics;
  if (raw_block_comp_type != kNoCompression) {
    UncompressionContext context(raw_block_comp_type);
    UncompressionInfo info(context, uncompression_dict, raw_block_comp_type);
    s = UncompressBlockContents(info, raw_block_contents->data.data(),
                                raw_block_contents->data.size(),
                                &uncompressed_block_contents, format_version,
                                ioptions, memory_allocator);
  }
  if (!s.ok()) {
    return s;
  }

  if (raw_block_comp_type != kNoCompression) {
    cached_block->value = new Block(std::move(uncompressed_block_contents),
                                    seq_no, read_amp_bytes_per_bit,
                                    statistics);  // uncompressed block
  } else {
    cached_block->value =
        new Block(std::move(*raw_block_contents), seq_no,
                  read_amp_bytes_per_bit, ioptions.statistics);
  }

  // Insert compressed block into compressed block cache.
  // Release the hold on the compressed cache entry immediately.
  if (block_cache_compressed != nullptr &&
      raw_block_comp_type != kNoCompression && raw_block_contents != nullptr &&
      raw_block_contents->own_bytes()) {
#ifndef NDEBUG
    assert(raw_block_contents->is_raw_block);
#endif  // NDEBUG

    // We cannot directly put raw_block_contents because this could point to
    // an object in the stack.
    BlockContents* block_cont_for_comp_cache =
        new BlockContents(std::move(*raw_block_contents));
    s = block_cache_compressed->Insert(
        compressed_block_cache_key, block_cont_for_comp_cache,
        block_cont_for_comp_cache->ApproximateMemoryUsage(),
        &DeleteCachedEntry<BlockContents>);
    if (s.ok()) {
      // Avoid the following code to delete this cached block.
      RecordTick(statistics, BLOCK_CACHE_COMPRESSED_ADD);
    } else {
      RecordTick(statistics, BLOCK_CACHE_COMPRESSED_ADD_FAILURES);
      delete block_cont_for_comp_cache;
    }
  }

  // insert into uncompressed block cache
  if (block_cache != nullptr && cached_block->value->own_bytes()) {
    size_t charge = cached_block->value->ApproximateMemoryUsage();
    s = block_cache->Insert(block_cache_key, cached_block->value, charge,
                            &DeleteCachedEntry<Block>,
                            &(cached_block->cache_handle), priority);
#ifndef NDEBUG
    block_cache->TEST_mark_as_data_block(block_cache_key, charge);
#endif  // NDEBUG
    if (s.ok()) {
      assert(cached_block->cache_handle != nullptr);
      if (get_context != nullptr) {
        get_context->get_context_stats_.num_cache_add++;
        get_context->get_context_stats_.num_cache_bytes_write += charge;
      } else {
        RecordTick(statistics, BLOCK_CACHE_ADD);
        RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, charge);
      }
      if (is_index) {
        if (get_context != nullptr) {
          get_context->get_context_stats_.num_cache_index_add++;
          get_context->get_context_stats_.num_cache_index_bytes_insert +=
              charge;
        } else {
          RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
          RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT, charge);
        }
      } else {
        if (get_context != nullptr) {
          get_context->get_context_stats_.num_cache_data_add++;
          get_context->get_context_stats_.num_cache_data_bytes_insert += charge;
        } else {
          RecordTick(statistics, BLOCK_CACHE_DATA_ADD);
          RecordTick(statistics, BLOCK_CACHE_DATA_BYTES_INSERT, charge);
        }
      }
      assert(reinterpret_cast<Block*>(block_cache->Value(
                 cached_block->cache_handle)) == cached_block->value);
    } else {
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
      delete cached_block->value;
      cached_block->value = nullptr;
    }
  }

  return s;
}

FilterBlockReader* BlockBasedTable::ReadFilter(
    FilePrefetchBuffer* prefetch_buffer, const BlockHandle& filter_handle,
    const bool is_a_filter_partition,
    const SliceTransform* prefix_extractor) const {
  auto& rep = rep_;
  // TODO: We might want to unify with ReadBlockFromFile() if we start
  // requiring checksum verification in Table::Open.
  if (rep->filter_type == Rep::FilterType::kNoFilter) {
    return nullptr;
  }
  BlockContents block;

  BlockFetcher block_fetcher(
      rep->file.get(), prefetch_buffer, rep->footer, ReadOptions(),
      filter_handle, &block, rep->ioptions, false /* decompress */,
      false /*maybe_compressed*/, UncompressionDict::GetEmptyDict(),
      rep->persistent_cache_options, GetMemoryAllocator(rep->table_options));
  Status s = block_fetcher.ReadBlockContents();

  if (!s.ok()) {
    // Error reading the block
    return nullptr;
  }

  assert(rep->filter_policy);

  auto filter_type = rep->filter_type;
  if (rep->filter_type == Rep::FilterType::kPartitionedFilter &&
      is_a_filter_partition) {
    filter_type = Rep::FilterType::kFullFilter;
  }

  switch (filter_type) {
    case Rep::FilterType::kPartitionedFilter: {
      return new PartitionedFilterBlockReader(
          rep->prefix_filtering ? prefix_extractor : nullptr,
          rep->whole_key_filtering, std::move(block), nullptr,
          rep->ioptions.statistics, rep->internal_comparator, this,
          rep_->table_properties == nullptr ||
              rep_->table_properties->index_key_is_user_key == 0,
          rep_->table_properties == nullptr ||
              rep_->table_properties->index_value_is_delta_encoded == 0);
    }

    case Rep::FilterType::kBlockFilter:
      return new BlockBasedFilterBlockReader(
          rep->prefix_filtering ? prefix_extractor : nullptr,
          rep->table_options, rep->whole_key_filtering, std::move(block),
          rep->ioptions.statistics);

    case Rep::FilterType::kFullFilter: {
      auto filter_bits_reader =
          rep->filter_policy->GetFilterBitsReader(block.data);
      assert(filter_bits_reader != nullptr);
      return new FullFilterBlockReader(
          rep->prefix_filtering ? prefix_extractor : nullptr,
          rep->whole_key_filtering, std::move(block), filter_bits_reader,
          rep->ioptions.statistics);
    }

    default:
      // filter_type is either kNoFilter (exited the function at the first if),
      // or it must be covered in this switch block
      assert(false);
      return nullptr;
  }
}

BlockBasedTable::CachableEntry<FilterBlockReader> BlockBasedTable::GetFilter(
    const SliceTransform* prefix_extractor, FilePrefetchBuffer* prefetch_buffer,
    bool no_io, GetContext* get_context) const {
  const BlockHandle& filter_blk_handle = rep_->filter_handle;
  const bool is_a_filter_partition = true;
  return GetFilter(prefetch_buffer, filter_blk_handle, !is_a_filter_partition,
                   no_io, get_context, prefix_extractor);
}

BlockBasedTable::CachableEntry<FilterBlockReader> BlockBasedTable::GetFilter(
    FilePrefetchBuffer* prefetch_buffer, const BlockHandle& filter_blk_handle,
    const bool is_a_filter_partition, bool no_io, GetContext* get_context,
    const SliceTransform* prefix_extractor) const {
  // If cache_index_and_filter_blocks is false, filter should be pre-populated.
  // We will return rep_->filter anyway. rep_->filter can be nullptr if filter
  // read fails at Open() time. We don't want to reload again since it will
  // most probably fail again.
  if (!is_a_filter_partition &&
      !rep_->table_options.cache_index_and_filter_blocks) {
    return {rep_->filter.get(), nullptr /* cache handle */};
  }

  Cache* block_cache = rep_->table_options.block_cache.get();
  if (rep_->filter_policy == nullptr /* do not use filter */ ||
      block_cache == nullptr /* no block cache at all */) {
    return {nullptr /* filter */, nullptr /* cache handle */};
  }

  if (!is_a_filter_partition && rep_->filter_entry.IsSet()) {
    return rep_->filter_entry;
  }

  PERF_TIMER_GUARD(read_filter_block_nanos);

  // Fetching from the cache
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                         filter_blk_handle, cache_key);

  Statistics* statistics = rep_->ioptions.statistics;
  auto cache_handle = GetEntryFromCache(
      block_cache, key, rep_->level,
      BLOCK_CACHE_FILTER_MISS, BLOCK_CACHE_FILTER_HIT,
      get_context ? &get_context->get_context_stats_.num_cache_filter_miss
                  : nullptr,
      get_context ? &get_context->get_context_stats_.num_cache_filter_hit
                  : nullptr,
      statistics, get_context);

  FilterBlockReader* filter = nullptr;
  if (cache_handle != nullptr) {
    PERF_COUNTER_ADD(block_cache_filter_hit_count, 1);
    filter = reinterpret_cast<FilterBlockReader*>(
        block_cache->Value(cache_handle));
  } else if (no_io) {
    // Do not invoke any io.
    return CachableEntry<FilterBlockReader>();
  } else {
    filter = ReadFilter(prefetch_buffer, filter_blk_handle,
                        is_a_filter_partition, prefix_extractor);
    if (filter != nullptr) {
      size_t usage = filter->ApproximateMemoryUsage();
      Status s = block_cache->Insert(
          key, filter, usage, &DeleteCachedFilterEntry, &cache_handle,
          rep_->table_options.cache_index_and_filter_blocks_with_high_priority
              ? Cache::Priority::HIGH
              : Cache::Priority::LOW);
      if (s.ok()) {
        PERF_COUNTER_ADD(filter_block_read_count, 1);
        if (get_context != nullptr) {
          get_context->get_context_stats_.num_cache_add++;
          get_context->get_context_stats_.num_cache_bytes_write += usage;
          get_context->get_context_stats_.num_cache_filter_add++;
          get_context->get_context_stats_.num_cache_filter_bytes_insert +=
              usage;
        } else {
          RecordTick(statistics, BLOCK_CACHE_ADD);
          RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, usage);
          RecordTick(statistics, BLOCK_CACHE_FILTER_ADD);
          RecordTick(statistics, BLOCK_CACHE_FILTER_BYTES_INSERT, usage);
        }
      } else {
        RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
        delete filter;
        return CachableEntry<FilterBlockReader>();
      }
    }
  }

  return { filter, cache_handle };
}

BlockBasedTable::CachableEntry<UncompressionDict>
BlockBasedTable::GetUncompressionDict(Rep* rep,
                                      FilePrefetchBuffer* prefetch_buffer,
                                      bool no_io, GetContext* get_context) {
  if (!rep->table_options.cache_index_and_filter_blocks) {
    // block cache is either disabled or not used for meta-blocks. In either
    // case, BlockBasedTableReader is the owner of the uncompression dictionary.
    return {rep->uncompression_dict.get(), nullptr /* cache handle */};
  }
  if (rep->compression_dict_handle.IsNull()) {
    return {nullptr, nullptr};
  }
  char cache_key_buf[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto cache_key =
      GetCacheKey(rep->cache_key_prefix, rep->cache_key_prefix_size,
                  rep->compression_dict_handle, cache_key_buf);
  auto cache_handle = GetEntryFromCache(
      rep->table_options.block_cache.get(), cache_key, rep->level,
      BLOCK_CACHE_COMPRESSION_DICT_MISS, BLOCK_CACHE_COMPRESSION_DICT_HIT,
      get_context
          ? &get_context->get_context_stats_.num_cache_compression_dict_miss
          : nullptr,
      get_context
          ? &get_context->get_context_stats_.num_cache_compression_dict_hit
          : nullptr,
      rep->ioptions.statistics, get_context);
  UncompressionDict* dict = nullptr;
  if (cache_handle != nullptr) {
    dict = reinterpret_cast<UncompressionDict*>(
        rep->table_options.block_cache->Value(cache_handle));
  } else if (no_io) {
    // Do not invoke any io.
  } else {
    std::unique_ptr<const BlockContents> compression_dict_block;
    Status s =
        ReadCompressionDictBlock(rep, prefetch_buffer, &compression_dict_block);
    size_t usage = 0;
    if (s.ok()) {
      assert(compression_dict_block != nullptr);
      // TODO(ajkr): find a way to avoid the `compression_dict_block` data copy
      dict = new UncompressionDict(compression_dict_block->data.ToString(),
                                   rep->blocks_definitely_zstd_compressed,
                                   rep->ioptions.statistics);
      usage = dict->ApproximateMemoryUsage();
      s = rep->table_options.block_cache->Insert(
          cache_key, dict, usage, &DeleteCachedUncompressionDictEntry,
          &cache_handle,
          rep->table_options.cache_index_and_filter_blocks_with_high_priority
              ? Cache::Priority::HIGH
              : Cache::Priority::LOW);
    }
    if (s.ok()) {
      PERF_COUNTER_ADD(compression_dict_block_read_count, 1);
      if (get_context != nullptr) {
        get_context->get_context_stats_.num_cache_add++;
        get_context->get_context_stats_.num_cache_bytes_write += usage;
        get_context->get_context_stats_.num_cache_compression_dict_add++;
        get_context->get_context_stats_
            .num_cache_compression_dict_bytes_insert += usage;
      } else {
        RecordTick(rep->ioptions.statistics, BLOCK_CACHE_ADD);
        RecordTick(rep->ioptions.statistics, BLOCK_CACHE_BYTES_WRITE, usage);
        RecordTick(rep->ioptions.statistics, BLOCK_CACHE_COMPRESSION_DICT_ADD);
        RecordTick(rep->ioptions.statistics,
                   BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT, usage);
      }
    } else {
      // There should be no way to get here if block cache insertion succeeded.
      // Though it is still possible something failed earlier.
      RecordTick(rep->ioptions.statistics, BLOCK_CACHE_ADD_FAILURES);
      delete dict;
      dict = nullptr;
      assert(cache_handle == nullptr);
    }
  }
  return {dict, cache_handle};
}

// disable_prefix_seek should be set to true when prefix_extractor found in SST
// differs from the one in mutable_cf_options and index type is HashBasedIndex
InternalIteratorBase<BlockHandle>* BlockBasedTable::NewIndexIterator(
    const ReadOptions& read_options, bool disable_prefix_seek,
    IndexBlockIter* input_iter, CachableEntry<IndexReader>* index_entry,
    GetContext* get_context) {
  // index reader has already been pre-populated.
  if (rep_->index_reader) {
    // We don't return pinned datat from index blocks, so no need
    // to set `block_contents_pinned`.
    return rep_->index_reader->NewIterator(
        input_iter, read_options.total_order_seek || disable_prefix_seek,
        read_options.fill_cache);
  }
  // we have a pinned index block
  if (rep_->index_entry.IsSet()) {
    // We don't return pinned datat from index blocks, so no need
    // to set `block_contents_pinned`.
    return rep_->index_entry.value->NewIterator(
        input_iter, read_options.total_order_seek || disable_prefix_seek,
        read_options.fill_cache);
  }

  PERF_TIMER_GUARD(read_index_block_nanos);

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  Cache* block_cache = rep_->table_options.block_cache.get();
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key =
      GetCacheKeyFromOffset(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                            rep_->dummy_index_reader_offset, cache_key);
  Statistics* statistics = rep_->ioptions.statistics;
  auto cache_handle = GetEntryFromCache(
      block_cache, key, rep_->level,
      BLOCK_CACHE_INDEX_MISS, BLOCK_CACHE_INDEX_HIT,
      get_context ? &get_context->get_context_stats_.num_cache_index_miss
                  : nullptr,
      get_context ? &get_context->get_context_stats_.num_cache_index_hit
                  : nullptr,
      statistics, get_context);

  if (cache_handle == nullptr && no_io) {
    if (input_iter != nullptr) {
      input_iter->Invalidate(Status::Incomplete("no blocking io"));
      return input_iter;
    } else {
      return NewErrorInternalIterator<BlockHandle>(
          Status::Incomplete("no blocking io"));
    }
  }

  IndexReader* index_reader = nullptr;
  if (cache_handle != nullptr) {
    PERF_COUNTER_ADD(block_cache_index_hit_count, 1);
    index_reader =
        reinterpret_cast<IndexReader*>(block_cache->Value(cache_handle));
  } else {
    // Create index reader and put it in the cache.
    Status s;
    TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread2:2");
    s = CreateIndexReader(nullptr /* prefetch_buffer */, &index_reader);
    TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread1:1");
    TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread2:3");
    TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread1:4");
    size_t charge = 0;
    if (s.ok()) {
      assert(index_reader != nullptr);
      charge = index_reader->ApproximateMemoryUsage();
      s = block_cache->Insert(
          key, index_reader, charge, &DeleteCachedIndexEntry, &cache_handle,
          rep_->table_options.cache_index_and_filter_blocks_with_high_priority
              ? Cache::Priority::HIGH
              : Cache::Priority::LOW);
    }

    if (s.ok()) {
      if (get_context != nullptr) {
        get_context->get_context_stats_.num_cache_add++;
        get_context->get_context_stats_.num_cache_bytes_write += charge;
      } else {
        RecordTick(statistics, BLOCK_CACHE_ADD);
        RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, charge);
      }
      PERF_COUNTER_ADD(index_block_read_count, 1);
      RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
      RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT, charge);
    } else {
      if (index_reader != nullptr) {
        delete index_reader;
      }
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
      // make sure if something goes wrong, index_reader shall remain intact.
      if (input_iter != nullptr) {
        input_iter->Invalidate(s);
        return input_iter;
      } else {
        return NewErrorInternalIterator<BlockHandle>(s);
      }
    }

  }

  assert(cache_handle);
  // We don't return pinned datat from index blocks, so no need
  // to set `block_contents_pinned`.
  auto* iter = index_reader->NewIterator(
      input_iter, read_options.total_order_seek || disable_prefix_seek);

  // the caller would like to take ownership of the index block
  // don't call RegisterCleanup() in this case, the caller will take care of it
  if (index_entry != nullptr) {
    *index_entry = {index_reader, cache_handle};
  } else {
    iter->RegisterCleanup(&ReleaseCachedEntry, block_cache, cache_handle);
  }

  return iter;
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
template <typename TBlockIter>
TBlockIter* BlockBasedTable::NewDataBlockIterator(
    Rep* rep, const ReadOptions& ro, const BlockHandle& handle,
    TBlockIter* input_iter, bool is_index, bool key_includes_seq,
    bool index_key_is_full, GetContext* get_context, Status s,
    FilePrefetchBuffer* prefetch_buffer) {
  PERF_TIMER_GUARD(new_table_block_iter_nanos);

  Cache* block_cache = rep->table_options.block_cache.get();
  CachableEntry<Block> block;
  TBlockIter* iter;
  {
    const bool no_io = (ro.read_tier == kBlockCacheTier);
    auto uncompression_dict_storage =
        GetUncompressionDict(rep, prefetch_buffer, no_io, get_context);
    const UncompressionDict& uncompression_dict =
        uncompression_dict_storage.value == nullptr
            ? UncompressionDict::GetEmptyDict()
            : *uncompression_dict_storage.value;
    if (s.ok()) {
      s = MaybeReadBlockAndLoadToCache(prefetch_buffer, rep, ro, handle,
                                       uncompression_dict, &block, is_index,
                                       get_context);
    }

    if (input_iter != nullptr) {
      iter = input_iter;
    } else {
      iter = new TBlockIter;
    }
    // Didn't get any data from block caches.
    if (s.ok() && block.value == nullptr) {
      if (no_io) {
        // Could not read from block_cache and can't do IO
        iter->Invalidate(Status::Incomplete("no blocking io"));
        return iter;
      }
      std::unique_ptr<Block> block_value;
      {
        StopWatch sw(rep->ioptions.env, rep->ioptions.statistics,
                     READ_BLOCK_GET_MICROS);
        s = ReadBlockFromFile(
            rep->file.get(), prefetch_buffer, rep->footer, ro, handle,
            &block_value, rep->ioptions,
            rep->blocks_maybe_compressed /*do_decompress*/,
            rep->blocks_maybe_compressed, uncompression_dict,
            rep->persistent_cache_options,
            is_index ? kDisableGlobalSequenceNumber : rep->global_seqno,
            rep->table_options.read_amp_bytes_per_bit,
            GetMemoryAllocator(rep->table_options));
      }
      if (s.ok()) {
        block.value = block_value.release();
      }
    }
    // TODO(ajkr): also pin compression dictionary block when
    // `pin_l0_filter_and_index_blocks_in_cache == true`.
    uncompression_dict_storage.Release(block_cache);
  }

  if (s.ok()) {
    assert(block.value != nullptr);
    const bool kTotalOrderSeek = true;
    // Block contents are pinned and it is still pinned after the iterator
    // is destroyed as long as cleanup functions are moved to another object,
    // when:
    // 1. block cache handle is set to be released in cleanup function, or
    // 2. it's pointing to immortal source. If own_bytes is true then we are
    //    not reading data from the original source, whether immortal or not.
    //    Otherwise, the block is pinned iff the source is immortal.
    bool block_contents_pinned =
        (block.cache_handle != nullptr ||
         (!block.value->own_bytes() && rep->immortal_table));
    iter = block.value->NewIterator<TBlockIter>(
        &rep->internal_comparator, rep->internal_comparator.user_comparator(),
        iter, rep->ioptions.statistics, kTotalOrderSeek, key_includes_seq,
        index_key_is_full, block_contents_pinned);
    if (block.cache_handle != nullptr) {
      iter->RegisterCleanup(&ReleaseCachedEntry, block_cache,
                            block.cache_handle);
    } else {
      if (!ro.fill_cache && rep->cache_key_prefix_size != 0) {
        // insert a dummy record to block cache to track the memory usage
        Cache::Handle* cache_handle;
        // There are two other types of cache keys: 1) SST cache key added in
        // `MaybeReadBlockAndLoadToCache` 2) dummy cache key added in
        // `write_buffer_manager`. Use longer prefix (41 bytes) to differentiate
        // from SST cache key(31 bytes), and use non-zero prefix to
        // differentiate from `write_buffer_manager`
        const size_t kExtraCacheKeyPrefix = kMaxVarint64Length * 4 + 1;
        char cache_key[kExtraCacheKeyPrefix + kMaxVarint64Length];
        // Prefix: use rep->cache_key_prefix padded by 0s
        memset(cache_key, 0, kExtraCacheKeyPrefix + kMaxVarint64Length);
        assert(rep->cache_key_prefix_size != 0);
        assert(rep->cache_key_prefix_size <= kExtraCacheKeyPrefix);
        memcpy(cache_key, rep->cache_key_prefix, rep->cache_key_prefix_size);
        char* end = EncodeVarint64(cache_key + kExtraCacheKeyPrefix,
                                   next_cache_key_id_++);
        assert(end - cache_key <=
               static_cast<int>(kExtraCacheKeyPrefix + kMaxVarint64Length));
        Slice unique_key =
            Slice(cache_key, static_cast<size_t>(end - cache_key));
        s = block_cache->Insert(unique_key, nullptr,
                                block.value->ApproximateMemoryUsage(), nullptr,
                                &cache_handle);
        if (s.ok()) {
          if (cache_handle != nullptr) {
            iter->RegisterCleanup(&ForceReleaseCachedEntry, block_cache,
                                  cache_handle);
          }
        }
      }
      iter->RegisterCleanup(&DeleteHeldResource<Block>, block.value, nullptr);
    }
  } else {
    assert(block.value == nullptr);
    iter->Invalidate(s);
  }
  return iter;
}

Status BlockBasedTable::MaybeReadBlockAndLoadToCache(
    FilePrefetchBuffer* prefetch_buffer, Rep* rep, const ReadOptions& ro,
    const BlockHandle& handle, const UncompressionDict& uncompression_dict,
    CachableEntry<Block>* block_entry, bool is_index, GetContext* get_context) {
  assert(block_entry != nullptr);
  const bool no_io = (ro.read_tier == kBlockCacheTier);
  Cache* block_cache = rep->table_options.block_cache.get();

  // No point to cache compressed blocks if it never goes away
  Cache* block_cache_compressed =
      rep->immortal_table ? nullptr
                          : rep->table_options.block_cache_compressed.get();

  // First, try to get the block from the cache
  //
  // If either block cache is enabled, we'll try to read from it.
  Status s;
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  char compressed_cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  Slice key /* key to the block cache */;
  Slice ckey /* key to the compressed block cache */;
  if (block_cache != nullptr || block_cache_compressed != nullptr) {
    // create key for block cache
    if (block_cache != nullptr) {
      key = GetCacheKey(rep->cache_key_prefix, rep->cache_key_prefix_size,
                        handle, cache_key);
    }

    if (block_cache_compressed != nullptr) {
      ckey = GetCacheKey(rep->compressed_cache_key_prefix,
                         rep->compressed_cache_key_prefix_size, handle,
                         compressed_cache_key);
    }

    s = GetDataBlockFromCache(key, ckey, block_cache, block_cache_compressed,
                              rep, ro, block_entry, uncompression_dict,
                              rep->table_options.read_amp_bytes_per_bit,
                              is_index, get_context);

    // Can't find the block from the cache. If I/O is allowed, read from the
    // file.
    if (block_entry->value == nullptr && !no_io && ro.fill_cache) {
      Statistics* statistics = rep->ioptions.statistics;
      bool do_decompress =
          block_cache_compressed == nullptr && rep->blocks_maybe_compressed;
      CompressionType raw_block_comp_type;
      BlockContents raw_block_contents;
      {
        StopWatch sw(rep->ioptions.env, statistics, READ_BLOCK_GET_MICROS);
        BlockFetcher block_fetcher(
            rep->file.get(), prefetch_buffer, rep->footer, ro, handle,
            &raw_block_contents, rep->ioptions,
            do_decompress /* do uncompress */, rep->blocks_maybe_compressed,
            uncompression_dict, rep->persistent_cache_options,
            GetMemoryAllocator(rep->table_options),
            GetMemoryAllocatorForCompressedBlock(rep->table_options));
        s = block_fetcher.ReadBlockContents();
        raw_block_comp_type = block_fetcher.get_compression_type();
      }

      if (s.ok()) {
        SequenceNumber seq_no = rep->get_global_seqno(is_index);
        // If filling cache is allowed and a cache is configured, try to put the
        // block to the cache.
        s = PutDataBlockToCache(
            key, ckey, block_cache, block_cache_compressed, ro, rep->ioptions,
            block_entry, &raw_block_contents, raw_block_comp_type,
            rep->table_options.format_version, uncompression_dict, seq_no,
            rep->table_options.read_amp_bytes_per_bit,
            GetMemoryAllocator(rep->table_options), is_index,
            is_index && rep->table_options
                            .cache_index_and_filter_blocks_with_high_priority
                ? Cache::Priority::HIGH
                : Cache::Priority::LOW,
            get_context);
      }
    }
  }
  assert(s.ok() || block_entry->value == nullptr);
  return s;
}

BlockBasedTable::PartitionedIndexIteratorState::PartitionedIndexIteratorState(
    BlockBasedTable* table,
    std::unordered_map<uint64_t, CachableEntry<Block>>* block_map,
    bool index_key_includes_seq, bool index_key_is_full)
    : table_(table),
      block_map_(block_map),
      index_key_includes_seq_(index_key_includes_seq),
      index_key_is_full_(index_key_is_full) {}

template <class TBlockIter, typename TValue>
const size_t BlockBasedTableIterator<TBlockIter, TValue>::kMaxReadaheadSize =
    256 * 1024;

InternalIteratorBase<BlockHandle>*
BlockBasedTable::PartitionedIndexIteratorState::NewSecondaryIterator(
    const BlockHandle& handle) {
  // Return a block iterator on the index partition
  auto rep = table_->get_rep();
  auto block = block_map_->find(handle.offset());
  // This is a possible scenario since block cache might not have had space
  // for the partition
  if (block != block_map_->end()) {
    PERF_COUNTER_ADD(block_cache_hit_count, 1);
    RecordTick(rep->ioptions.statistics, BLOCK_CACHE_INDEX_HIT);
    RecordTick(rep->ioptions.statistics, BLOCK_CACHE_HIT);
    Cache* block_cache = rep->table_options.block_cache.get();
    assert(block_cache);
    RecordTick(rep->ioptions.statistics, BLOCK_CACHE_BYTES_READ,
               block_cache->GetUsage(block->second.cache_handle));
    Statistics* kNullStats = nullptr;
    // We don't return pinned datat from index blocks, so no need
    // to set `block_contents_pinned`.
    return block->second.value->NewIterator<IndexBlockIter>(
        &rep->internal_comparator, rep->internal_comparator.user_comparator(),
        nullptr, kNullStats, true, index_key_includes_seq_, index_key_is_full_);
  }
  // Create an empty iterator
  return new IndexBlockIter();
}

// This will be broken if the user specifies an unusual implementation
// of Options.comparator, or if the user specifies an unusual
// definition of prefixes in BlockBasedTableOptions.filter_policy.
// In particular, we require the following three properties:
//
// 1) key.starts_with(prefix(key))
// 2) Compare(prefix(key), key) <= 0.
// 3) If Compare(key1, key2) <= 0, then Compare(prefix(key1), prefix(key2)) <= 0
//
// Otherwise, this method guarantees no I/O will be incurred.
//
// REQUIRES: this method shouldn't be called while the DB lock is held.
bool BlockBasedTable::PrefixMayMatch(
    const Slice& internal_key, const ReadOptions& read_options,
    const SliceTransform* options_prefix_extractor,
    const bool need_upper_bound_check) {
  if (!rep_->filter_policy) {
    return true;
  }

  const SliceTransform* prefix_extractor;

  if (rep_->table_prefix_extractor == nullptr) {
    if (need_upper_bound_check) {
      return true;
    }
    prefix_extractor = options_prefix_extractor;
  } else {
    prefix_extractor = rep_->table_prefix_extractor.get();
  }
  auto user_key = ExtractUserKey(internal_key);
  if (!prefix_extractor->InDomain(user_key)) {
    return true;
  }

  bool may_match = true;
  Status s;

  // First, try check with full filter
  auto filter_entry = GetFilter(prefix_extractor);
  FilterBlockReader* filter = filter_entry.value;
  bool filter_checked = true;
  if (filter != nullptr) {
    if (!filter->IsBlockBased()) {
      const Slice* const const_ikey_ptr = &internal_key;
      may_match = filter->RangeMayExist(
          read_options.iterate_upper_bound, user_key, prefix_extractor,
          rep_->internal_comparator.user_comparator(), const_ikey_ptr,
          &filter_checked, need_upper_bound_check);
    } else {
      // if prefix_extractor changed for block based filter, skip filter
      if (need_upper_bound_check) {
        if (!rep_->filter_entry.IsSet()) {
          filter_entry.Release(rep_->table_options.block_cache.get());
        }
        return true;
      }
      auto prefix = prefix_extractor->Transform(user_key);
      InternalKey internal_key_prefix(prefix, kMaxSequenceNumber, kTypeValue);
      auto internal_prefix = internal_key_prefix.Encode();

      // To prevent any io operation in this method, we set `read_tier` to make
      // sure we always read index or filter only when they have already been
      // loaded to memory.
      ReadOptions no_io_read_options;
      no_io_read_options.read_tier = kBlockCacheTier;

      // Then, try find it within each block
      // we already know prefix_extractor and prefix_extractor_name must match
      // because `CheckPrefixMayMatch` first checks `check_filter_ == true`
      std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter(
          NewIndexIterator(no_io_read_options,
                           /* need_upper_bound_check */ false));
      iiter->Seek(internal_prefix);

      if (!iiter->Valid()) {
        // we're past end of file
        // if it's incomplete, it means that we avoided I/O
        // and we're not really sure that we're past the end
        // of the file
        may_match = iiter->status().IsIncomplete();
      } else if ((rep_->table_properties &&
                          rep_->table_properties->index_key_is_user_key
                      ? iiter->key()
                      : ExtractUserKey(iiter->key()))
                     .starts_with(ExtractUserKey(internal_prefix))) {
        // we need to check for this subtle case because our only
        // guarantee is that "the key is a string >= last key in that data
        // block" according to the doc/table_format.txt spec.
        //
        // Suppose iiter->key() starts with the desired prefix; it is not
        // necessarily the case that the corresponding data block will
        // contain the prefix, since iiter->key() need not be in the
        // block.  However, the next data block may contain the prefix, so
        // we return true to play it safe.
        may_match = true;
      } else if (filter->IsBlockBased()) {
        // iiter->key() does NOT start with the desired prefix.  Because
        // Seek() finds the first key that is >= the seek target, this
        // means that iiter->key() > prefix.  Thus, any data blocks coming
        // after the data block corresponding to iiter->key() cannot
        // possibly contain the key.  Thus, the corresponding data block
        // is the only on could potentially contain the prefix.
        BlockHandle handle = iiter->value();
        may_match =
            filter->PrefixMayMatch(prefix, prefix_extractor, handle.offset());
      }
    }
  }

  if (filter_checked) {
    Statistics* statistics = rep_->ioptions.statistics;
    RecordTick(statistics, BLOOM_FILTER_PREFIX_CHECKED);
    if (!may_match) {
      RecordTick(statistics, BLOOM_FILTER_PREFIX_USEFUL);
    }
  }

  // if rep_->filter_entry is not set, we should call Release(); otherwise
  // don't call, in this case we have a local copy in rep_->filter_entry,
  // it's pinned to the cache and will be released in the destructor
  if (!rep_->filter_entry.IsSet()) {
    filter_entry.Release(rep_->table_options.block_cache.get());
  }
  return may_match;
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::Seek(const Slice& target) {
  is_out_of_bound_ = false;
  if (!CheckPrefixMayMatch(target)) {
    ResetDataIter();
    return;
  }

  SavePrevIndexValue();

  index_iter_->Seek(target);

  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }

  InitDataBlock();

  block_iter_.Seek(target);

  FindKeyForward();
  assert(
      !block_iter_.Valid() ||
      (key_includes_seq_ && icomp_.Compare(target, block_iter_.key()) <= 0) ||
      (!key_includes_seq_ &&
       icomp_.user_comparator()->Compare(ExtractUserKey(target),
                                         block_iter_.key()) <= 0));
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::SeekForPrev(
    const Slice& target) {
  is_out_of_bound_ = false;
  if (!CheckPrefixMayMatch(target)) {
    ResetDataIter();
    return;
  }

  SavePrevIndexValue();

  // Call Seek() rather than SeekForPrev() in the index block, because the
  // target data block will likely to contain the position for `target`, the
  // same as Seek(), rather than than before.
  // For example, if we have three data blocks, each containing two keys:
  //   [2, 4]  [6, 8] [10, 12]
  //  (the keys in the index block would be [4, 8, 12])
  // and the user calls SeekForPrev(7), we need to go to the second block,
  // just like if they call Seek(7).
  // The only case where the block is difference is when they seek to a position
  // in the boundary. For example, if they SeekForPrev(5), we should go to the
  // first block, rather than the second. However, we don't have the information
  // to distinguish the two unless we read the second block. In this case, we'll
  // end up with reading two blocks.
  index_iter_->Seek(target);

  if (!index_iter_->Valid()) {
    index_iter_->SeekToLast();
    if (!index_iter_->Valid()) {
      ResetDataIter();
      block_iter_points_to_real_block_ = false;
      return;
    }
  }

  InitDataBlock();

  block_iter_.SeekForPrev(target);

  FindKeyBackward();
  assert(!block_iter_.Valid() ||
         icomp_.Compare(target, block_iter_.key()) >= 0);
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::SeekToFirst() {
  is_out_of_bound_ = false;
  SavePrevIndexValue();
  index_iter_->SeekToFirst();
  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }
  InitDataBlock();
  block_iter_.SeekToFirst();
  FindKeyForward();
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::SeekToLast() {
  is_out_of_bound_ = false;
  SavePrevIndexValue();
  index_iter_->SeekToLast();
  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }
  InitDataBlock();
  block_iter_.SeekToLast();
  FindKeyBackward();
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::Next() {
  assert(block_iter_points_to_real_block_);
  block_iter_.Next();
  FindKeyForward();
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::Prev() {
  assert(block_iter_points_to_real_block_);
  block_iter_.Prev();
  FindKeyBackward();
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::InitDataBlock() {
  BlockHandle data_block_handle = index_iter_->value();
  if (!block_iter_points_to_real_block_ ||
      data_block_handle.offset() != prev_index_value_.offset() ||
      // if previous attempt of reading the block missed cache, try again
      block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetDataIter();
    }
    auto* rep = table_->get_rep();

    // Automatically prefetch additional data when a range scan (iterator) does
    // more than 2 sequential IOs. This is enabled only for user reads and when
    // ReadOptions.readahead_size is 0.
    if (!for_compaction_ && read_options_.readahead_size == 0) {
      num_file_reads_++;
      if (num_file_reads_ > 2) {
        if (!rep->file->use_direct_io() &&
            (data_block_handle.offset() +
                 static_cast<size_t>(data_block_handle.size()) +
                 kBlockTrailerSize >
             readahead_limit_)) {
          // Buffered I/O
          // Discarding the return status of Prefetch calls intentionally, as we
          // can fallback to reading from disk if Prefetch fails.
          rep->file->Prefetch(data_block_handle.offset(), readahead_size_);
          readahead_limit_ =
              static_cast<size_t>(data_block_handle.offset() + readahead_size_);
          // Keep exponentially increasing readahead size until
          // kMaxReadaheadSize.
          readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_ * 2);
        } else if (rep->file->use_direct_io() && !prefetch_buffer_) {
          // Direct I/O
          // Let FilePrefetchBuffer take care of the readahead.
          prefetch_buffer_.reset(new FilePrefetchBuffer(
              rep->file.get(), kInitReadaheadSize, kMaxReadaheadSize));
        }
      }
    }

    Status s;
    BlockBasedTable::NewDataBlockIterator<TBlockIter>(
        rep, read_options_, data_block_handle, &block_iter_, is_index_,
        key_includes_seq_, index_key_is_full_,
        /* get_context */ nullptr, s, prefetch_buffer_.get());
    block_iter_points_to_real_block_ = true;
  }
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::FindKeyForward() {
  assert(!is_out_of_bound_);
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  while (!block_iter_.Valid()) {
    if (!block_iter_.status().ok()) {
      return;
    }
    ResetDataIter();
    // We used to check the current index key for upperbound.
    // It will only save a data reading for a small percentage of use cases,
    // so for code simplicity, we removed it. We can add it back if there is a
    // significnat performance regression.
    index_iter_->Next();

    if (index_iter_->Valid()) {
      InitDataBlock();
      block_iter_.SeekToFirst();
    } else {
      return;
    }
  }

  // Check upper bound on the current key
  bool reached_upper_bound =
      (read_options_.iterate_upper_bound != nullptr &&
       block_iter_points_to_real_block_ && block_iter_.Valid() &&
       icomp_.user_comparator()->Compare(ExtractUserKey(block_iter_.key()),
                                         *read_options_.iterate_upper_bound) >=
           0);
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTable::BlockEntryIteratorState::KeyReachedUpperBound",
      &reached_upper_bound);
  if (reached_upper_bound) {
    is_out_of_bound_ = true;
    return;
  }
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::FindKeyBackward() {
  assert(!is_out_of_bound_);
  while (!block_iter_.Valid()) {
    if (!block_iter_.status().ok()) {
      return;
    }

    ResetDataIter();
    index_iter_->Prev();

    if (index_iter_->Valid()) {
      InitDataBlock();
      block_iter_.SeekToLast();
    } else {
      return;
    }
  }

  // We could have check lower bound here too, but we opt not to do it for
  // code simplicity.
}

InternalIterator* BlockBasedTable::NewIterator(
    const ReadOptions& read_options, const SliceTransform* prefix_extractor,
    Arena* arena, bool skip_filters, bool for_compaction) {
  bool need_upper_bound_check =
      PrefixExtractorChanged(rep_->table_properties.get(), prefix_extractor);
  const bool kIsNotIndex = false;
  if (arena == nullptr) {
    return new BlockBasedTableIterator<DataBlockIter>(
        this, read_options, rep_->internal_comparator,
        NewIndexIterator(
            read_options,
            need_upper_bound_check &&
                rep_->index_type == BlockBasedTableOptions::kHashSearch),
        !skip_filters && !read_options.total_order_seek &&
            prefix_extractor != nullptr,
        need_upper_bound_check, prefix_extractor, kIsNotIndex,
        true /*key_includes_seq*/, for_compaction);
  } else {
    auto* mem =
        arena->AllocateAligned(sizeof(BlockBasedTableIterator<DataBlockIter>));
    return new (mem) BlockBasedTableIterator<DataBlockIter>(
        this, read_options, rep_->internal_comparator,
        NewIndexIterator(read_options, need_upper_bound_check),
        !skip_filters && !read_options.total_order_seek &&
            prefix_extractor != nullptr,
        need_upper_bound_check, prefix_extractor, kIsNotIndex,
        true /*key_includes_seq*/, for_compaction);
  }
}

FragmentedRangeTombstoneIterator* BlockBasedTable::NewRangeTombstoneIterator(
    const ReadOptions& read_options) {
  if (rep_->fragmented_range_dels == nullptr) {
    return nullptr;
  }
  SequenceNumber snapshot = kMaxSequenceNumber;
  if (read_options.snapshot != nullptr) {
    snapshot = read_options.snapshot->GetSequenceNumber();
  }
  return new FragmentedRangeTombstoneIterator(
      rep_->fragmented_range_dels, rep_->internal_comparator, snapshot);
}

bool BlockBasedTable::FullFilterKeyMayMatch(
    const ReadOptions& read_options, FilterBlockReader* filter,
    const Slice& internal_key, const bool no_io,
    const SliceTransform* prefix_extractor) const {
  if (filter == nullptr || filter->IsBlockBased()) {
    return true;
  }
  Slice user_key = ExtractUserKey(internal_key);
  const Slice* const const_ikey_ptr = &internal_key;
  bool may_match = true;
  if (filter->whole_key_filtering()) {
    may_match = filter->KeyMayMatch(user_key, prefix_extractor, kNotValid,
                                    no_io, const_ikey_ptr);
  } else if (!read_options.total_order_seek && prefix_extractor &&
             rep_->table_properties->prefix_extractor_name.compare(
                 prefix_extractor->Name()) == 0 &&
             prefix_extractor->InDomain(user_key) &&
             !filter->PrefixMayMatch(prefix_extractor->Transform(user_key),
                                     prefix_extractor, kNotValid, false,
                                     const_ikey_ptr)) {
    may_match = false;
  }
  if (may_match) {
    RecordTick(rep_->ioptions.statistics, BLOOM_FILTER_FULL_POSITIVE);
    PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_positive, 1, rep_->level);
  }
  return may_match;
}

Status BlockBasedTable::Get(const ReadOptions& read_options, const Slice& key,
                            GetContext* get_context,
                            const SliceTransform* prefix_extractor,
                            bool skip_filters) {
  assert(key.size() >= 8);  // key must be internal key
  Status s;
  const bool no_io = read_options.read_tier == kBlockCacheTier;
  CachableEntry<FilterBlockReader> filter_entry;
  if (!skip_filters) {
    filter_entry =
        GetFilter(prefix_extractor, /*prefetch_buffer*/ nullptr,
                  read_options.read_tier == kBlockCacheTier, get_context);
  }
  FilterBlockReader* filter = filter_entry.value;

  // First check the full filter
  // If full filter not useful, Then go into each block
  if (!FullFilterKeyMayMatch(read_options, filter, key, no_io,
                             prefix_extractor)) {
    RecordTick(rep_->ioptions.statistics, BLOOM_FILTER_USEFUL);
    PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, rep_->level);
  } else {
    IndexBlockIter iiter_on_stack;
    // if prefix_extractor found in block differs from options, disable
    // BlockPrefixIndex. Only do this check when index_type is kHashSearch.
    bool need_upper_bound_check = false;
    if (rep_->index_type == BlockBasedTableOptions::kHashSearch) {
      need_upper_bound_check = PrefixExtractorChanged(
          rep_->table_properties.get(), prefix_extractor);
    }
    auto iiter =
        NewIndexIterator(read_options, need_upper_bound_check, &iiter_on_stack,
                         /* index_entry */ nullptr, get_context);
    std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter_unique_ptr;
    if (iiter != &iiter_on_stack) {
      iiter_unique_ptr.reset(iiter);
    }

    bool matched = false;  // if such user key mathced a key in SST
    bool done = false;
    for (iiter->Seek(key); iiter->Valid() && !done; iiter->Next()) {
      BlockHandle handle = iiter->value();

      bool not_exist_in_filter =
          filter != nullptr && filter->IsBlockBased() == true &&
          !filter->KeyMayMatch(ExtractUserKey(key), prefix_extractor,
                               handle.offset(), no_io);

      if (not_exist_in_filter) {
        // Not found
        // TODO: think about interaction with Merge. If a user key cannot
        // cross one data block, we should be fine.
        RecordTick(rep_->ioptions.statistics, BLOOM_FILTER_USEFUL);
        PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, rep_->level);
        break;
      } else {
        DataBlockIter biter;
        NewDataBlockIterator<DataBlockIter>(
            rep_, read_options, iiter->value(), &biter, false,
            true /* key_includes_seq */, true /* index_key_is_full */,
            get_context);

        if (read_options.read_tier == kBlockCacheTier &&
            biter.status().IsIncomplete()) {
          // couldn't get block from block_cache
          // Update Saver.state to Found because we are only looking for
          // whether we can guarantee the key is not there when "no_io" is set
          get_context->MarkKeyMayExist();
          break;
        }
        if (!biter.status().ok()) {
          s = biter.status();
          break;
        }

        bool may_exist = biter.SeekForGet(key);
        if (!may_exist) {
          // HashSeek cannot find the key this block and the the iter is not
          // the end of the block, i.e. cannot be in the following blocks
          // either. In this case, the seek_key cannot be found, so we break
          // from the top level for-loop.
          break;
        }

        // Call the *saver function on each entry/block until it returns false
        for (; biter.Valid(); biter.Next()) {
          ParsedInternalKey parsed_key;
          if (!ParseInternalKey(biter.key(), &parsed_key)) {
            s = Status::Corruption(Slice());
          }

          if (!get_context->SaveValue(
                  parsed_key, biter.value(), &matched,
                  biter.IsValuePinned() ? &biter : nullptr)) {
            done = true;
            break;
          }
        }
        s = biter.status();
      }
      if (done) {
        // Avoid the extra Next which is expensive in two-level indexes
        break;
      }
    }
    if (matched && filter != nullptr && !filter->IsBlockBased()) {
      RecordTick(rep_->ioptions.statistics, BLOOM_FILTER_FULL_TRUE_POSITIVE);
      PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_true_positive, 1,
                                rep_->level);
    }
    if (s.ok()) {
      s = iiter->status();
    }
  }

  // if rep_->filter_entry is not set, we should call Release(); otherwise
  // don't call, in this case we have a local copy in rep_->filter_entry,
  // it's pinned to the cache and will be released in the destructor
  if (!rep_->filter_entry.IsSet()) {
    filter_entry.Release(rep_->table_options.block_cache.get());
  }
  return s;
}

Status BlockBasedTable::Prefetch(const Slice* const begin,
                                 const Slice* const end) {
  auto& comparator = rep_->internal_comparator;
  auto user_comparator = comparator.user_comparator();
  // pre-condition
  if (begin && end && comparator.Compare(*begin, *end) > 0) {
    return Status::InvalidArgument(*begin, *end);
  }

  IndexBlockIter iiter_on_stack;
  auto iiter = NewIndexIterator(ReadOptions(), false, &iiter_on_stack);
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr =
        std::unique_ptr<InternalIteratorBase<BlockHandle>>(iiter);
  }

  if (!iiter->status().ok()) {
    // error opening index iterator
    return iiter->status();
  }

  // indicates if we are on the last page that need to be pre-fetched
  bool prefetching_boundary_page = false;

  for (begin ? iiter->Seek(*begin) : iiter->SeekToFirst(); iiter->Valid();
       iiter->Next()) {
    BlockHandle block_handle = iiter->value();
    const bool is_user_key = rep_->table_properties &&
                             rep_->table_properties->index_key_is_user_key > 0;
    if (end &&
        ((!is_user_key && comparator.Compare(iiter->key(), *end) >= 0) ||
         (is_user_key &&
          user_comparator->Compare(iiter->key(), ExtractUserKey(*end)) >= 0))) {
      if (prefetching_boundary_page) {
        break;
      }

      // The index entry represents the last key in the data block.
      // We should load this page into memory as well, but no more
      prefetching_boundary_page = true;
    }

    // Load the block specified by the block_handle into the block cache
    DataBlockIter biter;
    NewDataBlockIterator<DataBlockIter>(rep_, ReadOptions(), block_handle,
                                        &biter);

    if (!biter.status().ok()) {
      // there was an unexpected error while pre-fetching
      return biter.status();
    }
  }

  return Status::OK();
}

Status BlockBasedTable::VerifyChecksum() {
  Status s;
  // Check Meta blocks
  std::unique_ptr<Block> meta;
  std::unique_ptr<InternalIterator> meta_iter;
  s = ReadMetaBlock(rep_, nullptr /* prefetch buffer */, &meta, &meta_iter);
  if (s.ok()) {
    s = VerifyChecksumInBlocks(meta_iter.get());
    if (!s.ok()) {
      return s;
    }
  } else {
    return s;
  }
  // Check Data blocks
  IndexBlockIter iiter_on_stack;
  InternalIteratorBase<BlockHandle>* iiter =
      NewIndexIterator(ReadOptions(), false, &iiter_on_stack);
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr =
        std::unique_ptr<InternalIteratorBase<BlockHandle>>(iiter);
  }
  if (!iiter->status().ok()) {
    // error opening index iterator
    return iiter->status();
  }
  s = VerifyChecksumInBlocks(iiter);
  return s;
}

Status BlockBasedTable::VerifyChecksumInBlocks(
    InternalIteratorBase<BlockHandle>* index_iter) {
  Status s;
  for (index_iter->SeekToFirst(); index_iter->Valid(); index_iter->Next()) {
    s = index_iter->status();
    if (!s.ok()) {
      break;
    }
    BlockHandle handle = index_iter->value();
    BlockContents contents;
    BlockFetcher block_fetcher(
        rep_->file.get(), nullptr /* prefetch buffer */, rep_->footer,
        ReadOptions(), handle, &contents, rep_->ioptions,
        false /* decompress */, false /*maybe_compressed*/,
        UncompressionDict::GetEmptyDict(), rep_->persistent_cache_options);
    s = block_fetcher.ReadBlockContents();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status BlockBasedTable::VerifyChecksumInBlocks(
    InternalIteratorBase<Slice>* index_iter) {
  Status s;
  for (index_iter->SeekToFirst(); index_iter->Valid(); index_iter->Next()) {
    s = index_iter->status();
    if (!s.ok()) {
      break;
    }
    BlockHandle handle;
    Slice input = index_iter->value();
    s = handle.DecodeFrom(&input);
    BlockContents contents;
    BlockFetcher block_fetcher(
        rep_->file.get(), nullptr /* prefetch buffer */, rep_->footer,
        ReadOptions(), handle, &contents, rep_->ioptions,
        false /* decompress */, false /*maybe_compressed*/,
        UncompressionDict::GetEmptyDict(), rep_->persistent_cache_options);
    s = block_fetcher.ReadBlockContents();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

bool BlockBasedTable::TEST_KeyInCache(const ReadOptions& options,
                                      const Slice& key) {
  std::unique_ptr<InternalIteratorBase<BlockHandle>> iiter(
      NewIndexIterator(options));
  iiter->Seek(key);
  assert(iiter->Valid());
  CachableEntry<Block> block;

  BlockHandle handle = iiter->value();
  Cache* block_cache = rep_->table_options.block_cache.get();
  assert(block_cache != nullptr);

  char cache_key_storage[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  Slice cache_key =
      GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size, handle,
                  cache_key_storage);
  Slice ckey;

  Status s;
  if (!rep_->compression_dict_handle.IsNull()) {
    std::unique_ptr<const BlockContents> compression_dict_block;
    s = ReadCompressionDictBlock(rep_, nullptr /* prefetch_buffer */,
                                 &compression_dict_block);
    if (s.ok()) {
      assert(compression_dict_block != nullptr);
      UncompressionDict uncompression_dict(
          compression_dict_block->data.ToString(),
          rep_->blocks_definitely_zstd_compressed);
      s = GetDataBlockFromCache(cache_key, ckey, block_cache, nullptr, rep_,
                                options, &block, uncompression_dict,
                                0 /* read_amp_bytes_per_bit */);
    }
  } else {
    s = GetDataBlockFromCache(
        cache_key, ckey, block_cache, nullptr, rep_, options, &block,
        UncompressionDict::GetEmptyDict(), 0 /* read_amp_bytes_per_bit */);
  }
  assert(s.ok());
  bool in_cache = block.value != nullptr;
  if (in_cache) {
    ReleaseCachedEntry(block_cache, block.cache_handle);
  }
  return in_cache;
}

BlockBasedTableOptions::IndexType BlockBasedTable::UpdateIndexType() {
  // Some old version of block-based tables don't have index type present in
  // table properties. If that's the case we can safely use the kBinarySearch.
  BlockBasedTableOptions::IndexType index_type_on_file =
      BlockBasedTableOptions::kBinarySearch;
  if (rep_->table_properties) {
    auto& props = rep_->table_properties->user_collected_properties;
    auto pos = props.find(BlockBasedTablePropertyNames::kIndexType);
    if (pos != props.end()) {
      index_type_on_file = static_cast<BlockBasedTableOptions::IndexType>(
          DecodeFixed32(pos->second.c_str()));
      // update index_type with the true type
      rep_->index_type = index_type_on_file;
    }
  }
  return index_type_on_file;
}

// REQUIRES: The following fields of rep_ should have already been populated:
//  1. file
//  2. index_handle,
//  3. options
//  4. internal_comparator
//  5. index_type
Status BlockBasedTable::CreateIndexReader(
    FilePrefetchBuffer* prefetch_buffer, IndexReader** index_reader,
    InternalIterator* preloaded_meta_index_iter, int level) {
  auto index_type_on_file = UpdateIndexType();

  auto file = rep_->file.get();
  const InternalKeyComparator* icomparator = &rep_->internal_comparator;
  const Footer& footer = rep_->footer;

  // kHashSearch requires non-empty prefix_extractor but bypass checking
  // prefix_extractor here since we have no access to MutableCFOptions.
  // Add need_upper_bound_check flag in  BlockBasedTable::NewIndexIterator.
  // If prefix_extractor does not match prefix_extractor_name from table
  // properties, turn off Hash Index by setting total_order_seek to true

  switch (index_type_on_file) {
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      return PartitionIndexReader::Create(
          this, file, prefetch_buffer, footer, footer.index_handle(),
          rep_->ioptions, icomparator, index_reader,
          rep_->persistent_cache_options, level,
          rep_->table_properties == nullptr ||
              rep_->table_properties->index_key_is_user_key == 0,
          rep_->table_properties == nullptr ||
              rep_->table_properties->index_value_is_delta_encoded == 0,
          GetMemoryAllocator(rep_->table_options));
    }
    case BlockBasedTableOptions::kBinarySearch: {
      return BinarySearchIndexReader::Create(
          file, prefetch_buffer, footer, footer.index_handle(), rep_->ioptions,
          icomparator, index_reader, rep_->persistent_cache_options,
          rep_->table_properties == nullptr ||
              rep_->table_properties->index_key_is_user_key == 0,
          rep_->table_properties == nullptr ||
              rep_->table_properties->index_value_is_delta_encoded == 0,
          GetMemoryAllocator(rep_->table_options));
    }
    case BlockBasedTableOptions::kHashSearch: {
      std::unique_ptr<Block> meta_guard;
      std::unique_ptr<InternalIterator> meta_iter_guard;
      auto meta_index_iter = preloaded_meta_index_iter;
      if (meta_index_iter == nullptr) {
        auto s =
            ReadMetaBlock(rep_, prefetch_buffer, &meta_guard, &meta_iter_guard);
        if (!s.ok()) {
          // we simply fall back to binary search in case there is any
          // problem with prefix hash index loading.
          ROCKS_LOG_WARN(rep_->ioptions.info_log,
                         "Unable to read the metaindex block."
                         " Fall back to binary search index.");
          return BinarySearchIndexReader::Create(
              file, prefetch_buffer, footer, footer.index_handle(),
              rep_->ioptions, icomparator, index_reader,
              rep_->persistent_cache_options,
              rep_->table_properties == nullptr ||
                  rep_->table_properties->index_key_is_user_key == 0,
              rep_->table_properties == nullptr ||
                  rep_->table_properties->index_value_is_delta_encoded == 0,
              GetMemoryAllocator(rep_->table_options));
        }
        meta_index_iter = meta_iter_guard.get();
      }

      return HashIndexReader::Create(
          rep_->internal_prefix_transform.get(), footer, file, prefetch_buffer,
          rep_->ioptions, icomparator, footer.index_handle(), meta_index_iter,
          index_reader, rep_->hash_index_allow_collision,
          rep_->persistent_cache_options,
          rep_->table_properties == nullptr ||
              rep_->table_properties->index_key_is_user_key == 0,
          rep_->table_properties == nullptr ||
              rep_->table_properties->index_value_is_delta_encoded == 0,
          GetMemoryAllocator(rep_->table_options));
    }
    default: {
      std::string error_message =
          "Unrecognized index type: " + ToString(index_type_on_file);
      return Status::InvalidArgument(error_message.c_str());
    }
  }
}

uint64_t BlockBasedTable::ApproximateOffsetOf(const Slice& key) {
  std::unique_ptr<InternalIteratorBase<BlockHandle>> index_iter(
      NewIndexIterator(ReadOptions()));

  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle = index_iter->value();
    result = handle.offset();
  } else {
    // key is past the last key in the file. If table_properties is not
    // available, approximate the offset by returning the offset of the
    // metaindex block (which is right near the end of the file).
    result = 0;
    if (rep_->table_properties) {
      result = rep_->table_properties->data_size;
    }
    // table_properties is not present in the table.
    if (result == 0) {
      result = rep_->footer.metaindex_handle().offset();
    }
  }
  return result;
}

bool BlockBasedTable::TEST_filter_block_preloaded() const {
  return rep_->filter != nullptr;
}

bool BlockBasedTable::TEST_index_reader_preloaded() const {
  return rep_->index_reader != nullptr;
}

Status BlockBasedTable::GetKVPairsFromDataBlocks(
    std::vector<KVPairBlock>* kv_pair_blocks) {
  std::unique_ptr<InternalIteratorBase<BlockHandle>> blockhandles_iter(
      NewIndexIterator(ReadOptions()));

  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    // Cannot read Index Block
    return s;
  }

  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();

    if (!s.ok()) {
      break;
    }

    std::unique_ptr<InternalIterator> datablock_iter;
    datablock_iter.reset(NewDataBlockIterator<DataBlockIter>(
        rep_, ReadOptions(), blockhandles_iter->value()));
    s = datablock_iter->status();

    if (!s.ok()) {
      // Error reading the block - Skipped
      continue;
    }

    KVPairBlock kv_pair_block;
    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        // Error reading the block - Skipped
        break;
      }
      const Slice& key = datablock_iter->key();
      const Slice& value = datablock_iter->value();
      std::string key_copy = std::string(key.data(), key.size());
      std::string value_copy = std::string(value.data(), value.size());

      kv_pair_block.push_back(
          std::make_pair(std::move(key_copy), std::move(value_copy)));
    }
    kv_pair_blocks->push_back(std::move(kv_pair_block));
  }
  return Status::OK();
}

Status BlockBasedTable::DumpTable(WritableFile* out_file,
                                  const SliceTransform* prefix_extractor) {
  // Output Footer
  out_file->Append(
      "Footer Details:\n"
      "--------------------------------------\n"
      "  ");
  out_file->Append(rep_->footer.ToString().c_str());
  out_file->Append("\n");

  // Output MetaIndex
  out_file->Append(
      "Metaindex Details:\n"
      "--------------------------------------\n");
  std::unique_ptr<Block> meta;
  std::unique_ptr<InternalIterator> meta_iter;
  Status s =
      ReadMetaBlock(rep_, nullptr /* prefetch_buffer */, &meta, &meta_iter);
  if (s.ok()) {
    for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
      s = meta_iter->status();
      if (!s.ok()) {
        return s;
      }
      if (meta_iter->key() == rocksdb::kPropertiesBlock) {
        out_file->Append("  Properties block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      } else if (meta_iter->key() == rocksdb::kCompressionDictBlock) {
        out_file->Append("  Compression dictionary block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      } else if (strstr(meta_iter->key().ToString().c_str(),
                        "filter.rocksdb.") != nullptr) {
        out_file->Append("  Filter block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      } else if (meta_iter->key() == rocksdb::kRangeDelBlock) {
        out_file->Append("  Range deletion block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      }
    }
    out_file->Append("\n");
  } else {
    return s;
  }

  // Output TableProperties
  const rocksdb::TableProperties* table_properties;
  table_properties = rep_->table_properties.get();

  if (table_properties != nullptr) {
    out_file->Append(
        "Table Properties:\n"
        "--------------------------------------\n"
        "  ");
    out_file->Append(table_properties->ToString("\n  ", ": ").c_str());
    out_file->Append("\n");

    // Output Filter blocks
    if (!rep_->filter && !table_properties->filter_policy_name.empty()) {
      // Support only BloomFilter as off now
      rocksdb::BlockBasedTableOptions table_options;
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(1));
      if (table_properties->filter_policy_name.compare(
              table_options.filter_policy->Name()) == 0) {
        std::string filter_block_key = kFilterBlockPrefix;
        filter_block_key.append(table_properties->filter_policy_name);
        BlockHandle handle;
        if (FindMetaBlock(meta_iter.get(), filter_block_key, &handle).ok()) {
          BlockContents block;
          BlockFetcher block_fetcher(
              rep_->file.get(), nullptr /* prefetch_buffer */, rep_->footer,
              ReadOptions(), handle, &block, rep_->ioptions,
              false /*decompress*/, false /*maybe_compressed*/,
              UncompressionDict::GetEmptyDict(),
              rep_->persistent_cache_options);
          s = block_fetcher.ReadBlockContents();
          if (!s.ok()) {
            rep_->filter.reset(new BlockBasedFilterBlockReader(
                prefix_extractor, table_options,
                table_options.whole_key_filtering, std::move(block),
                rep_->ioptions.statistics));
          }
        }
      }
    }
  }
  if (rep_->filter) {
    out_file->Append(
        "Filter Details:\n"
        "--------------------------------------\n"
        "  ");
    out_file->Append(rep_->filter->ToString().c_str());
    out_file->Append("\n");
  }

  // Output Index block
  s = DumpIndexBlock(out_file);
  if (!s.ok()) {
    return s;
  }

  // Output compression dictionary
  if (!rep_->compression_dict_handle.IsNull()) {
    std::unique_ptr<const BlockContents> compression_dict_block;
    s = ReadCompressionDictBlock(rep_, nullptr /* prefetch_buffer */,
                                 &compression_dict_block);
    if (!s.ok()) {
      return s;
    }
    assert(compression_dict_block != nullptr);
    auto compression_dict = compression_dict_block->data;
    out_file->Append(
        "Compression Dictionary:\n"
        "--------------------------------------\n");
    out_file->Append("  size (bytes): ");
    out_file->Append(rocksdb::ToString(compression_dict.size()));
    out_file->Append("\n\n");
    out_file->Append("  HEX    ");
    out_file->Append(compression_dict.ToString(true).c_str());
    out_file->Append("\n\n");
  }

  // Output range deletions block
  auto* range_del_iter = NewRangeTombstoneIterator(ReadOptions());
  if (range_del_iter != nullptr) {
    range_del_iter->SeekToFirst();
    if (range_del_iter->Valid()) {
      out_file->Append(
          "Range deletions:\n"
          "--------------------------------------\n"
          "  ");
      for (; range_del_iter->Valid(); range_del_iter->Next()) {
        DumpKeyValue(range_del_iter->key(), range_del_iter->value(), out_file);
      }
      out_file->Append("\n");
    }
    delete range_del_iter;
  }
  // Output Data blocks
  s = DumpDataBlocks(out_file);

  return s;
}

void BlockBasedTable::Close() {
  if (rep_->closed) {
    return;
  }
  rep_->filter_entry.Release(rep_->table_options.block_cache.get());
  rep_->index_entry.Release(rep_->table_options.block_cache.get());
  // cleanup index and filter blocks to avoid accessing dangling pointer
  if (!rep_->table_options.no_block_cache) {
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    // Get the filter block key
    auto key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                           rep_->filter_handle, cache_key);
    rep_->table_options.block_cache.get()->Erase(key);
    // Get the index block key
    key = GetCacheKeyFromOffset(rep_->cache_key_prefix,
                                rep_->cache_key_prefix_size,
                                rep_->dummy_index_reader_offset, cache_key);
    rep_->table_options.block_cache.get()->Erase(key);
  }
  rep_->closed = true;
}

Status BlockBasedTable::DumpIndexBlock(WritableFile* out_file) {
  out_file->Append(
      "Index Details:\n"
      "--------------------------------------\n");
  std::unique_ptr<InternalIteratorBase<BlockHandle>> blockhandles_iter(
      NewIndexIterator(ReadOptions()));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_file->Append("Can not read Index Block \n\n");
    return s;
  }

  out_file->Append("  Block key hex dump: Data block handle\n");
  out_file->Append("  Block key ascii\n\n");
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }
    Slice key = blockhandles_iter->key();
    Slice user_key;
    InternalKey ikey;
    if (rep_->table_properties &&
        rep_->table_properties->index_key_is_user_key != 0) {
      user_key = key;
    } else {
      ikey.DecodeFrom(key);
      user_key = ikey.user_key();
    }

    out_file->Append("  HEX    ");
    out_file->Append(user_key.ToString(true).c_str());
    out_file->Append(": ");
    out_file->Append(blockhandles_iter->value().ToString(true).c_str());
    out_file->Append("\n");

    std::string str_key = user_key.ToString();
    std::string res_key("");
    char cspace = ' ';
    for (size_t i = 0; i < str_key.size(); i++) {
      res_key.append(&str_key[i], 1);
      res_key.append(1, cspace);
    }
    out_file->Append("  ASCII  ");
    out_file->Append(res_key.c_str());
    out_file->Append("\n  ------\n");
  }
  out_file->Append("\n");
  return Status::OK();
}

Status BlockBasedTable::DumpDataBlocks(WritableFile* out_file) {
  std::unique_ptr<InternalIteratorBase<BlockHandle>> blockhandles_iter(
      NewIndexIterator(ReadOptions()));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_file->Append("Can not read Index Block \n\n");
    return s;
  }

  uint64_t datablock_size_min = std::numeric_limits<uint64_t>::max();
  uint64_t datablock_size_max = 0;
  uint64_t datablock_size_sum = 0;

  size_t block_id = 1;
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       block_id++, blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }

    BlockHandle bh = blockhandles_iter->value();
    uint64_t datablock_size = bh.size();
    datablock_size_min = std::min(datablock_size_min, datablock_size);
    datablock_size_max = std::max(datablock_size_max, datablock_size);
    datablock_size_sum += datablock_size;

    out_file->Append("Data Block # ");
    out_file->Append(rocksdb::ToString(block_id));
    out_file->Append(" @ ");
    out_file->Append(blockhandles_iter->value().ToString(true).c_str());
    out_file->Append("\n");
    out_file->Append("--------------------------------------\n");

    std::unique_ptr<InternalIterator> datablock_iter;
    datablock_iter.reset(NewDataBlockIterator<DataBlockIter>(
        rep_, ReadOptions(), blockhandles_iter->value()));
    s = datablock_iter->status();

    if (!s.ok()) {
      out_file->Append("Error reading the block - Skipped \n\n");
      continue;
    }

    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        out_file->Append("Error reading the block - Skipped \n");
        break;
      }
      DumpKeyValue(datablock_iter->key(), datablock_iter->value(), out_file);
    }
    out_file->Append("\n");
  }

  uint64_t num_datablocks = block_id - 1;
  if (num_datablocks) {
    double datablock_size_avg =
        static_cast<double>(datablock_size_sum) / num_datablocks;
    out_file->Append("Data Block Summary:\n");
    out_file->Append("--------------------------------------");
    out_file->Append("\n  # data blocks: ");
    out_file->Append(rocksdb::ToString(num_datablocks));
    out_file->Append("\n  min data block size: ");
    out_file->Append(rocksdb::ToString(datablock_size_min));
    out_file->Append("\n  max data block size: ");
    out_file->Append(rocksdb::ToString(datablock_size_max));
    out_file->Append("\n  avg data block size: ");
    out_file->Append(rocksdb::ToString(datablock_size_avg));
    out_file->Append("\n");
  }

  return Status::OK();
}

void BlockBasedTable::DumpKeyValue(const Slice& key, const Slice& value,
                                   WritableFile* out_file) {
  InternalKey ikey;
  ikey.DecodeFrom(key);

  out_file->Append("  HEX    ");
  out_file->Append(ikey.user_key().ToString(true).c_str());
  out_file->Append(": ");
  out_file->Append(value.ToString(true).c_str());
  out_file->Append("\n");

  std::string str_key = ikey.user_key().ToString();
  std::string str_value = value.ToString();
  std::string res_key(""), res_value("");
  char cspace = ' ';
  for (size_t i = 0; i < str_key.size(); i++) {
    if (str_key[i] == '\0') {
      res_key.append("\\0", 2);
    } else {
      res_key.append(&str_key[i], 1);
    }
    res_key.append(1, cspace);
  }
  for (size_t i = 0; i < str_value.size(); i++) {
    if (str_value[i] == '\0') {
      res_value.append("\\0", 2);
    } else {
      res_value.append(&str_value[i], 1);
    }
    res_value.append(1, cspace);
  }

  out_file->Append("  ASCII  ");
  out_file->Append(res_key.c_str());
  out_file->Append(": ");
  out_file->Append(res_value.c_str());
  out_file->Append("\n  ------\n");
}

namespace {

void DeleteCachedFilterEntry(const Slice& /*key*/, void* value) {
  FilterBlockReader* filter = reinterpret_cast<FilterBlockReader*>(value);
  if (filter->statistics() != nullptr) {
    RecordTick(filter->statistics(), BLOCK_CACHE_FILTER_BYTES_EVICT,
               filter->ApproximateMemoryUsage());
  }
  delete filter;
}

void DeleteCachedIndexEntry(const Slice& /*key*/, void* value) {
  IndexReader* index_reader = reinterpret_cast<IndexReader*>(value);
  if (index_reader->statistics() != nullptr) {
    RecordTick(index_reader->statistics(), BLOCK_CACHE_INDEX_BYTES_EVICT,
               index_reader->ApproximateMemoryUsage());
  }
  delete index_reader;
}

void DeleteCachedUncompressionDictEntry(const Slice& /*key*/, void* value) {
  UncompressionDict* dict = reinterpret_cast<UncompressionDict*>(value);
  RecordTick(dict->statistics(), BLOCK_CACHE_COMPRESSION_DICT_BYTES_EVICT,
             dict->ApproximateMemoryUsage());
  delete dict;
}

}  // anonymous namespace

}  // namespace rocksdb
