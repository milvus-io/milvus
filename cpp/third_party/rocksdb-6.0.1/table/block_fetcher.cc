//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_fetcher.h"

#include <string>
#include <inttypes.h>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "rocksdb/env.h"
#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "table/format.h"
#include "table/persistent_cache_helper.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/logging.h"
#include "util/memory_allocator.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace rocksdb {

inline
void BlockFetcher::CheckBlockChecksum() {
  // Check the crc of the type and the block contents
  if (read_options_.verify_checksums) {
    const char* data = slice_.data();  // Pointer to where Read put the data
    PERF_TIMER_GUARD(block_checksum_time);
    uint32_t value = DecodeFixed32(data + block_size_ + 1);
    uint32_t actual = 0;
    switch (footer_.checksum()) {
      case kNoChecksum:
        break;
      case kCRC32c:
        value = crc32c::Unmask(value);
        actual = crc32c::Value(data, block_size_ + 1);
        break;
      case kxxHash:
        actual = XXH32(data, static_cast<int>(block_size_) + 1, 0);
        break;
      case kxxHash64:
        actual =static_cast<uint32_t> (
             XXH64(data, static_cast<int>(block_size_) + 1, 0) &
              uint64_t{0xffffffff}
          );
        break;
      default:
        status_ = Status::Corruption(
            "unknown checksum type " + ToString(footer_.checksum()) + " in " +
            file_->file_name() + " offset " + ToString(handle_.offset()) +
            " size " + ToString(block_size_));
    }
    if (status_.ok() && actual != value) {
      status_ = Status::Corruption(
          "block checksum mismatch: expected " + ToString(actual) + ", got " +
          ToString(value) + "  in " + file_->file_name() + " offset " +
          ToString(handle_.offset()) + " size " + ToString(block_size_));
    }
  }
}

inline
bool BlockFetcher::TryGetUncompressBlockFromPersistentCache() {
  if (cache_options_.persistent_cache &&
      !cache_options_.persistent_cache->IsCompressed()) {
    Status status = PersistentCacheHelper::LookupUncompressedPage(
        cache_options_, handle_, contents_);
    if (status.ok()) {
      // uncompressed page is found for the block handle
      return true;
    } else {
      // uncompressed page is not found
      if (ioptions_.info_log && !status.IsNotFound()) {
        assert(!status.ok());
        ROCKS_LOG_INFO(ioptions_.info_log,
                       "Error reading from persistent cache. %s",
                       status.ToString().c_str());
      }
    }
  }
  return false;
}

inline
bool BlockFetcher::TryGetFromPrefetchBuffer() {
  if (prefetch_buffer_ != nullptr &&
      prefetch_buffer_->TryReadFromCache(
          handle_.offset(),
          static_cast<size_t>(handle_.size()) + kBlockTrailerSize, &slice_)) {
    block_size_ = static_cast<size_t>(handle_.size());
    CheckBlockChecksum();
    if (!status_.ok()) {
      return true;
    }
    got_from_prefetch_buffer_ = true;
    used_buf_ = const_cast<char*>(slice_.data());
  }
  return got_from_prefetch_buffer_;
}

inline
bool BlockFetcher::TryGetCompressedBlockFromPersistentCache() {
  if (cache_options_.persistent_cache &&
      cache_options_.persistent_cache->IsCompressed()) {
    // lookup uncompressed cache mode p-cache
    std::unique_ptr<char[]> raw_data;
    status_ = PersistentCacheHelper::LookupRawPage(
        cache_options_, handle_, &raw_data, block_size_ + kBlockTrailerSize);
    if (status_.ok()) {
      heap_buf_ = CacheAllocationPtr(raw_data.release());
      used_buf_ = heap_buf_.get();
      slice_ = Slice(heap_buf_.get(), block_size_);
      return true;
    } else if (!status_.IsNotFound() && ioptions_.info_log) {
      assert(!status_.ok());
      ROCKS_LOG_INFO(ioptions_.info_log,
                     "Error reading from persistent cache. %s",
                     status_.ToString().c_str());
    }
  }
  return false;
}

inline
void BlockFetcher::PrepareBufferForBlockFromFile() {
  // cache miss read from device
  if (do_uncompress_ &&
      block_size_ + kBlockTrailerSize < kDefaultStackBufferSize) {
    // If we've got a small enough hunk of data, read it in to the
    // trivially allocated stack buffer instead of needing a full malloc()
    used_buf_ = &stack_buf_[0];
  } else if (maybe_compressed_ && !do_uncompress_) {
    compressed_buf_ = AllocateBlock(block_size_ + kBlockTrailerSize,
                                    memory_allocator_compressed_);
    used_buf_ = compressed_buf_.get();
  } else {
    heap_buf_ =
        AllocateBlock(block_size_ + kBlockTrailerSize, memory_allocator_);
    used_buf_ = heap_buf_.get();
  }
}

inline
void BlockFetcher::InsertCompressedBlockToPersistentCacheIfNeeded() {
  if (status_.ok() && read_options_.fill_cache &&
      cache_options_.persistent_cache &&
      cache_options_.persistent_cache->IsCompressed()) {
    // insert to raw cache
    PersistentCacheHelper::InsertRawPage(cache_options_, handle_, used_buf_,
                                         block_size_ + kBlockTrailerSize);
  }
}

inline
void BlockFetcher::InsertUncompressedBlockToPersistentCacheIfNeeded() {
  if (status_.ok() && !got_from_prefetch_buffer_ && read_options_.fill_cache &&
      cache_options_.persistent_cache &&
      !cache_options_.persistent_cache->IsCompressed()) {
    // insert to uncompressed cache
    PersistentCacheHelper::InsertUncompressedPage(cache_options_, handle_,
                                                  *contents_);
  }
}

inline void BlockFetcher::CopyBufferToHeap() {
  assert(used_buf_ != heap_buf_.get());
  heap_buf_ = AllocateBlock(block_size_ + kBlockTrailerSize, memory_allocator_);
  memcpy(heap_buf_.get(), used_buf_, block_size_ + kBlockTrailerSize);
}

inline
void BlockFetcher::GetBlockContents() {
  if (slice_.data() != used_buf_) {
    // the slice content is not the buffer provided
    *contents_ = BlockContents(Slice(slice_.data(), block_size_));
  } else {
    // page can be either uncompressed or compressed, the buffer either stack
    // or heap provided. Refer to https://github.com/facebook/rocksdb/pull/4096
    if (got_from_prefetch_buffer_ || used_buf_ == &stack_buf_[0]) {
      CopyBufferToHeap();
    } else if (used_buf_ == compressed_buf_.get()) {
      if (compression_type_ == kNoCompression &&
          memory_allocator_ != memory_allocator_compressed_) {
        CopyBufferToHeap();
      } else {
        heap_buf_ = std::move(compressed_buf_);
      }
    }
    *contents_ = BlockContents(std::move(heap_buf_), block_size_);
  }
#ifndef NDEBUG
  contents_->is_raw_block = true;
#endif
}

Status BlockFetcher::ReadBlockContents() {
  block_size_ = static_cast<size_t>(handle_.size());

  if (TryGetUncompressBlockFromPersistentCache()) {
    compression_type_ = kNoCompression;
#ifndef NDEBUG
    contents_->is_raw_block = true;
#endif  // NDEBUG
    return Status::OK();
  }
  if (TryGetFromPrefetchBuffer()) {
    if (!status_.ok()) {
      return status_;
    }
  } else if (!TryGetCompressedBlockFromPersistentCache()) {
    PrepareBufferForBlockFromFile();
    Status s;

    {
      PERF_TIMER_GUARD(block_read_time);
      // Actual file read
      status_ = file_->Read(handle_.offset(), block_size_ + kBlockTrailerSize,
                            &slice_, used_buf_);
    }
    PERF_COUNTER_ADD(block_read_count, 1);
    PERF_COUNTER_ADD(block_read_byte, block_size_ + kBlockTrailerSize);
    if (!status_.ok()) {
      return status_;
    }

    if (slice_.size() != block_size_ + kBlockTrailerSize) {
      return Status::Corruption("truncated block read from " +
                                file_->file_name() + " offset " +
                                ToString(handle_.offset()) + ", expected " +
                                ToString(block_size_ + kBlockTrailerSize) +
                                " bytes, got " + ToString(slice_.size()));
    }

    CheckBlockChecksum();
    if (status_.ok()) {
      InsertCompressedBlockToPersistentCacheIfNeeded();
    } else {
      return status_;
    }
  }

  PERF_TIMER_GUARD(block_decompress_time);

  compression_type_ = get_block_compression_type(slice_.data(), block_size_);

  if (do_uncompress_ && compression_type_ != kNoCompression) {
    // compressed page, uncompress, update cache
    UncompressionContext context(compression_type_);
    UncompressionInfo info(context, uncompression_dict_, compression_type_);
    status_ = UncompressBlockContents(info, slice_.data(), block_size_,
                                      contents_, footer_.version(), ioptions_,
                                      memory_allocator_);
    compression_type_ = kNoCompression;
  } else {
    GetBlockContents();
  }

  InsertUncompressedBlockToPersistentCacheIfNeeded();

  return status_;
}

}  // namespace rocksdb
