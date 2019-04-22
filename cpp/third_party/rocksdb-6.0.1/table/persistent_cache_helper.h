//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <string>

#include "monitoring/statistics.h"
#include "table/format.h"
#include "table/persistent_cache_options.h"

namespace rocksdb {

struct BlockContents;

// PersistentCacheHelper
//
// Encapsulates  some of the helper logic for read and writing from the cache
class PersistentCacheHelper {
 public:
  // insert block into raw page cache
  static void InsertRawPage(const PersistentCacheOptions& cache_options,
                            const BlockHandle& handle, const char* data,
                            const size_t size);

  // insert block into uncompressed cache
  static void InsertUncompressedPage(
      const PersistentCacheOptions& cache_options, const BlockHandle& handle,
      const BlockContents& contents);

  // lookup block from raw page cacge
  static Status LookupRawPage(const PersistentCacheOptions& cache_options,
                              const BlockHandle& handle,
                              std::unique_ptr<char[]>* raw_data,
                              const size_t raw_data_size);

  // lookup block from uncompressed cache
  static Status LookupUncompressedPage(
      const PersistentCacheOptions& cache_options, const BlockHandle& handle,
      BlockContents* contents);
};

}  // namespace rocksdb
