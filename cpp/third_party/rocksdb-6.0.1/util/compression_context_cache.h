// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

// Compression context cache allows to cache compression/uncompression contexts
// This helps with Random Read latencies and reduces CPU utilization
// Caching is implemented using CoreLocal facility. Compression/Uncompression
// instances are cached on a per core basis using CoreLocalArray. A borrowed
// instance is atomically replaced with a sentinel value for the time of being
// used. If it turns out that another thread is already makes use of the
// instance we still create one on the heap which is later is destroyed.

#pragma once

#include <stdint.h>

namespace rocksdb {
class ZSTDUncompressCachedData;

class CompressionContextCache {
 public:
  // Singleton
  static CompressionContextCache* Instance();
  static void InitSingleton();
  CompressionContextCache(const CompressionContextCache&) = delete;
  CompressionContextCache& operator=(const CompressionContextCache&) = delete;

  ZSTDUncompressCachedData GetCachedZSTDUncompressData();
  void ReturnCachedZSTDUncompressData(int64_t idx);

 private:
  // Singleton
  CompressionContextCache();
  ~CompressionContextCache();

  class Rep;
  Rep* rep_;
};

}  // namespace rocksdb
