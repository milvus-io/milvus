//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "db/version_edit.h"
#include "util/random.h"

namespace rocksdb {
static const uint32_t kFileReadSampleRate = 1024;
extern bool should_sample_file_read();
extern void sample_file_read_inc(FileMetaData*);

inline bool should_sample_file_read() {
  return (Random::GetTLSInstance()->Next() % kFileReadSampleRate == 307);
}

inline void sample_file_read_inc(FileMetaData* meta) {
  meta->stats.num_reads_sampled.fetch_add(kFileReadSampleRate,
                                          std::memory_order_relaxed);
}
}
