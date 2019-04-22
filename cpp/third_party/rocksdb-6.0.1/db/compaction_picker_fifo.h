//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef ROCKSDB_LITE

#include "db/compaction_picker.h"

namespace rocksdb {
class FIFOCompactionPicker : public CompactionPicker {
 public:
  FIFOCompactionPicker(const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}

  virtual Compaction* PickCompaction(const std::string& cf_name,
                                     const MutableCFOptions& mutable_cf_options,
                                     VersionStorageInfo* version,
                                     LogBuffer* log_buffer) override;

  virtual Compaction* CompactRange(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, int input_level, int output_level,
      uint32_t output_path_id, uint32_t max_subcompactions,
      const InternalKey* begin, const InternalKey* end,
      InternalKey** compaction_end, bool* manual_conflict) override;

  // The maximum allowed output level.  Always returns 0.
  virtual int MaxOutputLevel() const override { return 0; }

  virtual bool NeedsCompaction(
      const VersionStorageInfo* vstorage) const override;

 private:
  Compaction* PickTTLCompaction(const std::string& cf_name,
                                const MutableCFOptions& mutable_cf_options,
                                VersionStorageInfo* version,
                                LogBuffer* log_buffer);

  Compaction* PickSizeCompaction(const std::string& cf_name,
                                 const MutableCFOptions& mutable_cf_options,
                                 VersionStorageInfo* version,
                                 LogBuffer* log_buffer);
};
}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
