//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#ifndef ROCKSDB_LITE

#include "rocksdb/convenience.h"

#include "db/db_impl.h"
#include "util/cast_util.h"

namespace rocksdb {

void CancelAllBackgroundWork(DB* db, bool wait) {
  (static_cast_with_check<DBImpl, DB>(db->GetRootDB()))
      ->CancelAllBackgroundWork(wait);
}

Status DeleteFilesInRange(DB* db, ColumnFamilyHandle* column_family,
                          const Slice* begin, const Slice* end,
                          bool include_end) {
  RangePtr range(begin, end);
  return DeleteFilesInRanges(db, column_family, &range, 1, include_end);
}

Status DeleteFilesInRanges(DB* db, ColumnFamilyHandle* column_family,
                           const RangePtr* ranges, size_t n,
                           bool include_end) {
  return (static_cast_with_check<DBImpl, DB>(db->GetRootDB()))
      ->DeleteFilesInRanges(column_family, ranges, n, include_end);
}

Status VerifySstFileChecksum(const Options& options,
                             const EnvOptions& env_options,
                             const std::string& file_path) {
  std::unique_ptr<RandomAccessFile> file;
  uint64_t file_size;
  InternalKeyComparator internal_comparator(options.comparator);
  ImmutableCFOptions ioptions(options);

  Status s = ioptions.env->NewRandomAccessFile(file_path, &file, env_options);
  if (s.ok()) {
    s = ioptions.env->GetFileSize(file_path, &file_size);
  } else {
    return s;
  }
  std::unique_ptr<TableReader> table_reader;
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(file), file_path));
  const bool kImmortal = true;
  s = ioptions.table_factory->NewTableReader(
      TableReaderOptions(ioptions, options.prefix_extractor.get(), env_options,
                         internal_comparator, false /* skip_filters */,
                         !kImmortal, -1 /* level */),
      std::move(file_reader), file_size, &table_reader,
      false /* prefetch_index_and_filter_in_cache */);
  if (!s.ok()) {
    return s;
  }
  s = table_reader->VerifyChecksum();
  return s;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
