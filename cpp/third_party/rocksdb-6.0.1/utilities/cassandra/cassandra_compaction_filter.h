// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

namespace rocksdb {
namespace cassandra {

/**
 * Compaction filter for removing expired Cassandra data with ttl.
 * If option `purge_ttl_on_expiration` is set to true, expired data
 * will be directly purged. Otherwise expired data will be converted
 * tombstones first, then be eventally removed after gc grace period.
 * `purge_ttl_on_expiration` should only be on in the case all the
 * writes have same ttl setting, otherwise it could bring old data back.
 *
 * Compaction filter is also in charge of removing tombstone that has been
 * promoted to kValue type after serials of merging in compaction.
 */
class CassandraCompactionFilter : public CompactionFilter {
public:
 explicit CassandraCompactionFilter(bool purge_ttl_on_expiration,
                                    int32_t gc_grace_period_in_seconds)
     : purge_ttl_on_expiration_(purge_ttl_on_expiration),
       gc_grace_period_in_seconds_(gc_grace_period_in_seconds) {}

 const char* Name() const override;
 virtual Decision FilterV2(int level, const Slice& key, ValueType value_type,
                           const Slice& existing_value, std::string* new_value,
                           std::string* skip_until) const override;

private:
  bool purge_ttl_on_expiration_;
  int32_t gc_grace_period_in_seconds_;
};
}  // namespace cassandra
}  // namespace rocksdb
