//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "merge_operator.h"

#include <memory>
#include <assert.h>

#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"
#include "utilities/cassandra/format.h"

namespace rocksdb {
namespace cassandra {

// Implementation for the merge operation (merges two Cassandra values)
bool CassandraValueMergeOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  // Clear the *new_value for writing.
  merge_out->new_value.clear();
  std::vector<RowValue> row_values;
  if (merge_in.existing_value) {
    row_values.push_back(
      RowValue::Deserialize(merge_in.existing_value->data(),
                            merge_in.existing_value->size()));
  }

  for (auto& operand : merge_in.operand_list) {
    row_values.push_back(RowValue::Deserialize(operand.data(), operand.size()));
  }

  RowValue merged = RowValue::Merge(std::move(row_values));
  merged = merged.RemoveTombstones(gc_grace_period_in_seconds_);
  merge_out->new_value.reserve(merged.Size());
  merged.Serialize(&(merge_out->new_value));

  return true;
}

bool CassandraValueMergeOperator::PartialMergeMulti(
    const Slice& /*key*/, const std::deque<Slice>& operand_list,
    std::string* new_value, Logger* /*logger*/) const {
  // Clear the *new_value for writing.
  assert(new_value);
  new_value->clear();

  std::vector<RowValue> row_values;
  for (auto& operand : operand_list) {
    row_values.push_back(RowValue::Deserialize(operand.data(), operand.size()));
  }
  RowValue merged = RowValue::Merge(std::move(row_values));
  new_value->reserve(merged.Size());
  merged.Serialize(new_value);
  return true;
}

const char* CassandraValueMergeOperator::Name() const  {
  return "CassandraValueMergeOperator";
}

} // namespace cassandra

} // namespace rocksdb
