//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "test_utils.h"

namespace rocksdb {
namespace cassandra {
const char kData[] = {'d', 'a', 't', 'a'};
const char kExpiringData[] = {'e', 'd', 'a', 't', 'a'};
const int32_t kTtl = 86400;
const int8_t kColumn = 0;
const int8_t kTombstone = 1;
const int8_t kExpiringColumn = 2;

std::shared_ptr<ColumnBase> CreateTestColumn(int8_t mask,
                                             int8_t index,
                                             int64_t timestamp) {
  if ((mask & ColumnTypeMask::DELETION_MASK) != 0) {
    return std::shared_ptr<Tombstone>(
        new Tombstone(mask, index, ToSeconds(timestamp), timestamp));
  } else if ((mask & ColumnTypeMask::EXPIRATION_MASK) != 0) {
    return std::shared_ptr<ExpiringColumn>(new ExpiringColumn(
      mask, index, timestamp, sizeof(kExpiringData), kExpiringData, kTtl));
  } else {
    return std::shared_ptr<Column>(
      new Column(mask, index, timestamp, sizeof(kData), kData));
  }
}

std::tuple<int8_t, int8_t, int64_t> CreateTestColumnSpec(int8_t mask,
                                                         int8_t index,
                                                         int64_t timestamp) {
  return std::make_tuple(mask, index, timestamp);
}

RowValue CreateTestRowValue(
    std::vector<std::tuple<int8_t, int8_t, int64_t>> column_specs) {
  std::vector<std::shared_ptr<ColumnBase>> columns;
  int64_t last_modified_time = 0;
  for (auto spec: column_specs) {
    auto c = CreateTestColumn(std::get<0>(spec), std::get<1>(spec),
                              std::get<2>(spec));
    last_modified_time = std::max(last_modified_time, c -> Timestamp());
    columns.push_back(std::move(c));
  }
  return RowValue(std::move(columns), last_modified_time);
}

RowValue CreateRowTombstone(int64_t timestamp) {
  return RowValue(ToSeconds(timestamp), timestamp);
}

void VerifyRowValueColumns(
  std::vector<std::shared_ptr<ColumnBase>> &columns,
  std::size_t index_of_vector,
  int8_t expected_mask,
  int8_t expected_index,
  int64_t expected_timestamp
) {
  EXPECT_EQ(expected_timestamp, columns[index_of_vector]->Timestamp());
  EXPECT_EQ(expected_mask, columns[index_of_vector]->Mask());
  EXPECT_EQ(expected_index, columns[index_of_vector]->Index());
}

int64_t ToMicroSeconds(int64_t seconds) {
  return seconds * (int64_t)1000000;
}

int32_t ToSeconds(int64_t microseconds) {
  return (int32_t)(microseconds / (int64_t)1000000);
}
}
}
