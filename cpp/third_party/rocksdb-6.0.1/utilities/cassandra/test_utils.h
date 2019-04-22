//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <memory>
#include "util/testharness.h"
#include "utilities/cassandra/format.h"
#include "utilities/cassandra/serialize.h"

namespace rocksdb {
namespace cassandra {
extern const char kData[];
extern const char kExpiringData[];
extern const int32_t kTtl;
extern const int8_t kColumn;
extern const int8_t kTombstone;
extern const int8_t kExpiringColumn;


std::shared_ptr<ColumnBase> CreateTestColumn(int8_t mask,
                                             int8_t index,
                                             int64_t timestamp);

std::tuple<int8_t, int8_t, int64_t> CreateTestColumnSpec(int8_t mask,
                                                         int8_t index,
                                                         int64_t timestamp);

RowValue CreateTestRowValue(
    std::vector<std::tuple<int8_t, int8_t, int64_t>> column_specs);

RowValue CreateRowTombstone(int64_t timestamp);

void VerifyRowValueColumns(
  std::vector<std::shared_ptr<ColumnBase>> &columns,
  std::size_t index_of_vector,
  int8_t expected_mask,
  int8_t expected_index,
  int64_t expected_timestamp
);

int64_t ToMicroSeconds(int64_t seconds);
int32_t ToSeconds(int64_t microseconds);
}
}
