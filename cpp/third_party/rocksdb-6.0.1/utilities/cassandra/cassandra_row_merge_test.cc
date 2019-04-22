// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>
#include "util/testharness.h"
#include "utilities/cassandra/format.h"
#include "utilities/cassandra/test_utils.h"

namespace rocksdb {
namespace cassandra {

TEST(RowValueMergeTest, Merge) {
  std::vector<RowValue> row_values;
  row_values.push_back(
    CreateTestRowValue({
      CreateTestColumnSpec(kTombstone, 0, 5),
      CreateTestColumnSpec(kColumn, 1, 8),
      CreateTestColumnSpec(kExpiringColumn, 2, 5),
    })
  );

  row_values.push_back(
    CreateTestRowValue({
      CreateTestColumnSpec(kColumn, 0, 2),
      CreateTestColumnSpec(kExpiringColumn, 1, 5),
      CreateTestColumnSpec(kTombstone, 2, 7),
      CreateTestColumnSpec(kExpiringColumn, 7, 17),
    })
  );

  row_values.push_back(
    CreateTestRowValue({
      CreateTestColumnSpec(kExpiringColumn, 0, 6),
      CreateTestColumnSpec(kTombstone, 1, 5),
      CreateTestColumnSpec(kColumn, 2, 4),
      CreateTestColumnSpec(kTombstone, 11, 11),
    })
  );

  RowValue merged = RowValue::Merge(std::move(row_values));
  EXPECT_FALSE(merged.IsTombstone());
  EXPECT_EQ(merged.columns_.size(), 5);
  VerifyRowValueColumns(merged.columns_, 0, kExpiringColumn, 0, 6);
  VerifyRowValueColumns(merged.columns_, 1, kColumn, 1, 8);
  VerifyRowValueColumns(merged.columns_, 2, kTombstone, 2, 7);
  VerifyRowValueColumns(merged.columns_, 3, kExpiringColumn, 7, 17);
  VerifyRowValueColumns(merged.columns_, 4, kTombstone, 11, 11);
}

TEST(RowValueMergeTest, MergeWithRowTombstone) {
  std::vector<RowValue> row_values;

  // A row tombstone.
  row_values.push_back(
    CreateRowTombstone(11)
  );

  // This row's timestamp is smaller than tombstone.
  row_values.push_back(
    CreateTestRowValue({
      CreateTestColumnSpec(kColumn, 0, 5),
      CreateTestColumnSpec(kColumn, 1, 6),
    })
  );

  // Some of the column's row is smaller, some is larger.
  row_values.push_back(
    CreateTestRowValue({
      CreateTestColumnSpec(kColumn, 2, 10),
      CreateTestColumnSpec(kColumn, 3, 12),
    })
  );

  // All of the column's rows are larger than tombstone.
  row_values.push_back(
    CreateTestRowValue({
      CreateTestColumnSpec(kColumn, 4, 13),
      CreateTestColumnSpec(kColumn, 5, 14),
    })
  );

  RowValue merged = RowValue::Merge(std::move(row_values));
  EXPECT_FALSE(merged.IsTombstone());
  EXPECT_EQ(merged.columns_.size(), 3);
  VerifyRowValueColumns(merged.columns_, 0, kColumn, 3, 12);
  VerifyRowValueColumns(merged.columns_, 1, kColumn, 4, 13);
  VerifyRowValueColumns(merged.columns_, 2, kColumn, 5, 14);

  // If the tombstone's timestamp is the latest, then it returns a
  // row tombstone.
  row_values.push_back(
    CreateRowTombstone(15)
  );

  row_values.push_back(
    CreateRowTombstone(17)
  );

  merged = RowValue::Merge(std::move(row_values));
  EXPECT_TRUE(merged.IsTombstone());
  EXPECT_EQ(merged.LastModifiedTime(), 17);
}

} // namespace cassandra
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
