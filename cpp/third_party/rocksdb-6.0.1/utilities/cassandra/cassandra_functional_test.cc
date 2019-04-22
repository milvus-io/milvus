// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/testharness.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/cassandra/cassandra_compaction_filter.h"
#include "utilities/cassandra/merge_operator.h"
#include "utilities/cassandra/test_utils.h"

using namespace rocksdb;

namespace rocksdb {
namespace cassandra {

// Path to the database on file system
const std::string kDbName = test::PerThreadDBPath("cassandra_functional_test");

class CassandraStore {
 public:
  explicit CassandraStore(std::shared_ptr<DB> db)
      : db_(db), write_option_(), get_option_() {
    assert(db);
  }

  bool Append(const std::string& key, const RowValue& val){
    std::string result;
    val.Serialize(&result);
    Slice valSlice(result.data(), result.size());
    auto s = db_->Merge(write_option_, key, valSlice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << "ERROR " << s.ToString() << std::endl;
      return false;
    }
  }

  bool Put(const std::string& key, const RowValue& val) {
    std::string result;
    val.Serialize(&result);
    Slice valSlice(result.data(), result.size());
    auto s = db_->Put(write_option_, key, valSlice);
    if (s.ok()) {
      return true;
    } else {
      std::cerr << "ERROR " << s.ToString() << std::endl;
      return false;
    }
  }

  void Flush() {
    dbfull()->TEST_FlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }

  void Compact() {
    dbfull()->TEST_CompactRange(
      0, nullptr, nullptr, db_->DefaultColumnFamily());
  }

  std::tuple<bool, RowValue> Get(const std::string& key){
    std::string result;
    auto s = db_->Get(get_option_, key, &result);

    if (s.ok()) {
      return std::make_tuple(true,
                             RowValue::Deserialize(result.data(),
                                                   result.size()));
    }

    if (!s.IsNotFound()) {
      std::cerr << "ERROR " << s.ToString() << std::endl;
    }

    return std::make_tuple(false, RowValue(0, 0));
  }

 private:
  std::shared_ptr<DB> db_;
  WriteOptions write_option_;
  ReadOptions get_option_;

  DBImpl* dbfull() { return reinterpret_cast<DBImpl*>(db_.get()); }
};

class TestCompactionFilterFactory : public CompactionFilterFactory {
public:
 explicit TestCompactionFilterFactory(bool purge_ttl_on_expiration,
                                      int32_t gc_grace_period_in_seconds)
     : purge_ttl_on_expiration_(purge_ttl_on_expiration),
       gc_grace_period_in_seconds_(gc_grace_period_in_seconds) {}

 std::unique_ptr<CompactionFilter> CreateCompactionFilter(
     const CompactionFilter::Context& /*context*/) override {
   return std::unique_ptr<CompactionFilter>(new CassandraCompactionFilter(
       purge_ttl_on_expiration_, gc_grace_period_in_seconds_));
 }

 const char* Name() const override { return "TestCompactionFilterFactory"; }

private:
  bool purge_ttl_on_expiration_;
  int32_t gc_grace_period_in_seconds_;
};


// The class for unit-testing
class CassandraFunctionalTest : public testing::Test {
public:
  CassandraFunctionalTest() {
    DestroyDB(kDbName, Options());    // Start each test with a fresh DB
  }

  std::shared_ptr<DB> OpenDb() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.merge_operator.reset(new CassandraValueMergeOperator(gc_grace_period_in_seconds_));
    auto* cf_factory = new TestCompactionFilterFactory(
        purge_ttl_on_expiration_, gc_grace_period_in_seconds_);
    options.compaction_filter_factory.reset(cf_factory);
    EXPECT_OK(DB::Open(options, kDbName, &db));
    return std::shared_ptr<DB>(db);
  }

  bool purge_ttl_on_expiration_ = false;
  int32_t gc_grace_period_in_seconds_ = 100;
};

// THE TEST CASES BEGIN HERE

TEST_F(CassandraFunctionalTest, SimpleMergeTest) {
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kTombstone, 0, ToMicroSeconds(now + 5)),
    CreateTestColumnSpec(kColumn, 1, ToMicroSeconds(now + 8)),
    CreateTestColumnSpec(kExpiringColumn, 2, ToMicroSeconds(now + 5)),
  }));
  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kColumn, 0, ToMicroSeconds(now + 2)),
    CreateTestColumnSpec(kExpiringColumn, 1, ToMicroSeconds(now + 5)),
    CreateTestColumnSpec(kTombstone, 2, ToMicroSeconds(now + 7)),
    CreateTestColumnSpec(kExpiringColumn, 7, ToMicroSeconds(now + 17)),
  }));
  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now + 6)),
    CreateTestColumnSpec(kTombstone, 1, ToMicroSeconds(now + 5)),
    CreateTestColumnSpec(kColumn, 2, ToMicroSeconds(now + 4)),
    CreateTestColumnSpec(kTombstone, 11, ToMicroSeconds(now + 11)),
  }));

  auto ret = store.Get("k1");

  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.columns_.size(), 5);
  VerifyRowValueColumns(merged.columns_, 0, kExpiringColumn, 0, ToMicroSeconds(now + 6));
  VerifyRowValueColumns(merged.columns_, 1, kColumn, 1, ToMicroSeconds(now + 8));
  VerifyRowValueColumns(merged.columns_, 2, kTombstone, 2, ToMicroSeconds(now + 7));
  VerifyRowValueColumns(merged.columns_, 3, kExpiringColumn, 7, ToMicroSeconds(now + 17));
  VerifyRowValueColumns(merged.columns_, 4, kTombstone, 11, ToMicroSeconds(now + 11));
}

TEST_F(CassandraFunctionalTest,
       CompactionShouldConvertExpiredColumnsToTombstone) {
  CassandraStore store(OpenDb());
  int64_t now= time(nullptr);

  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 20)), //expired
    CreateTestColumnSpec(kExpiringColumn, 1, ToMicroSeconds(now - kTtl + 10)), // not expired
    CreateTestColumnSpec(kTombstone, 3, ToMicroSeconds(now))
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 10)), //expired
    CreateTestColumnSpec(kColumn, 2, ToMicroSeconds(now))
  }));

  store.Flush();
  store.Compact();

  auto ret = store.Get("k1");
  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.columns_.size(), 4);
  VerifyRowValueColumns(merged.columns_, 0, kTombstone, 0, ToMicroSeconds(now - 10));
  VerifyRowValueColumns(merged.columns_, 1, kExpiringColumn, 1, ToMicroSeconds(now - kTtl + 10));
  VerifyRowValueColumns(merged.columns_, 2, kColumn, 2, ToMicroSeconds(now));
  VerifyRowValueColumns(merged.columns_, 3, kTombstone, 3, ToMicroSeconds(now));
}


TEST_F(CassandraFunctionalTest,
       CompactionShouldPurgeExpiredColumnsIfPurgeTtlIsOn) {
  purge_ttl_on_expiration_ = true;
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 20)), //expired
    CreateTestColumnSpec(kExpiringColumn, 1, ToMicroSeconds(now)), // not expired
    CreateTestColumnSpec(kTombstone, 3, ToMicroSeconds(now))
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 10)), //expired
    CreateTestColumnSpec(kColumn, 2, ToMicroSeconds(now))
  }));

  store.Flush();
  store.Compact();

  auto ret = store.Get("k1");
  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.columns_.size(), 3);
  VerifyRowValueColumns(merged.columns_, 0, kExpiringColumn, 1, ToMicroSeconds(now));
  VerifyRowValueColumns(merged.columns_, 1, kColumn, 2, ToMicroSeconds(now));
  VerifyRowValueColumns(merged.columns_, 2, kTombstone, 3, ToMicroSeconds(now));
}

TEST_F(CassandraFunctionalTest,
       CompactionShouldRemoveRowWhenAllColumnsExpiredIfPurgeTtlIsOn) {
  purge_ttl_on_expiration_ = true;
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 20)),
    CreateTestColumnSpec(kExpiringColumn, 1, ToMicroSeconds(now - kTtl - 20)),
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now - kTtl - 10)),
  }));

  store.Flush();
  store.Compact();
  ASSERT_FALSE(std::get<0>(store.Get("k1")));
}

TEST_F(CassandraFunctionalTest,
       CompactionShouldRemoveTombstoneExceedingGCGracePeriod) {
  purge_ttl_on_expiration_ = true;
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Append("k1", CreateTestRowValue({
    CreateTestColumnSpec(kTombstone, 0, ToMicroSeconds(now - gc_grace_period_in_seconds_ - 1)),
    CreateTestColumnSpec(kColumn, 1, ToMicroSeconds(now))
  }));

  store.Append("k2", CreateTestRowValue({
    CreateTestColumnSpec(kColumn, 0, ToMicroSeconds(now))
  }));

  store.Flush();

  store.Append("k1",CreateTestRowValue({
    CreateTestColumnSpec(kColumn, 1, ToMicroSeconds(now)),
  }));

  store.Flush();
  store.Compact();

  auto ret = store.Get("k1");
  ASSERT_TRUE(std::get<0>(ret));
  RowValue& gced = std::get<1>(ret);
  EXPECT_EQ(gced.columns_.size(), 1);
  VerifyRowValueColumns(gced.columns_, 0, kColumn, 1, ToMicroSeconds(now));
}

TEST_F(CassandraFunctionalTest, CompactionShouldRemoveTombstoneFromPut) {
  purge_ttl_on_expiration_ = true;
  CassandraStore store(OpenDb());
  int64_t now = time(nullptr);

  store.Put("k1", CreateTestRowValue({
    CreateTestColumnSpec(kTombstone, 0, ToMicroSeconds(now - gc_grace_period_in_seconds_ - 1)),
  }));

  store.Flush();
  store.Compact();
  ASSERT_FALSE(std::get<0>(store.Get("k1")));
}

} // namespace cassandra
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
