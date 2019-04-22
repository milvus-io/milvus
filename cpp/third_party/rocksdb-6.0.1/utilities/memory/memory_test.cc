// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "db/db_impl.h"
#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/memory_util.h"
#include "rocksdb/utilities/stackable_db.h"
#include "table/block_based_table_factory.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class MemoryTest : public testing::Test {
 public:
  MemoryTest() : kDbDir(test::PerThreadDBPath("memory_test")), rnd_(301) {
    assert(Env::Default()->CreateDirIfMissing(kDbDir).ok());
  }

  std::string GetDBName(int id) { return kDbDir + "db_" + ToString(id); }

  std::string RandomString(int len) {
    std::string r;
    test::RandomString(&rnd_, len, &r);
    return r;
  }

  void UpdateUsagesHistory(const std::vector<DB*>& dbs) {
    std::map<MemoryUtil::UsageType, uint64_t> usage_by_type;
    ASSERT_OK(GetApproximateMemoryUsageByType(dbs, &usage_by_type));
    for (int i = 0; i < MemoryUtil::kNumUsageTypes; ++i) {
      usage_history_[i].push_back(
          usage_by_type[static_cast<MemoryUtil::UsageType>(i)]);
    }
  }

  void GetCachePointersFromTableFactory(
      const TableFactory* factory,
      std::unordered_set<const Cache*>* cache_set) {
    const BlockBasedTableFactory* bbtf =
        dynamic_cast<const BlockBasedTableFactory*>(factory);
    if (bbtf != nullptr) {
      const auto bbt_opts = bbtf->table_options();
      cache_set->insert(bbt_opts.block_cache.get());
      cache_set->insert(bbt_opts.block_cache_compressed.get());
    }
  }

  void GetCachePointers(const std::vector<DB*>& dbs,
                        std::unordered_set<const Cache*>* cache_set) {
    cache_set->clear();

    for (auto* db : dbs) {
      // Cache from DBImpl
      StackableDB* sdb = dynamic_cast<StackableDB*>(db);
      DBImpl* db_impl = dynamic_cast<DBImpl*>(sdb ? sdb->GetBaseDB() : db);
      if (db_impl != nullptr) {
        cache_set->insert(db_impl->TEST_table_cache());
      }

      // Cache from DBOptions
      cache_set->insert(db->GetDBOptions().row_cache.get());

      // Cache from table factories
      std::unordered_map<std::string, const ImmutableCFOptions*> iopts_map;
      if (db_impl != nullptr) {
        ASSERT_OK(db_impl->TEST_GetAllImmutableCFOptions(&iopts_map));
      }
      for (auto pair : iopts_map) {
        GetCachePointersFromTableFactory(pair.second->table_factory, cache_set);
      }
    }
  }

  Status GetApproximateMemoryUsageByType(
      const std::vector<DB*>& dbs,
      std::map<MemoryUtil::UsageType, uint64_t>* usage_by_type) {
    std::unordered_set<const Cache*> cache_set;
    GetCachePointers(dbs, &cache_set);

    return MemoryUtil::GetApproximateMemoryUsageByType(dbs, cache_set,
                                                       usage_by_type);
  }

  const std::string kDbDir;
  Random rnd_;
  std::vector<uint64_t> usage_history_[MemoryUtil::kNumUsageTypes];
};

TEST_F(MemoryTest, SharedBlockCacheTotal) {
  std::vector<DB*> dbs;
  std::vector<uint64_t> usage_by_type;
  const int kNumDBs = 10;
  const int kKeySize = 100;
  const int kValueSize = 500;
  Options opt;
  opt.create_if_missing = true;
  opt.write_buffer_size = kKeySize + kValueSize;
  opt.max_write_buffer_number = 10;
  opt.min_write_buffer_number_to_merge = 10;
  opt.disable_auto_compactions = true;
  BlockBasedTableOptions bbt_opts;
  bbt_opts.block_cache = NewLRUCache(4096 * 1000 * 10);
  for (int i = 0; i < kNumDBs; ++i) {
    DestroyDB(GetDBName(i), opt);
    DB* db = nullptr;
    ASSERT_OK(DB::Open(opt, GetDBName(i), &db));
    dbs.push_back(db);
  }

  std::vector<std::string> keys_by_db[kNumDBs];

  // Fill one memtable per Put to make memtable use more memory.
  for (int p = 0; p < opt.min_write_buffer_number_to_merge / 2; ++p) {
    for (int i = 0; i < kNumDBs; ++i) {
      for (int j = 0; j < 100; ++j) {
        keys_by_db[i].emplace_back(RandomString(kKeySize));
        dbs[i]->Put(WriteOptions(), keys_by_db[i].back(),
                    RandomString(kValueSize));
      }
      dbs[i]->Flush(FlushOptions());
    }
  }
  for (int i = 0; i < kNumDBs; ++i) {
    for (auto& key : keys_by_db[i]) {
      std::string value;
      dbs[i]->Get(ReadOptions(), key, &value);
    }
    UpdateUsagesHistory(dbs);
  }
  for (size_t i = 1; i < usage_history_[MemoryUtil::kMemTableTotal].size();
       ++i) {
    // Expect EQ as we didn't flush more memtables.
    ASSERT_EQ(usage_history_[MemoryUtil::kTableReadersTotal][i],
              usage_history_[MemoryUtil::kTableReadersTotal][i - 1]);
  }
  for (int i = 0; i < kNumDBs; ++i) {
    delete dbs[i];
  }
}

TEST_F(MemoryTest, MemTableAndTableReadersTotal) {
  std::vector<DB*> dbs;
  std::vector<uint64_t> usage_by_type;
  std::vector<std::vector<ColumnFamilyHandle*>> vec_handles;
  const int kNumDBs = 10;
  const int kKeySize = 100;
  const int kValueSize = 500;
  Options opt;
  opt.create_if_missing = true;
  opt.create_missing_column_families = true;
  opt.write_buffer_size = kKeySize + kValueSize;
  opt.max_write_buffer_number = 10;
  opt.min_write_buffer_number_to_merge = 10;
  opt.disable_auto_compactions = true;

  std::vector<ColumnFamilyDescriptor> cf_descs = {
      {kDefaultColumnFamilyName, ColumnFamilyOptions(opt)},
      {"one", ColumnFamilyOptions(opt)},
      {"two", ColumnFamilyOptions(opt)},
  };

  for (int i = 0; i < kNumDBs; ++i) {
    DestroyDB(GetDBName(i), opt);
    std::vector<ColumnFamilyHandle*> handles;
    dbs.emplace_back();
    vec_handles.emplace_back();
    ASSERT_OK(DB::Open(DBOptions(opt), GetDBName(i), cf_descs,
                       &vec_handles.back(), &dbs.back()));
  }

  // Fill one memtable per Put to make memtable use more memory.
  for (int p = 0; p < opt.min_write_buffer_number_to_merge / 2; ++p) {
    for (int i = 0; i < kNumDBs; ++i) {
      for (auto* handle : vec_handles[i]) {
        dbs[i]->Put(WriteOptions(), handle, RandomString(kKeySize),
                    RandomString(kValueSize));
        UpdateUsagesHistory(dbs);
      }
    }
  }
  // Expect the usage history is monotonically increasing
  for (size_t i = 1; i < usage_history_[MemoryUtil::kMemTableTotal].size();
       ++i) {
    ASSERT_GT(usage_history_[MemoryUtil::kMemTableTotal][i],
              usage_history_[MemoryUtil::kMemTableTotal][i - 1]);
    ASSERT_GT(usage_history_[MemoryUtil::kMemTableUnFlushed][i],
              usage_history_[MemoryUtil::kMemTableUnFlushed][i - 1]);
    ASSERT_EQ(usage_history_[MemoryUtil::kTableReadersTotal][i],
              usage_history_[MemoryUtil::kTableReadersTotal][i - 1]);
  }

  size_t usage_check_point = usage_history_[MemoryUtil::kMemTableTotal].size();
  std::vector<Iterator*> iters;

  // Create an iterator and flush all memtables for each db
  for (int i = 0; i < kNumDBs; ++i) {
    iters.push_back(dbs[i]->NewIterator(ReadOptions()));
    dbs[i]->Flush(FlushOptions());

    for (int j = 0; j < 100; ++j) {
      std::string value;
      dbs[i]->Get(ReadOptions(), RandomString(kKeySize), &value);
    }

    UpdateUsagesHistory(dbs);
  }
  for (size_t i = usage_check_point;
       i < usage_history_[MemoryUtil::kMemTableTotal].size(); ++i) {
    // Since memtables are pinned by iterators, we don't expect the
    // memory usage of all the memtables decreases as they are pinned
    // by iterators.
    ASSERT_GE(usage_history_[MemoryUtil::kMemTableTotal][i],
              usage_history_[MemoryUtil::kMemTableTotal][i - 1]);
    // Expect the usage history from the "usage_decay_point" is
    // monotonically decreasing.
    ASSERT_LT(usage_history_[MemoryUtil::kMemTableUnFlushed][i],
              usage_history_[MemoryUtil::kMemTableUnFlushed][i - 1]);
    // Expect the usage history of the table readers increases
    // as we flush tables.
    ASSERT_GT(usage_history_[MemoryUtil::kTableReadersTotal][i],
              usage_history_[MemoryUtil::kTableReadersTotal][i - 1]);
    ASSERT_GT(usage_history_[MemoryUtil::kCacheTotal][i],
              usage_history_[MemoryUtil::kCacheTotal][i - 1]);
  }
  usage_check_point = usage_history_[MemoryUtil::kMemTableTotal].size();
  for (int i = 0; i < kNumDBs; ++i) {
    delete iters[i];
    UpdateUsagesHistory(dbs);
  }
  for (size_t i = usage_check_point;
       i < usage_history_[MemoryUtil::kMemTableTotal].size(); ++i) {
    // Expect the usage of all memtables decreasing as we delete iterators.
    ASSERT_LT(usage_history_[MemoryUtil::kMemTableTotal][i],
              usage_history_[MemoryUtil::kMemTableTotal][i - 1]);
    // Since the memory usage of un-flushed memtables is only affected
    // by Put and flush, we expect EQ here as we only delete iterators.
    ASSERT_EQ(usage_history_[MemoryUtil::kMemTableUnFlushed][i],
              usage_history_[MemoryUtil::kMemTableUnFlushed][i - 1]);
    // Expect EQ as we didn't flush more memtables.
    ASSERT_EQ(usage_history_[MemoryUtil::kTableReadersTotal][i],
              usage_history_[MemoryUtil::kTableReadersTotal][i - 1]);
  }

  for (int i = 0; i < kNumDBs; ++i) {
    for (auto* handle : vec_handles[i]) {
      delete handle;
    }
    delete dbs[i];
  }
}
}  // namespace rocksdb

int main(int argc, char** argv) {
#if !(defined NDEBUG) || !defined(OS_WIN)
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  return 0;
#endif
}

#else
#include <cstdio>

int main(int /*argc*/, char** /*argv*/) {
  printf("Skipped in RocksDBLite as utilities are not supported.\n");
  return 0;
}
#endif  // !ROCKSDB_LITE
