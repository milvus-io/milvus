//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <string>
#include <vector>

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/transaction_log.h"
#include "util/file_util.h"
#include "util/string_util.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
class RepairTest : public DBTestBase {
 public:
  RepairTest() : DBTestBase("/repair_test") {}

  std::string GetFirstSstPath() {
    uint64_t manifest_size;
    std::vector<std::string> files;
    db_->GetLiveFiles(files, &manifest_size);
    auto sst_iter =
        std::find_if(files.begin(), files.end(), [](const std::string& file) {
          uint64_t number;
          FileType type;
          bool ok = ParseFileName(file, &number, &type);
          return ok && type == kTableFile;
        });
    return sst_iter == files.end() ? "" : dbname_ + *sst_iter;
  }
};

TEST_F(RepairTest, LostManifest) {
  // Add a couple SST files, delete the manifest, and verify RepairDB() saves
  // the day.
  Put("key", "val");
  Flush();
  Put("key2", "val2");
  Flush();
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  Reopen(CurrentOptions());

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, CorruptManifest) {
  // Manifest is in an invalid format. Expect a full recovery.
  Put("key", "val");
  Flush();
  Put("key2", "val2");
  Flush();
  // Need to get path before Close() deletes db_, but overwrite it after Close()
  // to ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  CreateFile(env_, manifest_path, "blah", false /* use_fsync */);
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  Reopen(CurrentOptions());

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, IncompleteManifest) {
  // In this case, the manifest is valid but does not reference all of the SST
  // files. Expect a full recovery.
  Put("key", "val");
  Flush();
  std::string orig_manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  CopyFile(orig_manifest_path, orig_manifest_path + ".tmp");
  Put("key2", "val2");
  Flush();
  // Need to get path before Close() deletes db_, but overwrite it after Close()
  // to ensure Close() didn't change the manifest.
  std::string new_manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(new_manifest_path));
  // Replace the manifest with one that is only aware of the first SST file.
  CopyFile(orig_manifest_path + ".tmp", new_manifest_path);
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  Reopen(CurrentOptions());

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, PostRepairSstFileNumbering) {
  // Verify after a DB is repaired, new files will be assigned higher numbers
  // than old files.
  Put("key", "val");
  Flush();
  Put("key2", "val2");
  Flush();
  uint64_t pre_repair_file_num = dbfull()->TEST_Current_Next_FileNo();
  Close();

  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  Reopen(CurrentOptions());
  uint64_t post_repair_file_num = dbfull()->TEST_Current_Next_FileNo();
  ASSERT_GE(post_repair_file_num, pre_repair_file_num);
}

TEST_F(RepairTest, LostSst) {
  // Delete one of the SST files but preserve the manifest that refers to it,
  // then verify the DB is still usable for the intact SST.
  Put("key", "val");
  Flush();
  Put("key2", "val2");
  Flush();
  auto sst_path = GetFirstSstPath();
  ASSERT_FALSE(sst_path.empty());
  ASSERT_OK(env_->DeleteFile(sst_path));

  Close();
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  Reopen(CurrentOptions());

  // Exactly one of the key-value pairs should be in the DB now.
  ASSERT_TRUE((Get("key") == "val") != (Get("key2") == "val2"));
}

TEST_F(RepairTest, CorruptSst) {
  // Corrupt one of the SST files but preserve the manifest that refers to it,
  // then verify the DB is still usable for the intact SST.
  Put("key", "val");
  Flush();
  Put("key2", "val2");
  Flush();
  auto sst_path = GetFirstSstPath();
  ASSERT_FALSE(sst_path.empty());
  CreateFile(env_, sst_path, "blah", false /* use_fsync */);

  Close();
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  Reopen(CurrentOptions());

  // Exactly one of the key-value pairs should be in the DB now.
  ASSERT_TRUE((Get("key") == "val") != (Get("key2") == "val2"));
}

TEST_F(RepairTest, UnflushedSst) {
  // This test case invokes repair while some data is unflushed, then verifies
  // that data is in the db.
  Put("key", "val");
  VectorLogPtr wal_files;
  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 1);
  uint64_t total_ssts_size;
  GetAllSSTFiles(&total_ssts_size);
  ASSERT_EQ(total_ssts_size, 0);
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  Reopen(CurrentOptions());

  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 0);
  GetAllSSTFiles(&total_ssts_size);
  ASSERT_GT(total_ssts_size, 0);
  ASSERT_EQ(Get("key"), "val");
}

TEST_F(RepairTest, SeparateWalDir) {
  do {
    Options options = CurrentOptions();
    DestroyAndReopen(options);
    Put("key", "val");
    Put("foo", "bar");
    VectorLogPtr wal_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
    ASSERT_EQ(wal_files.size(), 1);
    uint64_t total_ssts_size;
    GetAllSSTFiles(&total_ssts_size);
    ASSERT_EQ(total_ssts_size, 0);
    std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

    Close();
    ASSERT_OK(env_->FileExists(manifest_path));
    ASSERT_OK(env_->DeleteFile(manifest_path));
    ASSERT_OK(RepairDB(dbname_, options));

    // make sure that all WALs are converted to SSTables.
    options.wal_dir = "";

    Reopen(options);
    ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
    ASSERT_EQ(wal_files.size(), 0);
    GetAllSSTFiles(&total_ssts_size);
    ASSERT_GT(total_ssts_size, 0);
    ASSERT_EQ(Get("key"), "val");
    ASSERT_EQ(Get("foo"), "bar");

 } while(ChangeWalOptions());
}

TEST_F(RepairTest, RepairMultipleColumnFamilies) {
  // Verify repair logic associates SST files with their original column
  // families.
  const int kNumCfs = 3;
  const int kEntriesPerCf = 2;
  DestroyAndReopen(CurrentOptions());
  CreateAndReopenWithCF({"pikachu1", "pikachu2"}, CurrentOptions());
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      Put(i, "key" + ToString(j), "val" + ToString(j));
      if (j == kEntriesPerCf - 1 && i == kNumCfs - 1) {
        // Leave one unflushed so we can verify WAL entries are properly
        // associated with column families.
        continue;
      }
      Flush(i);
    }
  }

  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() doesn't re-create the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  ReopenWithColumnFamilies({"default", "pikachu1", "pikachu2"},
                           CurrentOptions());
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + ToString(j)), "val" + ToString(j));
    }
  }
}

TEST_F(RepairTest, RepairColumnFamilyOptions) {
  // Verify repair logic uses correct ColumnFamilyOptions when repairing a
  // database with different options for column families.
  const int kNumCfs = 2;
  const int kEntriesPerCf = 2;

  Options opts(CurrentOptions()), rev_opts(CurrentOptions());
  opts.comparator = BytewiseComparator();
  rev_opts.comparator = ReverseBytewiseComparator();

  DestroyAndReopen(opts);
  CreateColumnFamilies({"reverse"}, rev_opts);
  ReopenWithColumnFamilies({"default", "reverse"},
                           std::vector<Options>{opts, rev_opts});
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      Put(i, "key" + ToString(j), "val" + ToString(j));
      if (i == kNumCfs - 1 && j == kEntriesPerCf - 1) {
        // Leave one unflushed so we can verify RepairDB's flush logic
        continue;
      }
      Flush(i);
    }
  }
  Close();

  // RepairDB() records the comparator in the manifest, and DB::Open would fail
  // if a different comparator were used.
  ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}, {"reverse", rev_opts}},
                     opts /* unknown_cf_opts */));
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "reverse"},
                                        std::vector<Options>{opts, rev_opts}));
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + ToString(j)), "val" + ToString(j));
    }
  }

  // Examine table properties to verify RepairDB() used the right options when
  // converting WAL->SST
  TablePropertiesCollection fname_to_props;
  db_->GetPropertiesOfAllTables(handles_[1], &fname_to_props);
  ASSERT_EQ(fname_to_props.size(), 2U);
  for (const auto& fname_and_props : fname_to_props) {
    std::string comparator_name (
      InternalKeyComparator(rev_opts.comparator).Name());
    comparator_name = comparator_name.substr(comparator_name.find(':') + 1);
    ASSERT_EQ(comparator_name,
              fname_and_props.second->comparator_name);
  }
  Close();

  // Also check comparator when it's provided via "unknown" CF options
  ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}},
                     rev_opts /* unknown_cf_opts */));
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "reverse"},
                                        std::vector<Options>{opts, rev_opts}));
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + ToString(j)), "val" + ToString(j));
    }
  }
}

TEST_F(RepairTest, DbNameContainsTrailingSlash) {
  {
    bool tmp;
    if (env_->AreFilesSame("", "", &tmp).IsNotSupported()) {
      fprintf(stderr,
              "skipping RepairTest.DbNameContainsTrailingSlash due to "
              "unsupported Env::AreFilesSame\n");
      return;
    }
  }

  Put("key", "val");
  Flush();
  Close();

  ASSERT_OK(RepairDB(dbname_ + "/", CurrentOptions()));
  Reopen(CurrentOptions());
  ASSERT_EQ(Get("key"), "val");
}
#endif  // ROCKSDB_LITE
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as RepairDB is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
