//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <functional>

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_writer.h"
#include "util/testutil.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
class ExternalSSTFileBasicTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  ExternalSSTFileBasicTest() : DBTestBase("/external_sst_file_basic_test") {
    sst_files_dir_ = dbname_ + "/sst_files/";
    DestroyAndRecreateExternalSSTFilesDir();
  }

  void DestroyAndRecreateExternalSSTFilesDir() {
    test::DestroyDir(env_, sst_files_dir_);
    env_->CreateDir(sst_files_dir_);
  }

  Status DeprecatedAddFile(const std::vector<std::string>& files,
                           bool move_files = false,
                           bool skip_snapshot_check = false) {
    IngestExternalFileOptions opts;
    opts.move_files = move_files;
    opts.snapshot_consistency = !skip_snapshot_check;
    opts.allow_global_seqno = false;
    opts.allow_blocking_flush = false;
    return db_->IngestExternalFile(files, opts);
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys,
      const std::vector<ValueType>& value_types,
      std::vector<std::pair<int, int>> range_deletions, int file_id,
      bool write_global_seqno, bool verify_checksums_before_ingest,
      std::map<std::string, std::string>* true_data) {
    assert(value_types.size() == 1 || keys.size() == value_types.size());
    std::string file_path = sst_files_dir_ + ToString(file_id);
    SstFileWriter sst_file_writer(EnvOptions(), options);

    Status s = sst_file_writer.Open(file_path);
    if (!s.ok()) {
      return s;
    }
    for (size_t i = 0; i < range_deletions.size(); i++) {
      // Account for the effect of range deletions on true_data before
      // all point operators, even though sst_file_writer.DeleteRange
      // must be called before other sst_file_writer methods. This is
      // because point writes take precedence over range deletions
      // in the same ingested sst.
      std::string start_key = Key(range_deletions[i].first);
      std::string end_key = Key(range_deletions[i].second);
      s = sst_file_writer.DeleteRange(start_key, end_key);
      if (!s.ok()) {
        sst_file_writer.Finish();
        return s;
      }
      auto start_key_it = true_data->find(start_key);
      if (start_key_it == true_data->end()) {
        start_key_it = true_data->upper_bound(start_key);
      }
      auto end_key_it = true_data->find(end_key);
      if (end_key_it == true_data->end()) {
        end_key_it = true_data->upper_bound(end_key);
      }
      true_data->erase(start_key_it, end_key_it);
    }
    for (size_t i = 0; i < keys.size(); i++) {
      std::string key = Key(keys[i]);
      std::string value = Key(keys[i]) + ToString(file_id);
      ValueType value_type =
          (value_types.size() == 1 ? value_types[0] : value_types[i]);
      switch (value_type) {
        case ValueType::kTypeValue:
          s = sst_file_writer.Put(key, value);
          (*true_data)[key] = value;
          break;
        case ValueType::kTypeMerge:
          s = sst_file_writer.Merge(key, value);
          // we only use TestPutOperator in this test
          (*true_data)[key] = value;
          break;
        case ValueType::kTypeDeletion:
          s = sst_file_writer.Delete(key);
          true_data->erase(key);
          break;
        default:
          return Status::InvalidArgument("Value type is not supported");
      }
      if (!s.ok()) {
        sst_file_writer.Finish();
        return s;
      }
    }
    s = sst_file_writer.Finish();

    if (s.ok()) {
      IngestExternalFileOptions ifo;
      ifo.allow_global_seqno = true;
      ifo.write_global_seqno = write_global_seqno;
      ifo.verify_checksums_before_ingest = verify_checksums_before_ingest;
      s = db_->IngestExternalFile({file_path}, ifo);
    }
    return s;
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys,
      const std::vector<ValueType>& value_types, int file_id,
      bool write_global_seqno, bool verify_checksums_before_ingest,
      std::map<std::string, std::string>* true_data) {
    return GenerateAndAddExternalFile(
        options, keys, value_types, {}, file_id, write_global_seqno,
        verify_checksums_before_ingest, true_data);
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys, const ValueType value_type,
      int file_id, bool write_global_seqno, bool verify_checksums_before_ingest,
      std::map<std::string, std::string>* true_data) {
    return GenerateAndAddExternalFile(
        options, keys, std::vector<ValueType>(1, value_type), file_id,
        write_global_seqno, verify_checksums_before_ingest, true_data);
  }

  ~ExternalSSTFileBasicTest() override {
    test::DestroyDir(env_, sst_files_dir_);
  }

 protected:
  std::string sst_files_dir_;
};

TEST_F(ExternalSSTFileBasicTest, Basic) {
  Options options = CurrentOptions();

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // Current file size should be 0 after sst_file_writer init and before open a
  // file.
  ASSERT_EQ(sst_file_writer.FileSize(), 0);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Current file size should be non-zero after success write.
  ASSERT_GT(sst_file_writer.FileSize(), 0);

  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(0));
  ASSERT_EQ(file1_info.largest_key, Key(99));
  ASSERT_EQ(file1_info.num_range_del_entries, 0);
  ASSERT_EQ(file1_info.smallest_range_del_key, "");
  ASSERT_EQ(file1_info.largest_range_del_key, "");
  // sst_file_writer already finished, cannot add this value
  s = sst_file_writer.Put(Key(100), "bad_val");
  ASSERT_FALSE(s.ok()) << s.ToString();
  s = sst_file_writer.DeleteRange(Key(100), Key(200));
  ASSERT_FALSE(s.ok()) << s.ToString();

  DestroyAndReopen(options);
  // Add file using file path
  s = DeprecatedAddFile({file1});
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
  for (int k = 0; k < 100; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }

  DestroyAndRecreateExternalSSTFilesDir();
}

TEST_F(ExternalSSTFileBasicTest, NoCopy) {
  Options options = CurrentOptions();
  const ImmutableCFOptions ioptions(options);

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(0));
  ASSERT_EQ(file1_info.largest_key, Key(99));

  // file2.sst (100 => 299)
  std::string file2 = sst_files_dir_ + "file2.sst";
  ASSERT_OK(sst_file_writer.Open(file2));
  for (int k = 100; k < 300; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file2_info;
  s = sst_file_writer.Finish(&file2_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(file2_info.file_path, file2);
  ASSERT_EQ(file2_info.num_entries, 200);
  ASSERT_EQ(file2_info.smallest_key, Key(100));
  ASSERT_EQ(file2_info.largest_key, Key(299));

  // file3.sst (110 => 124) .. overlap with file2.sst
  std::string file3 = sst_files_dir_ + "file3.sst";
  ASSERT_OK(sst_file_writer.Open(file3));
  for (int k = 110; k < 125; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
  }
  ExternalSstFileInfo file3_info;
  s = sst_file_writer.Finish(&file3_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(file3_info.file_path, file3);
  ASSERT_EQ(file3_info.num_entries, 15);
  ASSERT_EQ(file3_info.smallest_key, Key(110));
  ASSERT_EQ(file3_info.largest_key, Key(124));

  s = DeprecatedAddFile({file1}, true /* move file */);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(Status::NotFound(), env_->FileExists(file1));

  s = DeprecatedAddFile({file2}, false /* copy file */);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_OK(env_->FileExists(file2));

  // This file has overlapping values with the existing data
  s = DeprecatedAddFile({file3}, true /* move file */);
  ASSERT_FALSE(s.ok()) << s.ToString();
  ASSERT_OK(env_->FileExists(file3));

  for (int k = 0; k < 300; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithGlobalSeqnoPickedSeqno) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;

    int file_id = 1;

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {10, 11, 12, 13}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 4, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {11, 15, 19}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120, 130}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 130}, ValueType::kTypeValue, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    // Write some keys through normal write path
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(i), "memtable"));
      true_data[Key(i)] = "memtable";
    }
    SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {60, 61, 62}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {40, 41, 42}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {20, 30, 40}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 2);

    const Snapshot* snapshot = db_->GetSnapshot();

    // We will need a seqno for the file regardless if the file overwrite
    // keys in the DB or not because we have a snapshot
    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1000, 1002}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {2000, 3002}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 20, 40, 100, 150}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    db_->ReleaseSnapshot(snapshot);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {5000, 5001}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // No snapshot anymore, no need to assign a seqno
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithMultipleValueType) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    options.merge_operator.reset(new TestPutOperator());
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;

    int file_id = 1;

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {10, 11, 12, 13}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 4, 6}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {11, 15, 19}, ValueType::kTypeDeletion, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120, 130}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 130}, ValueType::kTypeDeletion, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120}, {ValueType::kTypeValue}, {{120, 135}}, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {}, {}, {{110, 120}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // The range deletion ends on a key, but it doesn't actually delete
    // this key because the largest key in the range is exclusive. Still,
    // it counts as an overlap so a new seqno will be assigned.
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {}, {}, {{100, 109}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

    // Write some keys through normal write path
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(i), "memtable"));
      true_data[Key(i)] = "memtable";
    }
    SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {60, 61, 62}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {40, 41, 42}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {20, 30, 40}, ValueType::kTypeDeletion, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 2);

    const Snapshot* snapshot = db_->GetSnapshot();

    // We will need a seqno for the file regardless if the file overwrite
    // keys in the DB or not because we have a snapshot
    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1000, 1002}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {2000, 3002}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 20, 40, 100, 150}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    db_->ReleaseSnapshot(snapshot);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {5000, 5001}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // No snapshot anymore, no need to assign a seqno
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithMixedValueType) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    options.merge_operator.reset(new TestPutOperator());
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;

    int file_id = 1;

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue,
         ValueType::kTypeMerge, ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {10, 11, 12, 13},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue,
         ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 4, 6},
        {ValueType::kTypeDeletion, ValueType::kTypeValue,
         ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {11, 15, 19},
        {ValueType::kTypeDeletion, ValueType::kTypeMerge,
         ValueType::kTypeValue},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120, 130}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 130}, {ValueType::kTypeMerge, ValueType::kTypeDeletion},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {150, 151, 152},
        {ValueType::kTypeValue, ValueType::kTypeMerge,
         ValueType::kTypeDeletion},
        {{150, 160}, {180, 190}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {150, 151, 152},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue},
        {{200, 250}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {300, 301, 302},
        {ValueType::kTypeValue, ValueType::kTypeMerge,
         ValueType::kTypeDeletion},
        {{1, 2}, {152, 154}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

    // Write some keys through normal write path
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(i), "memtable"));
      true_data[Key(i)] = "memtable";
    }
    SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {60, 61, 62},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {40, 41, 42},
        {ValueType::kTypeValue, ValueType::kTypeDeletion,
         ValueType::kTypeDeletion},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {20, 30, 40},
        {ValueType::kTypeDeletion, ValueType::kTypeDeletion,
         ValueType::kTypeDeletion},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 2);

    const Snapshot* snapshot = db_->GetSnapshot();

    // We will need a seqno for the file regardless if the file overwrite
    // keys in the DB or not because we have a snapshot
    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1000, 1002}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {2000, 3002}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 20, 40, 100, 150},
        {ValueType::kTypeDeletion, ValueType::kTypeDeletion,
         ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    db_->ReleaseSnapshot(snapshot);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {5000, 5001}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // No snapshot anymore, no need to assign a seqno
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_F(ExternalSSTFileBasicTest, FadviseTrigger) {
  Options options = CurrentOptions();
  const int kNumKeys = 10000;

  size_t total_fadvised_bytes = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileWriter::Rep::InvalidatePageCache", [&](void* arg) {
        size_t fadvise_size = *(reinterpret_cast<size_t*>(arg));
        total_fadvised_bytes += fadvise_size;
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<SstFileWriter> sst_file_writer;

  std::string sst_file_path = sst_files_dir_ + "file_fadvise_disable.sst";
  sst_file_writer.reset(
      new SstFileWriter(EnvOptions(), options, nullptr, false));
  ASSERT_OK(sst_file_writer->Open(sst_file_path));
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(sst_file_writer->Put(Key(i), Key(i)));
  }
  ASSERT_OK(sst_file_writer->Finish());
  // fadvise disabled
  ASSERT_EQ(total_fadvised_bytes, 0);

  sst_file_path = sst_files_dir_ + "file_fadvise_enable.sst";
  sst_file_writer.reset(
      new SstFileWriter(EnvOptions(), options, nullptr, true));
  ASSERT_OK(sst_file_writer->Open(sst_file_path));
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(sst_file_writer->Put(Key(i), Key(i)));
  }
  ASSERT_OK(sst_file_writer->Finish());
  // fadvise enabled
  ASSERT_EQ(total_fadvised_bytes, sst_file_writer->FileSize());
  ASSERT_GT(total_fadvised_bytes, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(ExternalSSTFileBasicTest, IngestionWithRangeDeletions) {
  int kNumLevels = 7;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = kNumLevels;
  Reopen(options);

  std::map<std::string, std::string> true_data;
  int file_id = 1;
  // prevent range deletions from being dropped due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();

  // range del [0, 50) in L6 file, [50, 100) in L0 file, [100, 150) in memtable
  for (int i = 0; i < 3; i++) {
    if (i != 0) {
      db_->Flush(FlushOptions());
      if (i == 1) {
        MoveFilesToLevel(kNumLevels - 1);
      }
    }
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(50 * i), Key(50 * (i + 1))));
  }
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 1));

  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  // overlaps with L0 file but not memtable, so flush is skipped and file is
  // ingested into L0
  SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {60, 90}, {ValueType::kTypeValue, ValueType::kTypeValue},
      {{65, 70}, {70, 85}}, file_id++, write_global_seqno,
      verify_checksums_before_ingest, &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // overlaps with L6 file but not memtable or L0 file, so flush is skipped and
  // file is ingested into L5
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {10, 40}, {ValueType::kTypeValue, ValueType::kTypeValue},
      file_id++, write_global_seqno, verify_checksums_before_ingest,
      &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // overlaps with L5 file but not memtable or L0 file, so flush is skipped and
  // file is ingested into L4
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {}, {}, {{5, 15}}, file_id++, write_global_seqno,
      verify_checksums_before_ingest, &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // ingested file overlaps with memtable, so flush is triggered before the file
  // is ingested such that the ingested data is considered newest. So L0 file
  // count increases by two.
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {100, 140}, {ValueType::kTypeValue, ValueType::kTypeValue},
      file_id++, write_global_seqno, verify_checksums_before_ingest,
      &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(4, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // snapshot unneeded now that all range deletions are persisted
  db_->ReleaseSnapshot(snapshot);

  // overlaps with nothing, so places at bottom level and skips incrementing
  // seqnum.
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {151, 175}, {ValueType::kTypeValue, ValueType::kTypeValue},
      {{160, 200}}, file_id++, write_global_seqno,
      verify_checksums_before_ingest, &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);
  ASSERT_EQ(4, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(2, NumTableFilesAtLevel(options.num_levels - 1));
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithBadBlockChecksum) {
  bool change_checksum_called = false;
  const auto& change_checksum = [&](void* arg) {
    if (!change_checksum_called) {
      char* buf = reinterpret_cast<char*>(arg);
      assert(nullptr != buf);
      buf[0] ^= 0x1;
      change_checksum_called = true;
    }
  };
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteRawBlock:TamperWithChecksum",
      change_checksum);
  SyncPoint::GetInstance()->EnableProcessing();
  int file_id = 0;
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;
    Status s = GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data);
    if (verify_checksums_before_ingest) {
      ASSERT_NOK(s);
    } else {
      ASSERT_OK(s);
    }
    change_checksum_called = false;
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithFirstByteTampered) {
  SyncPoint::GetInstance()->DisableProcessing();
  int file_id = 0;
  EnvOptions env_options;
  do {
    Options options = CurrentOptions();
    std::string file_path = sst_files_dir_ + ToString(file_id++);
    SstFileWriter sst_file_writer(env_options, options);
    Status s = sst_file_writer.Open(file_path);
    ASSERT_OK(s);
    for (int i = 0; i != 100; ++i) {
      std::string key = Key(i);
      std::string value = Key(i) + ToString(0);
      ASSERT_OK(sst_file_writer.Put(key, value));
    }
    ASSERT_OK(sst_file_writer.Finish());
    {
      // Get file size
      uint64_t file_size = 0;
      ASSERT_OK(env_->GetFileSize(file_path, &file_size));
      ASSERT_GT(file_size, 8);
      std::unique_ptr<RandomRWFile> rwfile;
      ASSERT_OK(env_->NewRandomRWFile(file_path, &rwfile, EnvOptions()));
      // Manually corrupt the file
      // We deterministically corrupt the first byte because we currently
      // cannot choose a random offset. The reason for this limitation is that
      // we do not checksum property block at present.
      const uint64_t offset = 0;
      char scratch[8] = {0};
      Slice buf;
      ASSERT_OK(rwfile->Read(offset, sizeof(scratch), &buf, scratch));
      scratch[0] ^= 0xff;  // flip one bit
      ASSERT_OK(rwfile->Write(offset, buf));
    }
    // Ingest file.
    IngestExternalFileOptions ifo;
    ifo.write_global_seqno = std::get<0>(GetParam());
    ifo.verify_checksums_before_ingest = std::get<1>(GetParam());
    s = db_->IngestExternalFile({file_path}, ifo);
    if (ifo.verify_checksums_before_ingest) {
      ASSERT_NOK(s);
    } else {
      ASSERT_OK(s);
    }
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestExternalFileWithCorruptedPropsBlock) {
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  if (!verify_checksums_before_ingest) {
    return;
  }
  uint64_t props_block_offset = 0;
  size_t props_block_size = 0;
  const auto& get_props_block_offset = [&](void* arg) {
    props_block_offset = *reinterpret_cast<uint64_t*>(arg);
  };
  const auto& get_props_block_size = [&](void* arg) {
    props_block_size = *reinterpret_cast<uint64_t*>(arg);
  };
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockOffset",
      get_props_block_offset);
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockSize",
      get_props_block_size);
  SyncPoint::GetInstance()->EnableProcessing();
  int file_id = 0;
  Random64 rand(time(nullptr));
  do {
    std::string file_path = sst_files_dir_ + ToString(file_id++);
    Options options = CurrentOptions();
    SstFileWriter sst_file_writer(EnvOptions(), options);
    Status s = sst_file_writer.Open(file_path);
    ASSERT_OK(s);
    for (int i = 0; i != 100; ++i) {
      std::string key = Key(i);
      std::string value = Key(i) + ToString(0);
      ASSERT_OK(sst_file_writer.Put(key, value));
    }
    ASSERT_OK(sst_file_writer.Finish());

    {
      std::unique_ptr<RandomRWFile> rwfile;
      ASSERT_OK(env_->NewRandomRWFile(file_path, &rwfile, EnvOptions()));
      // Manually corrupt the file
      ASSERT_GT(props_block_size, 8);
      uint64_t offset =
          props_block_offset + rand.Next() % (props_block_size - 8);
      char scratch[8] = {0};
      Slice buf;
      ASSERT_OK(rwfile->Read(offset, sizeof(scratch), &buf, scratch));
      scratch[0] ^= 0xff;  // flip one bit
      ASSERT_OK(rwfile->Write(offset, buf));
    }

    // Ingest file.
    IngestExternalFileOptions ifo;
    ifo.write_global_seqno = std::get<0>(GetParam());
    ifo.verify_checksums_before_ingest = true;
    s = db_->IngestExternalFile({file_path}, ifo);
    ASSERT_NOK(s);
  } while (ChangeOptionsForFileIngestionTest());
}

INSTANTIATE_TEST_CASE_P(ExternalSSTFileBasicTest, ExternalSSTFileBasicTest,
                        testing::Values(std::make_tuple(true, true),
                                        std::make_tuple(true, false),
                                        std::make_tuple(false, true),
                                        std::make_tuple(false, false)));

#endif  // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
