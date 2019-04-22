//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction_picker.h"

#include <limits>
#include <string>
#include <utility>
#include "db/compaction.h"
#include "db/compaction_picker_fifo.h"
#include "db/compaction_picker_universal.h"

#include "util/logging.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class CountingLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override { log_count++; }
  size_t log_count;
};

class CompactionPickerTest : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableCFOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  LevelCompactionPicker level_compaction_picker;
  std::string cf_name_;
  CountingLogger logger_;
  LogBuffer log_buffer_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::unique_ptr<VersionStorageInfo> vstorage_;
  std::vector<std::unique_ptr<FileMetaData>> files_;
  // does not own FileMetaData
  std::unordered_map<uint32_t, std::pair<FileMetaData*, int>> file_map_;
  // input files to compaction process.
  std::vector<CompactionInputFiles> input_files_;
  int compaction_level_start_;

  CompactionPickerTest()
      : ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        ioptions_(options_),
        mutable_cf_options_(options_),
        level_compaction_picker(ioptions_, &icmp_),
        cf_name_("dummy"),
        log_buffer_(InfoLogLevel::INFO_LEVEL, &logger_),
        file_num_(1),
        vstorage_(nullptr) {
    // ioptions_.compaction_pri = kMinOverlappingRatio has its own set of
    // tests to cover.
    ioptions_.compaction_pri = kByCompensatedSize;
    fifo_options_.max_table_files_size = 1;
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    ioptions_.cf_paths.emplace_back("dummy",
                                    std::numeric_limits<uint64_t>::max());
  }

  ~CompactionPickerTest() override {}

  void NewVersionStorage(int num_levels, CompactionStyle style) {
    DeleteVersionStorage();
    options_.num_levels = num_levels;
    vstorage_.reset(new VersionStorageInfo(&icmp_, ucmp_, options_.num_levels,
                                           style, nullptr, false));
    vstorage_->CalculateBaseBytes(ioptions_, mutable_cf_options_);
  }

  void DeleteVersionStorage() {
    vstorage_.reset();
    files_.clear();
    file_map_.clear();
    input_files_.clear();
  }

  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 1, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100, SequenceNumber largest_seq = 100,
           size_t compensated_file_size = 0) {
    assert(level < vstorage_->num_levels());
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(file_number, path_id, file_size);
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
    f->largest = InternalKey(largest, largest_seq, kTypeValue);
    f->fd.smallest_seqno = smallest_seq;
    f->fd.largest_seqno = largest_seq;
    f->compensated_file_size =
        (compensated_file_size != 0) ? compensated_file_size : file_size;
    f->refs = 0;
    vstorage_->AddFile(level, f);
    files_.emplace_back(f);
    file_map_.insert({file_number, {f, level}});
  }

  void SetCompactionInputFilesLevels(int level_count, int start_level) {
    input_files_.resize(level_count);
    for (int i = 0; i < level_count; ++i) {
      input_files_[i].level = start_level + i;
    }
    compaction_level_start_ = start_level;
  }

  void AddToCompactionFiles(uint32_t file_number) {
    auto iter = file_map_.find(file_number);
    assert(iter != file_map_.end());
    int level = iter->second.second;
    assert(level < vstorage_->num_levels());
    input_files_[level - compaction_level_start_].files.emplace_back(
        iter->second.first);
  }

  void UpdateVersionStorageInfo() {
    vstorage_->CalculateBaseBytes(ioptions_, mutable_cf_options_);
    vstorage_->UpdateFilesByCompactionPri(ioptions_.compaction_pri);
    vstorage_->UpdateNumNonEmptyLevels();
    vstorage_->GenerateFileIndexer();
    vstorage_->GenerateLevelFilesBrief();
    vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
    vstorage_->GenerateLevel0NonOverlapping();
    vstorage_->ComputeFilesMarkedForCompaction();
    vstorage_->SetFinalized();
  }
};

TEST_F(CompactionPickerTest, Empty) {
  NewVersionStorage(6, kCompactionStyleLevel);
  UpdateVersionStorageInfo();
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, Single) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  Add(0, 1U, "p", "q");
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, Level0Trigger) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, Level1Trigger) {
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(1, 66U, "150", "200", 1000000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(66U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, Level1Trigger2) {
  mutable_cf_options_.target_file_size_base = 10000000000;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(1, 66U, "150", "200", 1000000001U);
  Add(1, 88U, "201", "300", 1000000000U);
  Add(2, 6U, "150", "179", 1000000000U);
  Add(2, 7U, "180", "220", 1000000000U);
  Add(2, 8U, "221", "300", 1000000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(66U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(uint64_t{1073741824}, compaction->OutputFilePreallocationSize());
}

TEST_F(CompactionPickerTest, LevelMaxScore) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.target_file_size_base = 10000000;
  mutable_cf_options_.max_bytes_for_level_base = 10 * 1024 * 1024;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  Add(0, 1U, "150", "200", 1000000U);
  // Level 1 score 1.2
  Add(1, 66U, "150", "200", 6000000U);
  Add(1, 88U, "201", "300", 6000000U);
  // Level 2 score 1.8. File 7 is the largest. Should be picked
  Add(2, 6U, "150", "179", 60000000U);
  Add(2, 7U, "180", "220", 60000001U);
  Add(2, 8U, "221", "300", 60000000U);
  // Level 3 score slightly larger than 1
  Add(3, 26U, "150", "170", 260000000U);
  Add(3, 27U, "171", "179", 260000000U);
  Add(3, 28U, "191", "220", 260000000U);
  Add(3, 29U, "221", "300", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(mutable_cf_options_.target_file_size_base +
                mutable_cf_options_.target_file_size_base / 10,
            compaction->OutputFilePreallocationSize());
}

TEST_F(CompactionPickerTest, NeedsCompactionLevel) {
  const int kLevels = 6;
  const int kFileCount = 20;

  for (int level = 0; level < kLevels - 1; ++level) {
    NewVersionStorage(kLevels, kCompactionStyleLevel);
    uint64_t file_size = vstorage_->MaxBytesForLevel(level) * 2 / kFileCount;
    for (int file_count = 1; file_count <= kFileCount; ++file_count) {
      // start a brand new version in each test.
      NewVersionStorage(kLevels, kCompactionStyleLevel);
      for (int i = 0; i < file_count; ++i) {
        Add(level, i, ToString((i + 100) * 1000).c_str(),
            ToString((i + 100) * 1000 + 999).c_str(),
            file_size, 0, i * 100, i * 100 + 99);
      }
      UpdateVersionStorageInfo();
      ASSERT_EQ(vstorage_->CompactionScoreLevel(0), level);
      ASSERT_EQ(level_compaction_picker.NeedsCompaction(vstorage_.get()),
                vstorage_->CompactionScore(0) >= 1);
      // release the version storage
      DeleteVersionStorage();
    }
  }
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(1, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 1, compaction->output_level());
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic2) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");
  Add(num_levels - 1, 3U, "200", "250", 300U);

  UpdateVersionStorageInfo();
  ASSERT_EQ(vstorage_->base_level(), num_levels - 2);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(1, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 2, compaction->output_level());
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic3) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");
  Add(num_levels - 1, 3U, "200", "250", 300U);
  Add(num_levels - 1, 4U, "300", "350", 3000U);

  UpdateVersionStorageInfo();
  ASSERT_EQ(vstorage_->base_level(), num_levels - 3);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(1, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 3, compaction->output_level());
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic4) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");
  Add(num_levels - 1, 3U, "200", "250", 300U);
  Add(num_levels - 1, 4U, "300", "350", 3000U);
  Add(num_levels - 3, 5U, "150", "180", 3U);
  Add(num_levels - 3, 6U, "181", "300", 3U);
  Add(num_levels - 3, 7U, "400", "450", 3U);

  UpdateVersionStorageInfo();
  ASSERT_EQ(vstorage_->base_level(), num_levels - 3);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(num_levels - 3, compaction->level(1));
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(2, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 3, compaction->output_level());
}

TEST_F(CompactionPickerTest, LevelTriggerDynamic4) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(num_levels - 1, 3U, "200", "250", 300U);
  Add(num_levels - 1, 4U, "300", "350", 3000U);
  Add(num_levels - 1, 4U, "400", "450", 3U);
  Add(num_levels - 2, 5U, "150", "180", 300U);
  Add(num_levels - 2, 6U, "181", "350", 500U);
  Add(num_levels - 2, 7U, "400", "450", 200U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(5U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(0, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(num_levels - 1, compaction->output_level());
}

// Universal and FIFO Compactions are not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
TEST_F(CompactionPickerTest, NeedsCompactionUniversal) {
  NewVersionStorage(1, kCompactionStyleUniversal);
  UniversalCompactionPicker universal_compaction_picker(
      ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(universal_compaction_picker.NeedsCompaction(vstorage_.get()),
            false);

  // verify the trigger given different number of L0 files.
  for (int i = 1;
       i <= mutable_cf_options_.level0_file_num_compaction_trigger * 2; ++i) {
    NewVersionStorage(1, kCompactionStyleUniversal);
    Add(0, i, ToString((i + 100) * 1000).c_str(),
        ToString((i + 100) * 1000 + 999).c_str(), 1000000, 0, i * 100,
        i * 100 + 99);
    UpdateVersionStorageInfo();
    ASSERT_EQ(level_compaction_picker.NeedsCompaction(vstorage_.get()),
              vstorage_->CompactionScore(0) >= 1);
  }
}

TEST_F(CompactionPickerTest, CompactionUniversalIngestBehindReservedLevel) {
  const uint64_t kFileSize = 100000;
  NewVersionStorage(1, kCompactionStyleUniversal);
  ioptions_.allow_ingest_behind = true;
  ioptions_.num_levels = 3;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(universal_compaction_picker.NeedsCompaction(vstorage_.get()),
            false);

  NewVersionStorage(3, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(1, 5U, "100", "151", kFileSize, 0, 200, 251);
  Add(1, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(2, 6U, "120", "200", kFileSize, 0, 20, 100);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));

  // output level should be the one above the bottom-most
  ASSERT_EQ(1, compaction->output_level());
}
// Tests if the files can be trivially moved in multi level
// universal compaction when allow_trivial_move option is set
// In this test as the input files overlaps, they cannot
// be trivially moved.

TEST_F(CompactionPickerTest, CannotTrivialMoveUniversal) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.compaction_options_universal.allow_trivial_move = true;
  NewVersionStorage(1, kCompactionStyleUniversal);
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(universal_compaction_picker.NeedsCompaction(vstorage_.get()),
            false);

  NewVersionStorage(3, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(1, 5U, "100", "151", kFileSize, 0, 200, 251);
  Add(1, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(2, 6U, "120", "200", kFileSize, 0, 20, 100);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));

  ASSERT_TRUE(!compaction->is_trivial_move());
}
// Tests if the files can be trivially moved in multi level
// universal compaction when allow_trivial_move option is set
// In this test as the input files doesn't overlaps, they should
// be trivially moved.
TEST_F(CompactionPickerTest, AllowsTrivialMoveUniversal) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.compaction_options_universal.allow_trivial_move = true;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(3, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(1, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(2, 3U, "301", "350", kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));

  ASSERT_TRUE(compaction->is_trivial_move());
}

TEST_F(CompactionPickerTest, NeedsCompactionFIFO) {
  NewVersionStorage(1, kCompactionStyleFIFO);
  const int kFileCount =
      mutable_cf_options_.level0_file_num_compaction_trigger * 3;
  const uint64_t kFileSize = 100000;
  const uint64_t kMaxSize = kFileSize * kFileCount / 2;

  fifo_options_.max_table_files_size = kMaxSize;
  mutable_cf_options_.compaction_options_fifo = fifo_options_;
  FIFOCompactionPicker fifo_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), false);

  // verify whether compaction is needed based on the current
  // size of L0 files.
  uint64_t current_size = 0;
  for (int i = 1; i <= kFileCount; ++i) {
    NewVersionStorage(1, kCompactionStyleFIFO);
    Add(0, i, ToString((i + 100) * 1000).c_str(),
        ToString((i + 100) * 1000 + 999).c_str(),
        kFileSize, 0, i * 100, i * 100 + 99);
    current_size += kFileSize;
    UpdateVersionStorageInfo();
    ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()),
              vstorage_->CompactionScore(0) >= 1);
  }
}
#endif  // ROCKSDB_LITE

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping1) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.target_file_size_base = 100000000000;
  mutable_cf_options_.target_file_size_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_base = 10 * 1024 * 1024;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);

  Add(2, 6U, "150", "179", 50000000U);
  Add(2, 7U, "180", "220", 50000000U);
  Add(2, 8U, "321", "400", 50000000U);  // File not overlapping
  Add(2, 9U, "721", "800", 50000000U);

  Add(3, 26U, "150", "170", 260000000U);
  Add(3, 27U, "171", "179", 260000000U);
  Add(3, 28U, "191", "220", 260000000U);
  Add(3, 29U, "221", "300", 260000000U);
  Add(3, 30U, "750", "900", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Pick file 8 because it overlaps with 0 files on level 3.
  ASSERT_EQ(8U, compaction->input(0, 0)->fd.GetNumber());
  // Compaction input size * 1.1
  ASSERT_GE(uint64_t{55000000}, compaction->OutputFilePreallocationSize());
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping2) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.target_file_size_base = 10000000;
  mutable_cf_options_.target_file_size_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_base = 10 * 1024 * 1024;

  Add(2, 6U, "150", "175",
      60000000U);  // Overlaps with file 26, 27, total size 521M
  Add(2, 7U, "176", "200", 60000000U);  // Overlaps with file 27, 28, total size
                                        // 520M, the smalelst overlapping
  Add(2, 8U, "201", "300",
      60000000U);  // Overlaps with file 28, 29, total size 521M

  Add(3, 26U, "100", "110", 261000000U);
  Add(3, 26U, "150", "170", 261000000U);
  Add(3, 27U, "171", "179", 260000000U);
  Add(3, 28U, "191", "220", 260000000U);
  Add(3, 29U, "221", "300", 261000000U);
  Add(3, 30U, "321", "400", 261000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 7 because overlapping ratio is the biggest.
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping3) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 10000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  // file 7 and 8 over lap with the same file, but file 8 is smaller so
  // it will be picked.
  Add(2, 6U, "150", "167", 60000000U);  // Overlaps with file 26, 27
  Add(2, 7U, "168", "169", 60000000U);  // Overlaps with file 27
  Add(2, 8U, "201", "300", 61000000U);  // Overlaps with file 28, but the file
                                        // itself is larger. Should be picked.

  Add(3, 26U, "160", "165", 260000000U);
  Add(3, 27U, "166", "170", 260000000U);
  Add(3, 28U, "180", "400", 260000000U);
  Add(3, 29U, "401", "500", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 8 because overlapping ratio is the biggest.
  ASSERT_EQ(8U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping4) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 10000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  // file 7 and 8 over lap with the same file, but file 8 is smaller so
  // it will be picked.
  // Overlaps with file 26, 27. And the file is compensated so will be
  // picked up.
  Add(2, 6U, "150", "167", 60000000U, 0, 100, 100, 180000000U);
  Add(2, 7U, "168", "169", 60000000U);  // Overlaps with file 27
  Add(2, 8U, "201", "300", 61000000U);  // Overlaps with file 28

  Add(3, 26U, "160", "165", 60000000U);
  // Boosted file size in output level is not considered.
  Add(3, 27U, "166", "170", 60000000U, 0, 100, 100, 260000000U);
  Add(3, 28U, "180", "400", 60000000U);
  Add(3, 29U, "401", "500", 60000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 8 because overlapping ratio is the biggest.
  ASSERT_EQ(6U, compaction->input(0, 0)->fd.GetNumber());
}

// This test exhibits the bug where we don't properly reset parent_index in
// PickCompaction()
TEST_F(CompactionPickerTest, ParentIndexResetBug) {
  int num_levels = ioptions_.num_levels;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");       // <- marked for compaction
  Add(1, 3U, "400", "500", 600);  // <- this one needs compacting
  Add(2, 4U, "150", "200");
  Add(2, 5U, "201", "210");
  Add(2, 6U, "300", "310");
  Add(2, 7U, "400", "500");  // <- being compacted

  vstorage_->LevelFiles(2)[3]->being_compacted = true;
  vstorage_->LevelFiles(0)[0]->marked_for_compaction = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
}

// This test checks ExpandWhileOverlapping() by having overlapping user keys
// ranges (with different sequence numbers) in the input files.
TEST_F(CompactionPickerTest, OverlappingUserKeys) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kByCompensatedSize;

  Add(1, 1U, "100", "150", 1U);
  // Overlapping user keys
  Add(1, 2U, "200", "400", 1U);
  Add(1, 3U, "400", "500", 1000000000U, 0, 0);
  Add(2, 4U, "600", "700", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
              cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys2) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1000000000U);
  Add(1, 2U, "400", "500", 1U, 0, 0);
  Add(2, 3U, "000", "100", 1U);
  Add(2, 4U, "100", "600", 1U, 0, 0);
  Add(2, 5U, "600", "700", 1U, 0, 0);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
              cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(3U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(1, 2)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys3) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Chain of overlapping user key ranges (forces ExpandWhileOverlapping() to
  // expand multiple times)
  Add(1, 1U, "100", "150", 1U);
  Add(1, 2U, "150", "200", 1U, 0, 0);
  Add(1, 3U, "200", "250", 1000000000U, 0, 0);
  Add(1, 4U, "250", "300", 1U, 0, 0);
  Add(1, 5U, "300", "350", 1U, 0, 0);
  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "350", "400", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
              cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(5U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 3)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(0, 4)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys4) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_bytes_for_level_base = 1000000;

  Add(1, 1U, "100", "150", 1U);
  Add(1, 2U, "150", "199", 1U, 0, 0);
  Add(1, 3U, "200", "250", 1100000U, 0, 0);
  Add(1, 4U, "251", "300", 1U, 0, 0);
  Add(1, 5U, "300", "350", 1U, 0, 0);

  Add(2, 6U, "100", "115", 1U);
  Add(2, 7U, "125", "325", 1U);
  Add(2, 8U, "350", "400", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys5) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1000000000U);
  Add(1, 2U, "400", "500", 1U, 0, 0);
  Add(2, 3U, "000", "100", 1U);
  Add(2, 4U, "100", "600", 1U, 0, 0);
  Add(2, 5U, "600", "700", 1U, 0, 0);

  vstorage_->LevelFiles(2)[2]->being_compacted = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, OverlappingUserKeys6) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1U, 0, 0);
  Add(1, 2U, "401", "500", 1U, 0, 0);
  Add(2, 3U, "000", "100", 1U);
  Add(2, 4U, "100", "300", 1U, 0, 0);
  Add(2, 5U, "305", "450", 1U, 0, 0);
  Add(2, 6U, "460", "600", 1U, 0, 0);
  Add(2, 7U, "600", "700", 1U, 0, 0);

  vstorage_->LevelFiles(1)[0]->marked_for_compaction = true;
  vstorage_->LevelFiles(1)[1]->marked_for_compaction = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(3U, compaction->num_input_files(1));
}

TEST_F(CompactionPickerTest, OverlappingUserKeys7) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1U, 0, 0);
  Add(1, 2U, "401", "500", 1000000000U, 0, 0);
  Add(2, 3U, "100", "250", 1U);
  Add(2, 4U, "300", "600", 1U, 0, 0);
  Add(2, 5U, "600", "800", 1U, 0, 0);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_GE(1U, compaction->num_input_files(0));
  ASSERT_GE(2U, compaction->num_input_files(1));
  // File 5 has to be included in the compaction
  ASSERT_EQ(5U, compaction->inputs(1)->back()->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys8) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;
  // grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up
  // Expand input level as much as possible
  // no overlapping case
  Add(1, 1U, "101", "150", 1U);
  Add(1, 2U, "151", "200", 1U);
  Add(1, 3U, "201", "300", 1000000000U);
  Add(1, 4U, "301", "400", 1U);
  Add(1, 5U, "401", "500", 1U);
  Add(2, 6U, "150", "200", 1U);
  Add(2, 7U, "200", "450", 1U, 0, 0);
  Add(2, 8U, "500", "600", 1U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys9) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;
  // grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up
  // Expand input level as much as possible
  // overlapping case
  Add(1, 1U, "121", "150", 1U);
  Add(1, 2U, "151", "200", 1U);
  Add(1, 3U, "201", "300", 1000000000U);
  Add(1, 4U, "301", "400", 1U);
  Add(1, 5U, "401", "500", 1U);
  Add(2, 6U, "100", "120", 1U);
  Add(2, 7U, "150", "200", 1U);
  Add(2, 8U, "200", "450", 1U, 0, 0);
  Add(2, 9U, "501", "600", 1U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(5U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 3)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(8U, compaction->input(1, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys10) {
  // Locked file encountered when pulling in extra input-level files with same
  // user keys. Verify we pick the next-best file from the same input level.
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;

  // file_number 2U is largest and thus first choice. But it overlaps with
  // file_number 1U which is being compacted. So instead we pick the next-
  // biggest file, 3U, which is eligible for compaction.
  Add(1 /* level */, 1U /* file_number */, "100" /* smallest */,
      "150" /* largest */, 1U /* file_size */);
  file_map_[1U].first->being_compacted = true;
  Add(1 /* level */, 2U /* file_number */, "150" /* smallest */,
      "200" /* largest */, 1000000000U /* file_size */, 0 /* smallest_seq */,
      0 /* largest_seq */);
  Add(1 /* level */, 3U /* file_number */, "201" /* smallest */,
      "250" /* largest */, 900000000U /* file_size */);
  Add(2 /* level */, 4U /* file_number */, "100" /* smallest */,
      "150" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "151" /* smallest */,
      "200" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 6U /* file_number */, "201" /* smallest */,
      "250" /* largest */, 1U /* file_size */);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys11) {
  // Locked file encountered when pulling in extra output-level files with same
  // user keys. Expected to skip that compaction and pick the next-best choice.
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;

  // score(L1) = 3.7
  // score(L2) = 1.85
  // There is no eligible file in L1 to compact since both candidates pull in
  // file_number 5U, which overlaps with a file pending compaction (6U). The
  // first eligible compaction is from L2->L3.
  Add(1 /* level */, 2U /* file_number */, "151" /* smallest */,
      "200" /* largest */, 1000000000U /* file_size */);
  Add(1 /* level */, 3U /* file_number */, "201" /* smallest */,
      "250" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 4U /* file_number */, "100" /* smallest */,
      "149" /* largest */, 5000000000U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "150" /* smallest */,
      "201" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 6U /* file_number */, "201" /* smallest */,
      "249" /* largest */, 1U /* file_size */, 0 /* smallest_seq */,
      0 /* largest_seq */);
  file_map_[6U].first->being_compacted = true;
  Add(3 /* level */, 7U /* file_number */, "100" /* smallest */,
      "149" /* largest */, 1U /* file_size */);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(4U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, NotScheduleL1IfL0WithHigherPri1) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 900000000U;

  // 6 L0 files, score 3.
  Add(0, 1U, "000", "400", 1U);
  Add(0, 2U, "001", "400", 1U, 0, 0);
  Add(0, 3U, "001", "400", 1000000000U, 0, 0);
  Add(0, 31U, "001", "400", 1000000000U, 0, 0);
  Add(0, 32U, "001", "400", 1000000000U, 0, 0);
  Add(0, 33U, "001", "400", 1000000000U, 0, 0);

  // L1 total size 2GB, score 2.2. If one file being comapcted, score 1.1.
  Add(1, 4U, "050", "300", 1000000000U, 0, 0);
  file_map_[4u].first->being_compacted = true;
  Add(1, 5U, "301", "350", 1000000000U, 0, 0);

  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "300", "400", 1U);

  // No compaction should be scheduled, if L0 has higher priority than L1
  // but L0->L1 compaction is blocked by a file in L1 being compacted.
  UpdateVersionStorageInfo();
  ASSERT_EQ(0, vstorage_->CompactionScoreLevel(0));
  ASSERT_EQ(1, vstorage_->CompactionScoreLevel(1));
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, NotScheduleL1IfL0WithHigherPri2) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 900000000U;

  // 6 L0 files, score 3.
  Add(0, 1U, "000", "400", 1U);
  Add(0, 2U, "001", "400", 1U, 0, 0);
  Add(0, 3U, "001", "400", 1000000000U, 0, 0);
  Add(0, 31U, "001", "400", 1000000000U, 0, 0);
  Add(0, 32U, "001", "400", 1000000000U, 0, 0);
  Add(0, 33U, "001", "400", 1000000000U, 0, 0);

  // L1 total size 2GB, score 2.2. If one file being comapcted, score 1.1.
  Add(1, 4U, "050", "300", 1000000000U, 0, 0);
  Add(1, 5U, "301", "350", 1000000000U, 0, 0);

  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "300", "400", 1U);

  // If no file in L1 being compacted, L0->L1 compaction will be scheduled.
  UpdateVersionStorageInfo();  // being_compacted flag is cleared here.
  ASSERT_EQ(0, vstorage_->CompactionScoreLevel(0));
  ASSERT_EQ(1, vstorage_->CompactionScoreLevel(1));
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
}

TEST_F(CompactionPickerTest, NotScheduleL1IfL0WithHigherPri3) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 900000000U;

  // 6 L0 files, score 3.
  Add(0, 1U, "000", "400", 1U);
  Add(0, 2U, "001", "400", 1U, 0, 0);
  Add(0, 3U, "001", "400", 1000000000U, 0, 0);
  Add(0, 31U, "001", "400", 1000000000U, 0, 0);
  Add(0, 32U, "001", "400", 1000000000U, 0, 0);
  Add(0, 33U, "001", "400", 1000000000U, 0, 0);

  // L1 score more than 6.
  Add(1, 4U, "050", "300", 1000000000U, 0, 0);
  file_map_[4u].first->being_compacted = true;
  Add(1, 5U, "301", "350", 1000000000U, 0, 0);
  Add(1, 51U, "351", "400", 6000000000U, 0, 0);

  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "300", "400", 1U);

  // If score in L1 is larger than L0, L1 compaction goes through despite
  // there is pending L0 compaction.
  UpdateVersionStorageInfo();
  ASSERT_EQ(1, vstorage_->CompactionScoreLevel(0));
  ASSERT_EQ(0, vstorage_->CompactionScoreLevel(1));
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeeded1) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200", 200);
  Add(0, 2U, "150", "200", 200);
  Add(0, 3U, "150", "200", 200);
  // Level 1 is over target by 200
  Add(1, 4U, "400", "500", 600);
  Add(1, 5U, "600", "700", 600);
  // Level 2 is less than target 10000 even added size of level 1
  // Size ratio of L2/L1 is 9600 / 1200 = 8
  Add(2, 6U, "150", "200", 2500);
  Add(2, 7U, "201", "210", 2000);
  Add(2, 8U, "300", "310", 2600);
  Add(2, 9U, "400", "500", 2500);
  // Level 3 exceeds target 100,000 of 1000
  Add(3, 10U, "400", "500", 101000);
  // Level 4 exceeds target 1,000,000 by 900 after adding size from level 3
  // Size ratio L4/L3 is 9.9
  // After merge from L3, L4 size is 1000900
  Add(4, 11U, "400", "500", 999900);
  Add(5, 11U, "400", "500", 8007200);

  UpdateVersionStorageInfo();

  ASSERT_EQ(200u * 9u + 10900u + 900u * 9,
            vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeeded2) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200", 200);
  Add(0, 2U, "150", "200", 200);
  Add(0, 4U, "150", "200", 200);
  Add(0, 5U, "150", "200", 200);
  Add(0, 6U, "150", "200", 200);
  // Level 1 size will be 1400 after merging with L0
  Add(1, 7U, "400", "500", 200);
  Add(1, 8U, "600", "700", 200);
  // Level 2 is less than target 10000 even added size of level 1
  Add(2, 9U, "150", "200", 9100);
  // Level 3 over the target, but since level 4 is empty, we assume it will be
  // a trivial move.
  Add(3, 10U, "400", "500", 101000);

  UpdateVersionStorageInfo();

  // estimated L1->L2 merge: 400 * (9100.0 / 1400.0 + 1.0)
  ASSERT_EQ(1400u + 3000u, vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeeded3) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200", 2000);
  Add(0, 2U, "150", "200", 2000);
  Add(0, 4U, "150", "200", 2000);
  Add(0, 5U, "150", "200", 2000);
  Add(0, 6U, "150", "200", 1000);
  // Level 1 size will be 10000 after merging with L0
  Add(1, 7U, "400", "500", 500);
  Add(1, 8U, "600", "700", 500);

  Add(2, 9U, "150", "200", 10000);

  UpdateVersionStorageInfo();

  ASSERT_EQ(10000u + 18000u, vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeededDynamicLevel) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);

  // Set Last level size 50000
  // num_levels - 1 target 5000
  // num_levels - 2 is base level with target 1000 (rounded up to
  // max_bytes_for_level_base).
  Add(num_levels - 1, 10U, "400", "500", 50000);

  Add(0, 1U, "150", "200", 200);
  Add(0, 2U, "150", "200", 200);
  Add(0, 4U, "150", "200", 200);
  Add(0, 5U, "150", "200", 200);
  Add(0, 6U, "150", "200", 200);
  // num_levels - 3 is over target by 100 + 1000
  Add(num_levels - 3, 7U, "400", "500", 550);
  Add(num_levels - 3, 8U, "600", "700", 550);
  // num_levels - 2 is over target by 1100 + 200
  Add(num_levels - 2, 9U, "150", "200", 5200);

  UpdateVersionStorageInfo();

  // Merging to the second last level: (5200 / 2100 + 1) * 1100
  // Merging to the last level: (50000 / 6300 + 1) * 1300
  ASSERT_EQ(2100u + 3823u + 11617u,
            vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, IsBottommostLevelTest) {
  // case 1: Higher levels are empty
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  bool result =
      Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_TRUE(result);

  // case 2: Higher levels have no overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "k", "p");
  Add(3, 8U, "t", "w");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_TRUE(result);

  // case 3.1: Higher levels (level 3) have overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "e", "g");
  Add(3, 8U, "h", "k");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // case 3.2: Higher levels (level 5) have overlap
  DeleteVersionStorage();
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "j", "k");
  Add(3, 8U, "l", "m");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  Add(5, 11U, "h", "k");
  Add(5, 12U, "y", "yy");
  Add(5, 13U, "z", "zz");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // case 3.3: Higher levels (level 5) have overlap, but it's only overlapping
  // one key ("d")
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "j", "k");
  Add(3, 8U, "l", "m");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  Add(5, 11U, "ccc", "d");
  Add(5, 12U, "y", "yy");
  Add(5, 13U, "z", "zz");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // Level 0 files overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "s", "t");
  Add(0, 2U, "a", "m");
  Add(0, 3U, "b", "z");
  Add(0, 4U, "e", "f");
  Add(5, 10U, "y", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(1, 0);
  AddToCompactionFiles(1U);
  AddToCompactionFiles(2U);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(4U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // Level 0 files don't overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "s", "t");
  Add(0, 2U, "a", "m");
  Add(0, 3U, "b", "k");
  Add(0, 4U, "e", "f");
  Add(5, 10U, "y", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(1, 0);
  AddToCompactionFiles(1U);
  AddToCompactionFiles(2U);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(4U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_TRUE(result);

  // Level 1 files overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "s", "t");
  Add(0, 2U, "a", "m");
  Add(0, 3U, "b", "k");
  Add(0, 4U, "e", "f");
  Add(1, 5U, "a", "m");
  Add(1, 6U, "n", "o");
  Add(1, 7U, "w", "y");
  Add(5, 10U, "y", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 0);
  AddToCompactionFiles(1U);
  AddToCompactionFiles(2U);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(4U);
  AddToCompactionFiles(5U);
  AddToCompactionFiles(6U);
  AddToCompactionFiles(7U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  DeleteVersionStorage();
}

TEST_F(CompactionPickerTest, MaxCompactionBytesHit) {
  mutable_cf_options_.max_bytes_for_level_base = 1000000u;
  mutable_cf_options_.max_compaction_bytes = 800000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2 and 5.
  // It can expand because adding file 1 and 3, the compaction size will
  // exceed mutable_cf_options_.max_bytes_for_level_base.
  Add(1, 1U, "100", "150", 300000U);
  Add(1, 2U, "151", "200", 300001U, 0, 0);
  Add(1, 3U, "201", "250", 300000U, 0, 0);
  Add(1, 4U, "251", "300", 300000U, 0, 0);
  Add(2, 5U, "100", "256", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, MaxCompactionBytesNotHit) {
  mutable_cf_options_.max_bytes_for_level_base = 800000u;
  mutable_cf_options_.max_compaction_bytes = 1000000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2 and 5.
  // and it expands to file 1 and 3 too.
  Add(1, 1U, "100", "150", 300000U);
  Add(1, 2U, "151", "200", 300001U, 0, 0);
  Add(1, 3U, "201", "250", 300000U, 0, 0);
  Add(1, 4U, "251", "300", 300000U, 0, 0);
  Add(2, 5U, "000", "251", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, IsTrivialMoveOn) {
  mutable_cf_options_.max_bytes_for_level_base = 10000u;
  mutable_cf_options_.max_compaction_bytes = 10001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2
  Add(1, 1U, "100", "150", 3000U);
  Add(1, 2U, "151", "200", 3001U);
  Add(1, 3U, "201", "250", 3000U);
  Add(1, 4U, "251", "300", 3000U);

  Add(3, 5U, "120", "130", 7000U);
  Add(3, 6U, "170", "180", 7000U);
  Add(3, 5U, "220", "230", 7000U);
  Add(3, 5U, "270", "280", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
    cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, IsTrivialMoveOff) {
  mutable_cf_options_.max_bytes_for_level_base = 1000000u;
  mutable_cf_options_.max_compaction_bytes = 10000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick all files from level 1
  Add(1, 1U, "100", "150", 300000U, 0, 0);
  Add(1, 2U, "150", "200", 300000U, 0, 0);
  Add(1, 3U, "200", "250", 300000U, 0, 0);
  Add(1, 4U, "250", "300", 300000U, 0, 0);

  Add(3, 5U, "120", "130", 6000U);
  Add(3, 6U, "140", "150", 6000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
    cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_FALSE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, CacheNextCompactionIndex) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;

  Add(1 /* level */, 1U /* file_number */, "100" /* smallest */,
      "149" /* largest */, 1000000000U /* file_size */);
  file_map_[1U].first->being_compacted = true;
  Add(1 /* level */, 2U /* file_number */, "150" /* smallest */,
      "199" /* largest */, 900000000U /* file_size */);
  Add(1 /* level */, 3U /* file_number */, "200" /* smallest */,
      "249" /* largest */, 800000000U /* file_size */);
  Add(1 /* level */, 4U /* file_number */, "250" /* smallest */,
      "299" /* largest */, 700000000U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "150" /* smallest */,
      "199" /* largest */, 1U /* file_size */);
  file_map_[5U].first->being_compacted = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(0U, compaction->num_input_files(1));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2, vstorage_->NextCompactionIndex(1 /* level */));

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(0U, compaction->num_input_files(1));
  ASSERT_EQ(4U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3, vstorage_->NextCompactionIndex(1 /* level */));

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
  ASSERT_EQ(4, vstorage_->NextCompactionIndex(1 /* level */));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
