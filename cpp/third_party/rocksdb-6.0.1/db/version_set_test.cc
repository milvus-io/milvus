//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"
#include "db/log_writer.h"
#include "table/mock_table.h"
#include "util/logging.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class GenerateLevelFilesBriefTest : public testing::Test {
 public:
  std::vector<FileMetaData*> files_;
  LevelFilesBrief file_level_;
  Arena arena_;

  GenerateLevelFilesBriefTest() { }

  ~GenerateLevelFilesBriefTest() override {
    for (size_t i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(files_.size() + 1, 0, 0);
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
    f->largest = InternalKey(largest, largest_seq, kTypeValue);
    files_.push_back(f);
  }

  int Compare() {
    int diff = 0;
    for (size_t i = 0; i < files_.size(); i++) {
      if (file_level_.files[i].fd.GetNumber() != files_[i]->fd.GetNumber()) {
        diff++;
      }
    }
    return diff;
  }
};

TEST_F(GenerateLevelFilesBriefTest, Empty) {
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(0u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

TEST_F(GenerateLevelFilesBriefTest, Single) {
  Add("p", "q");
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(1u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

TEST_F(GenerateLevelFilesBriefTest, Multiple) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(4u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

class CountingLogger : public Logger {
 public:
  CountingLogger() : log_count(0) {}
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override { log_count++; }
  int log_count;
};

Options GetOptionsWithNumLevels(int num_levels,
                                std::shared_ptr<CountingLogger> logger) {
  Options opt;
  opt.num_levels = num_levels;
  opt.info_log = logger;
  return opt;
}

class VersionStorageInfoTest : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  std::shared_ptr<CountingLogger> logger_;
  Options options_;
  ImmutableCFOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  VersionStorageInfo vstorage_;

  InternalKey GetInternalKey(const char* ukey,
                             SequenceNumber smallest_seq = 100) {
    return InternalKey(ukey, smallest_seq, kTypeValue);
  }

  VersionStorageInfoTest()
      : ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        logger_(new CountingLogger()),
        options_(GetOptionsWithNumLevels(6, logger_)),
        ioptions_(options_),
        mutable_cf_options_(options_),
        vstorage_(&icmp_, ucmp_, 6, kCompactionStyleLevel, nullptr, false) {}

  ~VersionStorageInfoTest() override {
    for (int i = 0; i < vstorage_.num_levels(); i++) {
      for (auto* f : vstorage_.LevelFiles(i)) {
        if (--f->refs == 0) {
          delete f;
        }
      }
    }
  }

  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(file_number, 0, file_size);
    f->smallest = GetInternalKey(smallest, 0);
    f->largest = GetInternalKey(largest, 0);
    f->compensated_file_size = file_size;
    f->refs = 0;
    f->num_entries = 0;
    f->num_deletions = 0;
    vstorage_.AddFile(level, f);
  }

  void Add(int level, uint32_t file_number, const InternalKey& smallest,
           const InternalKey& largest, uint64_t file_size = 0) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(file_number, 0, file_size);
    f->smallest = smallest;
    f->largest = largest;
    f->compensated_file_size = file_size;
    f->refs = 0;
    f->num_entries = 0;
    f->num_deletions = 0;
    vstorage_.AddFile(level, f);
  }

  std::string GetOverlappingFiles(int level, const InternalKey& begin,
                                  const InternalKey& end) {
    std::vector<FileMetaData*> inputs;
    vstorage_.GetOverlappingInputs(level, &begin, &end, &inputs);

    std::string result;
    for (size_t i = 0; i < inputs.size(); ++i) {
      if (i > 0) {
        result += ",";
      }
      AppendNumberTo(&result, inputs[i]->fd.GetNumber());
    }
    return result;
  }
};

TEST_F(VersionStorageInfoTest, MaxBytesForLevelStatic) {
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.max_bytes_for_level_base = 10;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  Add(4, 100U, "1", "2");
  Add(5, 101U, "1", "2");

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 10U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 50U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 250U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1250U);

  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamic) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  Add(5, 1U, "1", "2", 500U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.base_level(), 5);

  Add(5, 2U, "3", "4", 550U);
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 4);

  Add(4, 3U, "3", "4", 550U);
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 4);

  Add(3, 4U, "3", "4", 250U);
  Add(3, 5U, "5", "7", 300U);
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(1, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 3);

  Add(1, 6U, "3", "4", 5U);
  Add(1, 7U, "8", "9", 5U);
  logger_->log_count = 0;
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(1, logger_->log_count);
  ASSERT_GT(vstorage_.MaxBytesForLevel(4), 1005U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(3), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 1);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicLotsOfData) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 100;
  mutable_cf_options_.max_bytes_for_level_multiplier = 2;
  Add(0, 1U, "1", "2", 50U);
  Add(1, 2U, "1", "2", 50U);
  Add(2, 3U, "1", "2", 500U);
  Add(3, 4U, "1", "2", 500U);
  Add(4, 5U, "1", "2", 1700U);
  Add(5, 6U, "1", "2", 500U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 800U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 400U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 200U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 100U);
  ASSERT_EQ(vstorage_.base_level(), 1);
  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicLargeLevel) {
  uint64_t kOneGB = 1000U * 1000U * 1000U;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 10U * kOneGB;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  Add(0, 1U, "1", "2", 50U);
  Add(3, 4U, "1", "2", 32U * kOneGB);
  Add(4, 5U, "1", "2", 500U * kOneGB);
  Add(5, 6U, "1", "2", 3000U * kOneGB);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(5), 3000U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 300U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 30U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 10U * kOneGB);
  ASSERT_EQ(vstorage_.base_level(), 2);
  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicWithLargeL0_1) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 40000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;

  Add(0, 1U, "1", "2", 10000U);
  Add(0, 2U, "1", "2", 10000U);
  Add(0, 3U, "1", "2", 10000U);

  Add(5, 4U, "1", "2", 1286250U);
  Add(4, 5U, "1", "2", 200000U);
  Add(3, 6U, "1", "2", 40000U);
  Add(2, 7U, "1", "2", 8000U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(2, vstorage_.base_level());
  // level multiplier should be 3.5
  ASSERT_EQ(vstorage_.level_multiplier(), 5.0);
  // Level size should be around 30,000, 105,000, 367,500
  ASSERT_EQ(40000U, vstorage_.MaxBytesForLevel(2));
  ASSERT_EQ(51450U, vstorage_.MaxBytesForLevel(3));
  ASSERT_EQ(257250U, vstorage_.MaxBytesForLevel(4));
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicWithLargeL0_2) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 10000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;

  Add(0, 11U, "1", "2", 10000U);
  Add(0, 12U, "1", "2", 10000U);
  Add(0, 13U, "1", "2", 10000U);

  Add(5, 4U, "1", "2", 1286250U);
  Add(4, 5U, "1", "2", 200000U);
  Add(3, 6U, "1", "2", 40000U);
  Add(2, 7U, "1", "2", 8000U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(2, vstorage_.base_level());
  // level multiplier should be 3.5
  ASSERT_LT(vstorage_.level_multiplier(), 3.6);
  ASSERT_GT(vstorage_.level_multiplier(), 3.4);
  // Level size should be around 30,000, 105,000, 367,500
  ASSERT_EQ(30000U, vstorage_.MaxBytesForLevel(2));
  ASSERT_LT(vstorage_.MaxBytesForLevel(3), 110000U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(3), 100000U);
  ASSERT_LT(vstorage_.MaxBytesForLevel(4), 370000U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(4), 360000U);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicWithLargeL0_3) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 10000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;

  Add(0, 11U, "1", "2", 5000U);
  Add(0, 12U, "1", "2", 5000U);
  Add(0, 13U, "1", "2", 5000U);
  Add(0, 14U, "1", "2", 5000U);
  Add(0, 15U, "1", "2", 5000U);
  Add(0, 16U, "1", "2", 5000U);

  Add(5, 4U, "1", "2", 1286250U);
  Add(4, 5U, "1", "2", 200000U);
  Add(3, 6U, "1", "2", 40000U);
  Add(2, 7U, "1", "2", 8000U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(2, vstorage_.base_level());
  // level multiplier should be 3.5
  ASSERT_LT(vstorage_.level_multiplier(), 3.6);
  ASSERT_GT(vstorage_.level_multiplier(), 3.4);
  // Level size should be around 30,000, 105,000, 367,500
  ASSERT_EQ(30000U, vstorage_.MaxBytesForLevel(2));
  ASSERT_LT(vstorage_.MaxBytesForLevel(3), 110000U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(3), 100000U);
  ASSERT_LT(vstorage_.MaxBytesForLevel(4), 370000U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(4), 360000U);
}

TEST_F(VersionStorageInfoTest, EstimateLiveDataSize) {
  // Test whether the overlaps are detected as expected
  Add(1, 1U, "4", "7", 1U);  // Perfect overlap with last level
  Add(2, 2U, "3", "5", 1U);  // Partial overlap with last level
  Add(2, 3U, "6", "8", 1U);  // Partial overlap with last level
  Add(3, 4U, "1", "9", 1U);  // Contains range of last level
  Add(4, 5U, "4", "5", 1U);  // Inside range of last level
  Add(4, 5U, "6", "7", 1U);  // Inside range of last level
  Add(5, 6U, "4", "7", 10U);
  ASSERT_EQ(10U, vstorage_.EstimateLiveDataSize());
}

TEST_F(VersionStorageInfoTest, EstimateLiveDataSize2) {
  Add(0, 1U, "9", "9", 1U);  // Level 0 is not ordered
  Add(0, 1U, "5", "6", 1U);  // Ignored because of [5,6] in l1
  Add(1, 1U, "1", "2", 1U);  // Ignored because of [2,3] in l2
  Add(1, 2U, "3", "4", 1U);  // Ignored because of [2,3] in l2
  Add(1, 3U, "5", "6", 1U);
  Add(2, 4U, "2", "3", 1U);
  Add(3, 5U, "7", "8", 1U);
  ASSERT_EQ(4U, vstorage_.EstimateLiveDataSize());
}

TEST_F(VersionStorageInfoTest, GetOverlappingInputs) {
  // Two files that overlap at the range deletion tombstone sentinel.
  Add(1, 1U, {"a", 0, kTypeValue}, {"b", kMaxSequenceNumber, kTypeRangeDeletion}, 1);
  Add(1, 2U, {"b", 0, kTypeValue}, {"c", 0, kTypeValue}, 1);
  // Two files that overlap at the same user key.
  Add(1, 3U, {"d", 0, kTypeValue}, {"e", kMaxSequenceNumber, kTypeValue}, 1);
  Add(1, 4U, {"e", 0, kTypeValue}, {"f", 0, kTypeValue}, 1);
  // Two files that do not overlap.
  Add(1, 5U, {"g", 0, kTypeValue}, {"h", 0, kTypeValue}, 1);
  Add(1, 6U, {"i", 0, kTypeValue}, {"j", 0, kTypeValue}, 1);
  vstorage_.UpdateNumNonEmptyLevels();
  vstorage_.GenerateLevelFilesBrief();

  ASSERT_EQ("1,2", GetOverlappingFiles(
      1, {"a", 0, kTypeValue}, {"b", 0, kTypeValue}));
  ASSERT_EQ("1", GetOverlappingFiles(
      1, {"a", 0, kTypeValue}, {"b", kMaxSequenceNumber, kTypeRangeDeletion}));
  ASSERT_EQ("2", GetOverlappingFiles(
      1, {"b", kMaxSequenceNumber, kTypeValue}, {"c", 0, kTypeValue}));
  ASSERT_EQ("3,4", GetOverlappingFiles(
      1, {"d", 0, kTypeValue}, {"e", 0, kTypeValue}));
  ASSERT_EQ("3", GetOverlappingFiles(
      1, {"d", 0, kTypeValue}, {"e", kMaxSequenceNumber, kTypeRangeDeletion}));
  ASSERT_EQ("3,4", GetOverlappingFiles(
      1, {"e", kMaxSequenceNumber, kTypeValue}, {"f", 0, kTypeValue}));
  ASSERT_EQ("3,4", GetOverlappingFiles(
      1, {"e", 0, kTypeValue}, {"f", 0, kTypeValue}));
  ASSERT_EQ("5", GetOverlappingFiles(
      1, {"g", 0, kTypeValue}, {"h", 0, kTypeValue}));
  ASSERT_EQ("6", GetOverlappingFiles(
      1, {"i", 0, kTypeValue}, {"j", 0, kTypeValue}));
}


class FindLevelFileTest : public testing::Test {
 public:
  LevelFilesBrief file_level_;
  bool disjoint_sorted_files_;
  Arena arena_;

  FindLevelFileTest() : disjoint_sorted_files_(true) { }

  ~FindLevelFileTest() override {}

  void LevelFileInit(size_t num = 0) {
    char* mem = arena_.AllocateAligned(num * sizeof(FdWithKeyRange));
    file_level_.files = new (mem)FdWithKeyRange[num];
    file_level_.num_files = 0;
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    InternalKey smallest_key = InternalKey(smallest, smallest_seq, kTypeValue);
    InternalKey largest_key = InternalKey(largest, largest_seq, kTypeValue);

    Slice smallest_slice = smallest_key.Encode();
    Slice largest_slice = largest_key.Encode();

    char* mem = arena_.AllocateAligned(
        smallest_slice.size() + largest_slice.size());
    memcpy(mem, smallest_slice.data(), smallest_slice.size());
    memcpy(mem + smallest_slice.size(), largest_slice.data(),
        largest_slice.size());

    // add to file_level_
    size_t num = file_level_.num_files;
    auto& file = file_level_.files[num];
    file.fd = FileDescriptor(num + 1, 0, 0);
    file.smallest_key = Slice(mem, smallest_slice.size());
    file.largest_key = Slice(mem + smallest_slice.size(),
        largest_slice.size());
    file_level_.num_files++;
  }

  int Find(const char* key) {
    InternalKey target(key, 100, kTypeValue);
    InternalKeyComparator cmp(BytewiseComparator());
    return FindFile(cmp, file_level_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != nullptr ? smallest : "");
    Slice l(largest != nullptr ? largest : "");
    return SomeFileOverlapsRange(cmp, disjoint_sorted_files_, file_level_,
                                 (smallest != nullptr ? &s : nullptr),
                                 (largest != nullptr ? &l : nullptr));
  }
};

TEST_F(FindLevelFileTest, LevelEmpty) {
  LevelFileInit(0);

  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(! Overlaps("a", "z"));
  ASSERT_TRUE(! Overlaps(nullptr, "z"));
  ASSERT_TRUE(! Overlaps("a", nullptr));
  ASSERT_TRUE(! Overlaps(nullptr, nullptr));
}

TEST_F(FindLevelFileTest, LevelSingle) {
  LevelFileInit(1);

  Add("p", "q");
  ASSERT_EQ(0, Find("a"));
  ASSERT_EQ(0, Find("p"));
  ASSERT_EQ(0, Find("p1"));
  ASSERT_EQ(0, Find("q"));
  ASSERT_EQ(1, Find("q1"));
  ASSERT_EQ(1, Find("z"));

  ASSERT_TRUE(! Overlaps("a", "b"));
  ASSERT_TRUE(! Overlaps("z1", "z2"));
  ASSERT_TRUE(Overlaps("a", "p"));
  ASSERT_TRUE(Overlaps("a", "q"));
  ASSERT_TRUE(Overlaps("a", "z"));
  ASSERT_TRUE(Overlaps("p", "p1"));
  ASSERT_TRUE(Overlaps("p", "q"));
  ASSERT_TRUE(Overlaps("p", "z"));
  ASSERT_TRUE(Overlaps("p1", "p2"));
  ASSERT_TRUE(Overlaps("p1", "z"));
  ASSERT_TRUE(Overlaps("q", "q"));
  ASSERT_TRUE(Overlaps("q", "q1"));

  ASSERT_TRUE(! Overlaps(nullptr, "j"));
  ASSERT_TRUE(! Overlaps("r", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "p"));
  ASSERT_TRUE(Overlaps(nullptr, "p1"));
  ASSERT_TRUE(Overlaps("q", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
}

TEST_F(FindLevelFileTest, LevelMultiple) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_EQ(0, Find("100"));
  ASSERT_EQ(0, Find("150"));
  ASSERT_EQ(0, Find("151"));
  ASSERT_EQ(0, Find("199"));
  ASSERT_EQ(0, Find("200"));
  ASSERT_EQ(1, Find("201"));
  ASSERT_EQ(1, Find("249"));
  ASSERT_EQ(1, Find("250"));
  ASSERT_EQ(2, Find("251"));
  ASSERT_EQ(2, Find("299"));
  ASSERT_EQ(2, Find("300"));
  ASSERT_EQ(2, Find("349"));
  ASSERT_EQ(2, Find("350"));
  ASSERT_EQ(3, Find("351"));
  ASSERT_EQ(3, Find("400"));
  ASSERT_EQ(3, Find("450"));
  ASSERT_EQ(4, Find("451"));

  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("251", "299"));
  ASSERT_TRUE(! Overlaps("451", "500"));
  ASSERT_TRUE(! Overlaps("351", "399"));

  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
}

TEST_F(FindLevelFileTest, LevelMultipleNullBoundaries) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_TRUE(! Overlaps(nullptr, "149"));
  ASSERT_TRUE(! Overlaps("451", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "150"));
  ASSERT_TRUE(Overlaps(nullptr, "199"));
  ASSERT_TRUE(Overlaps(nullptr, "200"));
  ASSERT_TRUE(Overlaps(nullptr, "201"));
  ASSERT_TRUE(Overlaps(nullptr, "400"));
  ASSERT_TRUE(Overlaps(nullptr, "800"));
  ASSERT_TRUE(Overlaps("100", nullptr));
  ASSERT_TRUE(Overlaps("200", nullptr));
  ASSERT_TRUE(Overlaps("449", nullptr));
  ASSERT_TRUE(Overlaps("450", nullptr));
}

TEST_F(FindLevelFileTest, LevelOverlapSequenceChecks) {
  LevelFileInit(1);

  Add("200", "200", 5000, 3000);
  ASSERT_TRUE(! Overlaps("199", "199"));
  ASSERT_TRUE(! Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST_F(FindLevelFileTest, LevelOverlappingFiles) {
  LevelFileInit(2);

  Add("150", "600");
  Add("400", "500");
  disjoint_sorted_files_ = false;
  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("601", "700"));
  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
  ASSERT_TRUE(Overlaps("450", "700"));
  ASSERT_TRUE(Overlaps("600", "700"));
}

class VersionSetTestBase {
 public:
  const static std::string kColumnFamilyName1;
  const static std::string kColumnFamilyName2;
  const static std::string kColumnFamilyName3;

  VersionSetTestBase()
      : env_(Env::Default()),
        dbname_(test::PerThreadDBPath("version_set_test")),
        db_options_(),
        mutable_cf_options_(cf_options_),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        versions_(new VersionSet(dbname_, &db_options_, env_options_,
                                 table_cache_.get(), &write_buffer_manager_,
                                 &write_controller_)),
        shutting_down_(false),
        mock_table_factory_(std::make_shared<mock::MockTableFactory>()) {
    EXPECT_OK(env_->CreateDirIfMissing(dbname_));
    db_options_.db_paths.emplace_back(dbname_,
                                      std::numeric_limits<uint64_t>::max());
  }

  void PrepareManifest(std::vector<ColumnFamilyDescriptor>* column_families,
                       SequenceNumber* last_seqno,
                       std::unique_ptr<log::Writer>* log_writer) {
    assert(column_families != nullptr);
    assert(last_seqno != nullptr);
    assert(log_writer != nullptr);
    VersionEdit new_db;
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::vector<std::string> cf_names = {
        kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
        kColumnFamilyName3};
    const int kInitialNumOfCfs = static_cast<int>(cf_names.size());
    autovector<VersionEdit> new_cfs;
    uint64_t last_seq = 1;
    uint32_t cf_id = 1;
    for (int i = 1; i != kInitialNumOfCfs; ++i) {
      VersionEdit new_cf;
      new_cf.AddColumnFamily(cf_names[i]);
      new_cf.SetColumnFamily(cf_id++);
      new_cf.SetLogNumber(0);
      new_cf.SetNextFile(2);
      new_cf.SetLastSequence(last_seq++);
      new_cfs.emplace_back(new_cf);
    }
    *last_seqno = last_seq;

    const std::string manifest = DescriptorFileName(dbname_, 1);
    std::unique_ptr<WritableFile> file;
    Status s = env_->NewWritableFile(
        manifest, &file, env_->OptimizeForManifestWrite(env_options_));
    ASSERT_OK(s);
    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(file), manifest, env_options_));
    {
      log_writer->reset(new log::Writer(std::move(file_writer), 0, false));
      std::string record;
      new_db.EncodeTo(&record);
      s = (*log_writer)->AddRecord(record);
      for (const auto& e : new_cfs) {
        record.clear();
        e.EncodeTo(&record);
        s = (*log_writer)->AddRecord(record);
        ASSERT_OK(s);
      }
    }
    ASSERT_OK(s);

    cf_options_.table_factory = mock_table_factory_;
    for (const auto& cf_name : cf_names) {
      column_families->emplace_back(cf_name, cf_options_);
    }
  }

  // Create DB with 3 column families.
  void NewDB() {
    std::vector<ColumnFamilyDescriptor> column_families;
    SequenceNumber last_seqno;
    std::unique_ptr<log::Writer> log_writer;

    PrepareManifest(&column_families, &last_seqno, &log_writer);
    log_writer.reset();
    // Make "CURRENT" file point to the new manifest file.
    Status s = SetCurrentFile(env_, dbname_, 1, nullptr);
    ASSERT_OK(s);

    EXPECT_OK(versions_->Recover(column_families, false));
    EXPECT_EQ(column_families.size(),
              versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  }

  Env* env_;
  const std::string dbname_;
  EnvOptions env_options_;
  ImmutableDBOptions db_options_;
  ColumnFamilyOptions cf_options_;
  MutableCFOptions mutable_cf_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  std::shared_ptr<VersionSet> versions_;
  InstrumentedMutex mutex_;
  std::atomic<bool> shutting_down_;
  std::shared_ptr<mock::MockTableFactory> mock_table_factory_;
};

const std::string VersionSetTestBase::kColumnFamilyName1 = "alice";
const std::string VersionSetTestBase::kColumnFamilyName2 = "bob";
const std::string VersionSetTestBase::kColumnFamilyName3 = "charles";

class VersionSetTest : public VersionSetTestBase, public testing::Test {
 public:
  VersionSetTest() : VersionSetTestBase() {}
};

TEST_F(VersionSetTest, SameColumnFamilyGroupCommit) {
  NewDB();
  const int kGroupSize = 5;
  autovector<VersionEdit> edits;
  for (int i = 0; i != kGroupSize; ++i) {
    edits.emplace_back(VersionEdit());
  }
  autovector<ColumnFamilyData*> cfds;
  autovector<const MutableCFOptions*> all_mutable_cf_options;
  autovector<autovector<VersionEdit*>> edit_lists;
  for (int i = 0; i != kGroupSize; ++i) {
    cfds.emplace_back(versions_->GetColumnFamilySet()->GetDefault());
    all_mutable_cf_options.emplace_back(&mutable_cf_options_);
    autovector<VersionEdit*> edit_list;
    edit_list.emplace_back(&edits[i]);
    edit_lists.emplace_back(edit_list);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  int count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:SameColumnFamily", [&](void* arg) {
        uint32_t* cf_id = reinterpret_cast<uint32_t*>(arg);
        EXPECT_EQ(0, *cf_id);
        ++count;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  mutex_.Lock();
  Status s =
      versions_->LogAndApply(cfds, all_mutable_cf_options, edit_lists, &mutex_);
  mutex_.Unlock();
  EXPECT_OK(s);
  EXPECT_EQ(kGroupSize - 1, count);
}

TEST_F(VersionSetTest, HandleValidAtomicGroup) {
  std::vector<ColumnFamilyDescriptor> column_families;
  SequenceNumber last_seqno;
  std::unique_ptr<log::Writer> log_writer;
  PrepareManifest(&column_families, &last_seqno, &log_writer);

  // Append multiple version edits that form an atomic group
  const int kAtomicGroupSize = 3;
  std::vector<VersionEdit> edits(kAtomicGroupSize);
  int remaining = kAtomicGroupSize;
  for (size_t i = 0; i != edits.size(); ++i) {
    edits[i].SetLogNumber(0);
    edits[i].SetNextFile(2);
    edits[i].MarkAtomicGroup(--remaining);
    edits[i].SetLastSequence(last_seqno++);
  }
  Status s;
  for (const auto& edit : edits) {
    std::string record;
    edit.EncodeTo(&record);
    s = log_writer->AddRecord(record);
    ASSERT_OK(s);
  }
  log_writer.reset();

  s = SetCurrentFile(env_, dbname_, 1, nullptr);
  ASSERT_OK(s);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  bool first_in_atomic_group = false;
  bool last_in_atomic_group = false;

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Recover:FirstInAtomicGroup", [&](void* arg) {
        VersionEdit* e = reinterpret_cast<VersionEdit*>(arg);
        EXPECT_EQ(edits.front().DebugString(),
                  e->DebugString());  // compare based on value
        first_in_atomic_group = true;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Recover:LastInAtomicGroup", [&](void* arg) {
        VersionEdit* e = reinterpret_cast<VersionEdit*>(arg);
        EXPECT_EQ(edits.back().DebugString(),
                  e->DebugString());  // compare based on value
        EXPECT_TRUE(first_in_atomic_group);
        last_in_atomic_group = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  EXPECT_OK(versions_->Recover(column_families, false));
  EXPECT_EQ(column_families.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group);
  EXPECT_TRUE(last_in_atomic_group);
}

TEST_F(VersionSetTest, HandleIncompleteTrailingAtomicGroup) {
  std::vector<ColumnFamilyDescriptor> column_families;
  SequenceNumber last_seqno;
  std::unique_ptr<log::Writer> log_writer;
  PrepareManifest(&column_families, &last_seqno, &log_writer);

  // Append multiple version edits that form an atomic group
  const int kAtomicGroupSize = 4;
  const int kNumberOfPersistedVersionEdits = kAtomicGroupSize - 1;
  std::vector<VersionEdit> edits(kNumberOfPersistedVersionEdits);
  int remaining = kAtomicGroupSize;
  for (size_t i = 0; i != edits.size(); ++i) {
    edits[i].SetLogNumber(0);
    edits[i].SetNextFile(2);
    edits[i].MarkAtomicGroup(--remaining);
    edits[i].SetLastSequence(last_seqno++);
  }
  Status s;
  for (const auto& edit : edits) {
    std::string record;
    edit.EncodeTo(&record);
    s = log_writer->AddRecord(record);
    ASSERT_OK(s);
  }
  log_writer.reset();

  s = SetCurrentFile(env_, dbname_, 1, nullptr);
  ASSERT_OK(s);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  bool first_in_atomic_group = false;
  bool last_in_atomic_group = false;
  size_t num = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Recover:FirstInAtomicGroup", [&](void* arg) {
        VersionEdit* e = reinterpret_cast<VersionEdit*>(arg);
        EXPECT_EQ(edits.front().DebugString(),
                  e->DebugString());  // compare based on value
        first_in_atomic_group = true;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Recover:LastInAtomicGroup",
      [&](void* /* arg */) { last_in_atomic_group = true; });
  SyncPoint::GetInstance()->SetCallBack("VersionSet::Recover:AtomicGroup",
                                        [&](void* /* arg */) { ++num; });
  SyncPoint::GetInstance()->EnableProcessing();

  EXPECT_OK(versions_->Recover(column_families, false));
  EXPECT_EQ(column_families.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group);
  EXPECT_FALSE(last_in_atomic_group);
  EXPECT_EQ(kNumberOfPersistedVersionEdits, num);
}

TEST_F(VersionSetTest, HandleCorruptedAtomicGroup) {
  std::vector<ColumnFamilyDescriptor> column_families;
  SequenceNumber last_seqno;
  std::unique_ptr<log::Writer> log_writer;
  PrepareManifest(&column_families, &last_seqno, &log_writer);

  // Append multiple version edits that form an atomic group
  const int kAtomicGroupSize = 4;
  std::vector<VersionEdit> edits(kAtomicGroupSize);
  int remaining = kAtomicGroupSize;
  for (size_t i = 0; i != edits.size(); ++i) {
    edits[i].SetLogNumber(0);
    edits[i].SetNextFile(2);
    if (i != (kAtomicGroupSize / 2)) {
      edits[i].MarkAtomicGroup(--remaining);
    }
    edits[i].SetLastSequence(last_seqno++);
  }
  Status s;
  for (const auto& edit : edits) {
    std::string record;
    edit.EncodeTo(&record);
    s = log_writer->AddRecord(record);
    ASSERT_OK(s);
  }
  log_writer.reset();

  s = SetCurrentFile(env_, dbname_, 1, nullptr);
  ASSERT_OK(s);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  bool mixed = false;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Recover:AtomicGroupMixedWithNormalEdits", [&](void* arg) {
        VersionEdit* e = reinterpret_cast<VersionEdit*>(arg);
        EXPECT_EQ(edits[kAtomicGroupSize / 2].DebugString(), e->DebugString());
        mixed = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  EXPECT_NOK(versions_->Recover(column_families, false));
  EXPECT_EQ(column_families.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(mixed);
}

TEST_F(VersionSetTest, HandleIncorrectAtomicGroupSize) {
  std::vector<ColumnFamilyDescriptor> column_families;
  SequenceNumber last_seqno;
  std::unique_ptr<log::Writer> log_writer;
  PrepareManifest(&column_families, &last_seqno, &log_writer);

  // Append multiple version edits that form an atomic group
  const int kAtomicGroupSize = 4;
  std::vector<VersionEdit> edits(kAtomicGroupSize);
  int remaining = kAtomicGroupSize;
  for (size_t i = 0; i != edits.size(); ++i) {
    edits[i].SetLogNumber(0);
    edits[i].SetNextFile(2);
    if (i != 1) {
      edits[i].MarkAtomicGroup(--remaining);
    } else {
      edits[i].MarkAtomicGroup(remaining--);
    }
    edits[i].SetLastSequence(last_seqno++);
  }
  Status s;
  for (const auto& edit : edits) {
    std::string record;
    edit.EncodeTo(&record);
    s = log_writer->AddRecord(record);
    ASSERT_OK(s);
  }
  log_writer.reset();

  s = SetCurrentFile(env_, dbname_, 1, nullptr);
  ASSERT_OK(s);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  bool incorrect_group_size = false;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Recover:IncorrectAtomicGroupSize", [&](void* arg) {
        VersionEdit* e = reinterpret_cast<VersionEdit*>(arg);
        EXPECT_EQ(edits[1].DebugString(), e->DebugString());
        incorrect_group_size = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  EXPECT_NOK(versions_->Recover(column_families, false));
  EXPECT_EQ(column_families.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(incorrect_group_size);
}

class VersionSetTestDropOneCF : public VersionSetTestBase,
                                public testing::TestWithParam<std::string> {
 public:
  VersionSetTestDropOneCF() : VersionSetTestBase() {}
};

// This test simulates the following execution sequence
// Time  thread1                  bg_flush_thr
//  |                             Prepare version edits (e1,e2,e3) for atomic
//  |                             flush cf1, cf2, cf3
//  |    Enqueue e to drop cfi
//  |    to manifest_writers_
//  |                             Enqueue (e1,e2,e3) to manifest_writers_
//  |
//  |    Apply e,
//  |    cfi.IsDropped() is true
//  |                             Apply (e1,e2,e3),
//  |                             since cfi.IsDropped() == true, we need to
//  |                             drop ei and write the rest to MANIFEST.
//  V
//
//  Repeat the test for i = 1, 2, 3 to simulate dropping the first, middle and
//  last column family in an atomic group.
TEST_P(VersionSetTestDropOneCF, HandleDroppedColumnFamilyInAtomicGroup) {
  std::vector<ColumnFamilyDescriptor> column_families;
  SequenceNumber last_seqno;
  std::unique_ptr<log::Writer> log_writer;
  PrepareManifest(&column_families, &last_seqno, &log_writer);
  Status s = SetCurrentFile(env_, dbname_, 1, nullptr);
  ASSERT_OK(s);

  EXPECT_OK(versions_->Recover(column_families, false /* read_only */));
  EXPECT_EQ(column_families.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());

  const int kAtomicGroupSize = 3;
  const std::vector<std::string> non_default_cf_names = {
      kColumnFamilyName1, kColumnFamilyName2, kColumnFamilyName3};

  // Drop one column family
  VersionEdit drop_cf_edit;
  drop_cf_edit.DropColumnFamily();
  const std::string cf_to_drop_name(GetParam());
  auto cfd_to_drop =
      versions_->GetColumnFamilySet()->GetColumnFamily(cf_to_drop_name);
  ASSERT_NE(nullptr, cfd_to_drop);
  // Increase its refcount because cfd_to_drop is used later, and we need to
  // prevent it from being deleted.
  cfd_to_drop->Ref();
  drop_cf_edit.SetColumnFamily(cfd_to_drop->GetID());
  mutex_.Lock();
  s = versions_->LogAndApply(cfd_to_drop,
                             *cfd_to_drop->GetLatestMutableCFOptions(),
                             &drop_cf_edit, &mutex_);
  mutex_.Unlock();
  ASSERT_OK(s);

  std::vector<VersionEdit> edits(kAtomicGroupSize);
  uint32_t remaining = kAtomicGroupSize;
  size_t i = 0;
  autovector<ColumnFamilyData*> cfds;
  autovector<const MutableCFOptions*> mutable_cf_options_list;
  autovector<autovector<VersionEdit*>> edit_lists;
  for (const auto& cf_name : non_default_cf_names) {
    auto cfd = (cf_name != cf_to_drop_name)
                   ? versions_->GetColumnFamilySet()->GetColumnFamily(cf_name)
                   : cfd_to_drop;
    ASSERT_NE(nullptr, cfd);
    cfds.push_back(cfd);
    mutable_cf_options_list.emplace_back(cfd->GetLatestMutableCFOptions());
    edits[i].SetColumnFamily(cfd->GetID());
    edits[i].SetLogNumber(0);
    edits[i].SetNextFile(2);
    edits[i].MarkAtomicGroup(--remaining);
    edits[i].SetLastSequence(last_seqno++);
    autovector<VersionEdit*> tmp_edits;
    tmp_edits.push_back(&edits[i]);
    edit_lists.emplace_back(tmp_edits);
    ++i;
  }
  int called = 0;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:CheckOneAtomicGroup", [&](void* arg) {
        std::vector<VersionEdit*>* tmp_edits =
            reinterpret_cast<std::vector<VersionEdit*>*>(arg);
        EXPECT_EQ(kAtomicGroupSize - 1, tmp_edits->size());
        for (const auto e : *tmp_edits) {
          bool found = false;
          for (const auto& e2 : edits) {
            if (&e2 == e) {
              found = true;
              break;
            }
          }
          ASSERT_TRUE(found);
        }
        ++called;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  mutex_.Lock();
  s = versions_->LogAndApply(cfds, mutable_cf_options_list, edit_lists,
                             &mutex_);
  mutex_.Unlock();
  ASSERT_OK(s);
  ASSERT_EQ(1, called);
  if (cfd_to_drop->Unref()) {
    delete cfd_to_drop;
    cfd_to_drop = nullptr;
  }
}

INSTANTIATE_TEST_CASE_P(
    AtomicGroup, VersionSetTestDropOneCF,
    testing::Values(VersionSetTestBase::kColumnFamilyName1,
                    VersionSetTestBase::kColumnFamilyName2,
                    VersionSetTestBase::kColumnFamilyName3));

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
