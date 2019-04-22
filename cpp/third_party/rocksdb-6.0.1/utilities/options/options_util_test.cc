//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <cctype>
#include <unordered_map>

#include "options/options_parser.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_util.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace rocksdb {
class OptionsUtilTest : public testing::Test {
 public:
  OptionsUtilTest() : rnd_(0xFB) {
    env_.reset(new test::StringEnv(Env::Default()));
    dbname_ = test::PerThreadDBPath("options_util_test");
  }

 protected:
  std::unique_ptr<test::StringEnv> env_;
  std::string dbname_;
  Random rnd_;
};

bool IsBlockBasedTableFactory(TableFactory* tf) {
  return tf->Name() == BlockBasedTableFactory().Name();
}

TEST_F(OptionsUtilTest, SaveAndLoad) {
  const size_t kCFCount = 5;

  DBOptions db_opt;
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;
  test::RandomInitDBOptions(&db_opt, &rnd_);
  for (size_t i = 0; i < kCFCount; ++i) {
    cf_names.push_back(i == 0 ? kDefaultColumnFamilyName
                              : test::RandomName(&rnd_, 10));
    cf_opts.emplace_back();
    test::RandomInitCFOptions(&cf_opts.back(), &rnd_);
  }

  const std::string kFileName = "OPTIONS-123456";
  PersistRocksDBOptions(db_opt, cf_names, cf_opts, kFileName, env_.get());

  DBOptions loaded_db_opt;
  std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
  ASSERT_OK(LoadOptionsFromFile(kFileName, env_.get(), &loaded_db_opt,
                                &loaded_cf_descs));

  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(db_opt, loaded_db_opt));
  test::RandomInitDBOptions(&db_opt, &rnd_);
  ASSERT_NOK(RocksDBOptionsParser::VerifyDBOptions(db_opt, loaded_db_opt));

  for (size_t i = 0; i < kCFCount; ++i) {
    ASSERT_EQ(cf_names[i], loaded_cf_descs[i].name);
    ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
        cf_opts[i], loaded_cf_descs[i].options));
    if (IsBlockBasedTableFactory(cf_opts[i].table_factory.get())) {
      ASSERT_OK(RocksDBOptionsParser::VerifyTableFactory(
          cf_opts[i].table_factory.get(),
          loaded_cf_descs[i].options.table_factory.get()));
    }
    test::RandomInitCFOptions(&cf_opts[i], &rnd_);
    ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
        cf_opts[i], loaded_cf_descs[i].options));
  }

  for (size_t i = 0; i < kCFCount; ++i) {
    if (cf_opts[i].compaction_filter) {
      delete cf_opts[i].compaction_filter;
    }
  }
}

namespace {
class DummyTableFactory : public TableFactory {
 public:
  DummyTableFactory() {}
  ~DummyTableFactory() override {}

  const char* Name() const override { return "DummyTableFactory"; }

  Status NewTableReader(
      const TableReaderOptions& /*table_reader_options*/,
      std::unique_ptr<RandomAccessFileReader>&& /*file*/,
      uint64_t /*file_size*/, std::unique_ptr<TableReader>* /*table_reader*/,
      bool /*prefetch_index_and_filter_in_cache*/) const override {
    return Status::NotSupported();
  }

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& /*table_builder_options*/,
      uint32_t /*column_family_id*/,
      WritableFileWriter* /*file*/) const override {
    return nullptr;
  }

  Status SanitizeOptions(
      const DBOptions& /*db_opts*/,
      const ColumnFamilyOptions& /*cf_opts*/) const override {
    return Status::NotSupported();
  }

  std::string GetPrintableTableOptions() const override { return ""; }

  Status GetOptionString(std::string* /*opt_string*/,
                         const std::string& /*delimiter*/) const override {
    return Status::OK();
  }
};

class DummyMergeOperator : public MergeOperator {
 public:
  DummyMergeOperator() {}
  ~DummyMergeOperator() override {}

  bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                   MergeOperationOutput* /*merge_out*/) const override {
    return false;
  }

  bool PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& /*operand_list*/,
                         std::string* /*new_value*/,
                         Logger* /*logger*/) const override {
    return false;
  }

  const char* Name() const override { return "DummyMergeOperator"; }
};

class DummySliceTransform : public SliceTransform {
 public:
  DummySliceTransform() {}
  ~DummySliceTransform() override {}

  // Return the name of this transformation.
  const char* Name() const override { return "DummySliceTransform"; }

  // transform a src in domain to a dst in the range
  Slice Transform(const Slice& src) const override { return src; }

  // determine whether this is a valid src upon the function applies
  bool InDomain(const Slice& /*src*/) const override { return false; }

  // determine whether dst=Transform(src) for some src
  bool InRange(const Slice& /*dst*/) const override { return false; }
};

}  // namespace

TEST_F(OptionsUtilTest, SanityCheck) {
  DBOptions db_opt;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  const size_t kCFCount = 5;
  for (size_t i = 0; i < kCFCount; ++i) {
    cf_descs.emplace_back();
    cf_descs.back().name =
        (i == 0) ? kDefaultColumnFamilyName : test::RandomName(&rnd_, 10);

    cf_descs.back().options.table_factory.reset(NewBlockBasedTableFactory());
    // Assign non-null values to prefix_extractors except the first cf.
    cf_descs.back().options.prefix_extractor.reset(
        i != 0 ? test::RandomSliceTransform(&rnd_) : nullptr);
    cf_descs.back().options.merge_operator.reset(
        test::RandomMergeOperator(&rnd_));
  }

  db_opt.create_missing_column_families = true;
  db_opt.create_if_missing = true;

  DestroyDB(dbname_, Options(db_opt, cf_descs[0].options));
  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  // open and persist the options
  ASSERT_OK(DB::Open(db_opt, dbname_, cf_descs, &handles, &db));

  // close the db
  for (auto* handle : handles) {
    delete handle;
  }
  delete db;

  // perform sanity check
  ASSERT_OK(
      CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

  ASSERT_GE(kCFCount, 5);
  // merge operator
  {
    std::shared_ptr<MergeOperator> merge_op =
        cf_descs[0].options.merge_operator;

    ASSERT_NE(merge_op.get(), nullptr);
    cf_descs[0].options.merge_operator.reset();
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[0].options.merge_operator.reset(new DummyMergeOperator());
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[0].options.merge_operator = merge_op;
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));
  }

  // prefix extractor
  {
    std::shared_ptr<const SliceTransform> prefix_extractor =
        cf_descs[1].options.prefix_extractor;

    // It's okay to set prefix_extractor to nullptr.
    ASSERT_NE(prefix_extractor, nullptr);
    cf_descs[1].options.prefix_extractor.reset();
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[1].options.prefix_extractor.reset(new DummySliceTransform());
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[1].options.prefix_extractor = prefix_extractor;
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));
  }

  // prefix extractor nullptr case
  {
    std::shared_ptr<const SliceTransform> prefix_extractor =
        cf_descs[0].options.prefix_extractor;

    // It's okay to set prefix_extractor to nullptr.
    ASSERT_EQ(prefix_extractor, nullptr);
    cf_descs[0].options.prefix_extractor.reset();
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    // It's okay to change prefix_extractor from nullptr to non-nullptr
    cf_descs[0].options.prefix_extractor.reset(new DummySliceTransform());
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[0].options.prefix_extractor = prefix_extractor;
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));
  }

  // comparator
  {
    test::SimpleSuffixReverseComparator comparator;

    auto* prev_comparator = cf_descs[2].options.comparator;
    cf_descs[2].options.comparator = &comparator;
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[2].options.comparator = prev_comparator;
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));
  }

  // table factory
  {
    std::shared_ptr<TableFactory> table_factory =
        cf_descs[3].options.table_factory;

    ASSERT_NE(table_factory, nullptr);
    cf_descs[3].options.table_factory.reset(new DummyTableFactory());
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[3].options.table_factory = table_factory;
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}

#else
#include <cstdio>

int main(int /*argc*/, char** /*argv*/) {
  printf("Skipped in RocksDBLite as utilities are not supported.\n");
  return 0;
}
#endif  // !ROCKSDB_LITE
