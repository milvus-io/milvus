//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <functional>

#include "db/db_iter.h"
#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"

namespace rocksdb {

// A dumb ReadCallback which saying every key is committed.
class DummyReadCallback : public ReadCallback {
  bool IsVisible(SequenceNumber /*seq*/) override { return true; }
};

// Test param:
//   bool: whether to pass read_callback to NewIterator().
class DBIteratorTest : public DBTestBase,
                       public testing::WithParamInterface<bool> {
 public:
  DBIteratorTest() : DBTestBase("/db_iterator_test") {}

  Iterator* NewIterator(const ReadOptions& read_options,
                        ColumnFamilyHandle* column_family = nullptr) {
    if (column_family == nullptr) {
      column_family = db_->DefaultColumnFamily();
    }
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
    SequenceNumber seq = read_options.snapshot != nullptr
                             ? read_options.snapshot->GetSequenceNumber()
                             : db_->GetLatestSequenceNumber();
    bool use_read_callback = GetParam();
    ReadCallback* read_callback = use_read_callback ? &read_callback_ : nullptr;
    return dbfull()->NewIteratorImpl(read_options, cfd, seq, read_callback);
  }

 private:
  DummyReadCallback read_callback_;
};

class FlushBlockEveryKeyPolicy : public FlushBlockPolicy {
 public:
  bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
    if (!start_) {
      start_ = true;
      return false;
    }
    return true;
  }

 private:
  bool start_ = false;
};

class FlushBlockEveryKeyPolicyFactory : public FlushBlockPolicyFactory {
 public:
  explicit FlushBlockEveryKeyPolicyFactory() {}

  const char* Name() const override {
    return "FlushBlockEveryKeyPolicyFactory";
  }

  FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& /*data_block_builder*/) const override {
    return new FlushBlockEveryKeyPolicy;
  }
};

TEST_P(DBIteratorTest, IteratorProperty) {
  // The test needs to be changed if kPersistedTier is supported in iterator.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  Put(1, "1", "2");
  Delete(1, "2");
  ReadOptions ropt;
  ropt.pin_data = false;
  {
    std::unique_ptr<Iterator> iter(NewIterator(ropt, handles_[1]));
    iter->SeekToFirst();
    std::string prop_value;
    ASSERT_NOK(iter->GetProperty("non_existing.value", &prop_value));
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("0", prop_value);
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.internal-key", &prop_value));
    ASSERT_EQ("1", prop_value);
    iter->Next();
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("Iterator is not valid.", prop_value);

    // Get internal key at which the iteration stopped (tombstone in this case).
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.internal-key", &prop_value));
    ASSERT_EQ("2", prop_value);
  }
  Close();
}

TEST_P(DBIteratorTest, PersistedTierOnIterator) {
  // The test needs to be changed if kPersistedTier is supported in iterator.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ReadOptions ropt;
  ropt.read_tier = kPersistedTier;

  auto* iter = db_->NewIterator(ropt, handles_[1]);
  ASSERT_TRUE(iter->status().IsNotSupported());
  delete iter;

  std::vector<Iterator*> iters;
  ASSERT_TRUE(db_->NewIterators(ropt, {handles_[1]}, &iters).IsNotSupported());
  Close();
}

TEST_P(DBIteratorTest, NonBlockingIteration) {
  do {
    ReadOptions non_blocking_opts, regular_opts;
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    non_blocking_opts.read_tier = kBlockCacheTier;
    CreateAndReopenWithCF({"pikachu"}, options);
    // write one kv to the database.
    ASSERT_OK(Put(1, "a", "b"));

    // scan using non-blocking iterator. We should find it because
    // it is in memtable.
    Iterator* iter = NewIterator(non_blocking_opts, handles_[1]);
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_EQ(count, 1);
    delete iter;

    // flush memtable to storage. Now, the key should not be in the
    // memtable neither in the block cache.
    ASSERT_OK(Flush(1));

    // verify that a non-blocking iterator does not find any
    // kvs. Neither does it do any IOs to storage.
    uint64_t numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    uint64_t cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      count++;
    }
    ASSERT_EQ(count, 0);
    ASSERT_TRUE(iter->status().IsIncomplete());
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // read in the specified block via a regular get
    ASSERT_EQ(Get(1, "a"), "b");

    // verify that we can find it via a non-blocking scan
    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_EQ(count, 1);
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // This test verifies block cache behaviors, which is not used by plain
    // table format.
  } while (ChangeOptions(kSkipPlainTable | kSkipNoSeekToLast | kSkipMmapReads));
}

TEST_P(DBIteratorTest, IterSeekBeforePrev) {
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("0", "f"));
  ASSERT_OK(Put("1", "h"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("2", "j"));
  auto iter = NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  iter->Prev();
  iter->Seek(Slice("a"));
  iter->Prev();
  delete iter;
}

TEST_P(DBIteratorTest, IterSeekForPrevBeforeNext) {
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("0", "f"));
  ASSERT_OK(Put("1", "h"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("2", "j"));
  auto iter = NewIterator(ReadOptions());
  iter->SeekForPrev(Slice("0"));
  iter->Next();
  iter->SeekForPrev(Slice("1"));
  iter->Next();
  delete iter;
}

namespace {
std::string MakeLongKey(size_t length, char c) {
  return std::string(length, c);
}
}  // namespace

TEST_P(DBIteratorTest, IterLongKeys) {
  ASSERT_OK(Put(MakeLongKey(20, 0), "0"));
  ASSERT_OK(Put(MakeLongKey(32, 2), "2"));
  ASSERT_OK(Put("a", "b"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put(MakeLongKey(50, 1), "1"));
  ASSERT_OK(Put(MakeLongKey(127, 3), "3"));
  ASSERT_OK(Put(MakeLongKey(64, 4), "4"));
  auto iter = NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  iter->Seek(MakeLongKey(20, 0));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(20, 0) + "->0");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(64, 4) + "->4");

  iter->SeekForPrev(MakeLongKey(127, 3));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  delete iter;

  iter = NewIterator(ReadOptions());
  iter->Seek(MakeLongKey(50, 1));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  delete iter;
}

TEST_P(DBIteratorTest, IterNextWithNewerSeq) {
  ASSERT_OK(Put("0", "0"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("a"));
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->SeekForPrev(Slice("b"));
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->d");

  delete iter;
}

TEST_P(DBIteratorTest, IterPrevWithNewerSeq) {
  ASSERT_OK(Put("0", "0"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "d->e");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  iter->SeekForPrev(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "d->e");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  delete iter;
}

TEST_P(DBIteratorTest, IterPrevWithNewerSeq2) {
  ASSERT_OK(Put("0", "0"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("e", "f"));
  auto iter = NewIterator(ReadOptions());
  auto iter2 = NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  iter2->SeekForPrev(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "c->d");
  ASSERT_EQ(IterStatus(iter2), "c->d");

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  iter2->Prev();
  ASSERT_EQ(IterStatus(iter2), "a->b");
  iter2->Prev();
  delete iter;
  delete iter2;
}

TEST_P(DBIteratorTest, IterEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("foo");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekForPrev("foo");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IterSingle) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("b");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IterMulti) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", "vb"));
    ASSERT_OK(Put(1, "c", "vc"));
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("ax");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->SeekForPrev("d");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->SeekForPrev("c");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->SeekForPrev("bx");
    ASSERT_EQ(IterStatus(iter), "b->vb");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Seek("z");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("b");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    // Switch from reverse to forward
    iter->SeekToLast();
    iter->Prev();
    iter->Prev();
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Switch from forward to reverse
    iter->SeekToFirst();
    iter->Next();
    iter->Next();
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Make sure iter stays at snapshot
    ASSERT_OK(Put(1, "a", "va2"));
    ASSERT_OK(Put(1, "a2", "va3"));
    ASSERT_OK(Put(1, "b", "vb2"));
    ASSERT_OK(Put(1, "c", "vc2"));
    ASSERT_OK(Delete(1, "b"));
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

// Check that we can skip over a run of user keys
// by using reseek rather than sequential scan
TEST_P(DBIteratorTest, IterReseek) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  options.max_sequential_skip_in_iterations = 3;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // insert three keys with same userkey and verify that
  // reseek is not invoked. For each of these test cases,
  // verify that we can find the next key "b".
  ASSERT_OK(Put(1, "a", "zero"));
  ASSERT_OK(Put(1, "a", "one"));
  ASSERT_OK(Put(1, "a", "two"));
  ASSERT_OK(Put(1, "b", "bone"));
  Iterator* iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "a->two");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of three keys with same userkey and verify
  // that reseek is still not invoked.
  ASSERT_OK(Put(1, "a", "three"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->three");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of four keys with same userkey and verify
  // that reseek is invoked.
  ASSERT_OK(Put(1, "a", "four"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->four");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // Testing reverse iterator
  // At this point, we have three versions of "a" and one version of "b".
  // The reseek statistics is already at 1.
  int num_reseeks = static_cast<int>(
      TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION));

  // Insert another version of b and assert that reseek is not invoked
  ASSERT_OK(Put(1, "b", "btwo"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->btwo");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 1);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;

  // insert two more versions of b. This makes a total of 4 versions
  // of b and 4 versions of a.
  ASSERT_OK(Put(1, "b", "bthree"));
  ASSERT_OK(Put(1, "b", "bfour"));
  iter = NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->bfour");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 2);
  iter->Prev();

  // the previous Prev call should have invoked reseek
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 3);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;
}

TEST_P(DBIteratorTest, IterSmallAndLargeMix) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", std::string(100000, 'b')));
    ASSERT_OK(Put(1, "c", "vc"));
    ASSERT_OK(Put(1, "d", std::string(100000, 'd')));
    ASSERT_OK(Put(1, "e", std::string(100000, 'e')));

    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_P(DBIteratorTest, IterMultiWithDelete) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "ka", "va"));
    ASSERT_OK(Put(1, "kb", "vb"));
    ASSERT_OK(Put(1, "kc", "vc"));
    ASSERT_OK(Delete(1, "kb"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kb"));

    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);
    iter->Seek("kc");
    ASSERT_EQ(IterStatus(iter), "kc->vc");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ &&
          kHashSkipList != option_config_) {  // doesn't support SeekToLast
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "ka->va");
      }
    }
    delete iter;
  } while (ChangeOptions());
}

TEST_P(DBIteratorTest, IterPrevMaxSkip) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    for (int i = 0; i < 2; i++) {
      ASSERT_OK(Put(1, "key1", "v1"));
      ASSERT_OK(Put(1, "key2", "v2"));
      ASSERT_OK(Put(1, "key3", "v3"));
      ASSERT_OK(Put(1, "key4", "v4"));
      ASSERT_OK(Put(1, "key5", "v5"));
    }

    VerifyIterLast("key5->v5", 1);

    ASSERT_OK(Delete(1, "key5"));
    VerifyIterLast("key4->v4", 1);

    ASSERT_OK(Delete(1, "key4"));
    VerifyIterLast("key3->v3", 1);

    ASSERT_OK(Delete(1, "key3"));
    VerifyIterLast("key2->v2", 1);

    ASSERT_OK(Delete(1, "key2"));
    VerifyIterLast("key1->v1", 1);

    ASSERT_OK(Delete(1, "key1"));
    VerifyIterLast("(invalid)", 1);
  } while (ChangeOptions(kSkipMergePut | kSkipNoSeekToLast));
}

TEST_P(DBIteratorTest, IterWithSnapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    ASSERT_OK(Put(1, "key1", "val1"));
    ASSERT_OK(Put(1, "key2", "val2"));
    ASSERT_OK(Put(1, "key3", "val3"));
    ASSERT_OK(Put(1, "key4", "val4"));
    ASSERT_OK(Put(1, "key5", "val5"));

    const Snapshot* snapshot = db_->GetSnapshot();
    ReadOptions options;
    options.snapshot = snapshot;
    Iterator* iter = NewIterator(options, handles_[1]);

    ASSERT_OK(Put(1, "key0", "val0"));
    // Put more values after the snapshot
    ASSERT_OK(Put(1, "key100", "val100"));
    ASSERT_OK(Put(1, "key101", "val101"));

    iter->Seek("key5");
    ASSERT_EQ(IterStatus(iter), "key5->val5");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ && kHashSkipList != option_config_) {
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key3->val3");

        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key5->val5");
      }
      iter->Next();
      ASSERT_TRUE(!iter->Valid());
    }

    if (!CurrentOptions().merge_operator) {
      // TODO(gzh): merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ && kHashSkipList != option_config_) {
        iter->SeekForPrev("key1");
        ASSERT_EQ(IterStatus(iter), "key1->val1");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key2->val2");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key3->val3");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key2->val2");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key1->val1");
        iter->Prev();
        ASSERT_TRUE(!iter->Valid());
      }
    }
    db_->ReleaseSnapshot(snapshot);
    delete iter;
  } while (ChangeOptions());
}

TEST_P(DBIteratorTest, IteratorPinsRef) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Put(1, "foo", "hello");

    // Get iterator that will yield the current contents of the DB.
    Iterator* iter = NewIterator(ReadOptions(), handles_[1]);

    // Write to force compactions
    Put(1, "foo", "newvalue1");
    for (int i = 0; i < 100; i++) {
      // 100K values
      ASSERT_OK(Put(1, Key(i), Key(i) + std::string(100000, 'v')));
    }
    Put(1, "foo", "newvalue2");

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ("hello", iter->value().ToString());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  } while (ChangeCompactOptions());
}

// SetOptions not defined in ROCKSDB LITE
#ifndef ROCKSDB_LITE
TEST_P(DBIteratorTest, DBIteratorBoundTest) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;

  options.prefix_extractor = nullptr;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing basic case with no iterate_upper_bound and no prefix_extractor
  {
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("g1")), 0);

    iter->SeekForPrev("g1");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("g1")), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);
  }

  // testing iterate_upper_bound and forward iterator
  // to make sure it stops at bound
  {
    ReadOptions ro;
    // iterate_upper_bound points beyond the last expected entry
    Slice prefix("foo2");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("foo1")), 0);

    iter->Next();
    // should stop here...
    ASSERT_TRUE(!iter->Valid());
  }
  // Testing SeekToLast with iterate_upper_bound set
  {
    ReadOptions ro;

    Slice prefix("foo");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("a")), 0);
  }

  // prefix is the first letter of the key
  ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:1"}}));
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing with iterate_upper_bound and prefix_extractor
  // Seek target and iterate_upper_bound are not is same prefix
  // This should be an error
  {
    ReadOptions ro;
    Slice upper_bound("g");
    ro.iterate_upper_bound = &upper_bound;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo1", iter->key().ToString());

    iter->Next();
    ASSERT_TRUE(!iter->Valid());
  }

  // testing that iterate_upper_bound prevents iterating over deleted items
  // if the bound has already reached
  {
    options.prefix_extractor = nullptr;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_OK(Put("b", "0"));
    ASSERT_OK(Put("b1", "0"));
    ASSERT_OK(Put("c", "0"));
    ASSERT_OK(Put("d", "0"));
    ASSERT_OK(Put("e", "0"));
    ASSERT_OK(Delete("c"));
    ASSERT_OK(Delete("d"));

    // base case with no bound
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    get_perf_context()->Reset();
    iter->Next();

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(static_cast<int>(get_perf_context()->internal_delete_skipped_count), 2);

    // now testing with iterate_bound
    Slice prefix("c");
    ro.iterate_upper_bound = &prefix;

    iter.reset(NewIterator(ro));

    get_perf_context()->Reset();

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    iter->Next();
    // the iteration should stop as soon as the bound key is reached
    // even though the key is deleted
    // hence internal_delete_skipped_count should be 0
    ASSERT_TRUE(!iter->Valid());
    ASSERT_EQ(static_cast<int>(get_perf_context()->internal_delete_skipped_count), 0);
  }
}

TEST_P(DBIteratorTest, DBIteratorBoundMultiSeek) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.prefix_extractor = nullptr;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("z", "0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("foo2", "bar2"));
  ASSERT_OK(Put("foo3", "bar3"));
  ASSERT_OK(Put("foo4", "bar4"));

  {
    std::string up_str = "foo5";
    Slice up(up_str);
    ReadOptions ro;
    ro.iterate_upper_bound = &up;
    std::unique_ptr<Iterator> iter(NewIterator(ro));

    iter->Seek("foo1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    uint64_t prev_block_cache_hit =
        TestGetTickerCount(options, BLOCK_CACHE_HIT);
    uint64_t prev_block_cache_miss =
        TestGetTickerCount(options, BLOCK_CACHE_MISS);

    ASSERT_GT(prev_block_cache_hit + prev_block_cache_miss, 0);

    iter->Seek("foo4");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo4")), 0);
    ASSERT_EQ(prev_block_cache_hit,
              TestGetTickerCount(options, BLOCK_CACHE_HIT));
    ASSERT_EQ(prev_block_cache_miss,
              TestGetTickerCount(options, BLOCK_CACHE_MISS));

    iter->Seek("foo2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo2")), 0);
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo3")), 0);
    ASSERT_EQ(prev_block_cache_hit,
              TestGetTickerCount(options, BLOCK_CACHE_HIT));
    ASSERT_EQ(prev_block_cache_miss,
              TestGetTickerCount(options, BLOCK_CACHE_MISS));
  }
}
#endif

TEST_P(DBIteratorTest, DBIteratorBoundOptimizationTest) {
  int upper_bound_hits = 0;
  Options options = CurrentOptions();
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::BlockEntryIteratorState::KeyReachedUpperBound",
      [&upper_bound_hits](void* arg) {
        assert(arg != nullptr);
        upper_bound_hits += (*static_cast<bool*>(arg) ? 1 : 0);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor = nullptr;
  BlockBasedTableOptions table_options;
  table_options.flush_block_policy_factory =
    std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("foo2", "bar2"));
  ASSERT_OK(Put("foo4", "bar4"));
  ASSERT_OK(Flush());

  Slice ub("foo3");
  ReadOptions ro;
  ro.iterate_upper_bound = &ub;

  std::unique_ptr<Iterator> iter(NewIterator(ro));

  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);
  ASSERT_EQ(upper_bound_hits, 0);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("foo2")), 0);
  ASSERT_EQ(upper_bound_hits, 0);

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(upper_bound_hits, 1);
}
// TODO(3.13): fix the issue of Seek() + Prev() which might not necessary
//             return the biggest key which is smaller than the seek key.
TEST_P(DBIteratorTest, PrevAfterAndNextAfterMerge) {
  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.env = env_;
  DestroyAndReopen(options);

  // write three entries with different keys using Merge()
  WriteOptions wopts;
  db_->Merge(wopts, "1", "data1");
  db_->Merge(wopts, "2", "data2");
  db_->Merge(wopts, "3", "data3");

  std::unique_ptr<Iterator> it(NewIterator(ReadOptions()));

  it->Seek("2");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("2", it->key().ToString());

  it->Prev();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("1", it->key().ToString());

  it->SeekForPrev("1");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("1", it->key().ToString());

  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("2", it->key().ToString());
}

class DBIteratorTestForPinnedData : public DBIteratorTest {
 public:
  enum TestConfig {
    NORMAL,
    CLOSE_AND_OPEN,
    COMPACT_BEFORE_READ,
    FLUSH_EVERY_1000,
    MAX
  };
  DBIteratorTestForPinnedData() : DBIteratorTest() {}
  void PinnedDataIteratorRandomized(TestConfig run_config) {
    // Generate Random data
    Random rnd(301);

    int puts = 100000;
    int key_pool = static_cast<int>(puts * 0.7);
    int key_size = 100;
    int val_size = 1000;
    int seeks_percentage = 20;   // 20% of keys will be used to test seek()
    int delete_percentage = 20;  // 20% of keys will be deleted
    int merge_percentage = 20;   // 20% of keys will be added using Merge()

    Options options = CurrentOptions();
    BlockBasedTableOptions table_options;
    table_options.use_delta_encoding = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.merge_operator = MergeOperators::CreatePutOperator();
    DestroyAndReopen(options);

    std::vector<std::string> generated_keys(key_pool);
    for (int i = 0; i < key_pool; i++) {
      generated_keys[i] = RandomString(&rnd, key_size);
    }

    std::map<std::string, std::string> true_data;
    std::vector<std::string> random_keys;
    std::vector<std::string> deleted_keys;
    for (int i = 0; i < puts; i++) {
      auto& k = generated_keys[rnd.Next() % key_pool];
      auto v = RandomString(&rnd, val_size);

      // Insert data to true_data map and to DB
      true_data[k] = v;
      if (rnd.OneIn(static_cast<int>(100.0 / merge_percentage))) {
        ASSERT_OK(db_->Merge(WriteOptions(), k, v));
      } else {
        ASSERT_OK(Put(k, v));
      }

      // Pick random keys to be used to test Seek()
      if (rnd.OneIn(static_cast<int>(100.0 / seeks_percentage))) {
        random_keys.push_back(k);
      }

      // Delete some random keys
      if (rnd.OneIn(static_cast<int>(100.0 / delete_percentage))) {
        deleted_keys.push_back(k);
        true_data.erase(k);
        ASSERT_OK(Delete(k));
      }

      if (run_config == TestConfig::FLUSH_EVERY_1000) {
        if (i && i % 1000 == 0) {
          Flush();
        }
      }
    }

    if (run_config == TestConfig::CLOSE_AND_OPEN) {
      Close();
      Reopen(options);
    } else if (run_config == TestConfig::COMPACT_BEFORE_READ) {
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    }

    ReadOptions ro;
    ro.pin_data = true;
    auto iter = NewIterator(ro);

    {
      // Test Seek to random keys
      std::vector<Slice> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->Seek(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.lower_bound(k), true_data.end());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key());
        true_keys.push_back(true_data.lower_bound(k)->first);
      }

      for (size_t i = 0; i < keys_slices.size(); i++) {
        ASSERT_EQ(keys_slices[i].ToString(), true_keys[i]);
      }
    }

    {
      // Test SeekForPrev to random keys
      std::vector<Slice> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->SeekForPrev(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.upper_bound(k), true_data.begin());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key());
        true_keys.push_back((--true_data.upper_bound(k))->first);
      }

      for (size_t i = 0; i < keys_slices.size(); i++) {
        ASSERT_EQ(keys_slices[i].ToString(), true_keys[i]);
      }
    }

    {
      // Test iterating all data forward
      std::vector<Slice> all_keys;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key());
      }
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid
      auto data_iter = true_data.begin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i].ToString(), data_iter->first);
        data_iter++;
      }
    }

    {
      // Test iterating all data backward
      std::vector<Slice> all_keys;
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key());
      }
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid (backward)
      auto data_iter = true_data.rbegin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i].ToString(), data_iter->first);
        data_iter++;
      }
    }

    delete iter;
}
};

TEST_P(DBIteratorTestForPinnedData, PinnedDataIteratorRandomizedNormal) {
  PinnedDataIteratorRandomized(TestConfig::NORMAL);
}

TEST_P(DBIteratorTestForPinnedData, PinnedDataIteratorRandomizedCLoseAndOpen) {
  PinnedDataIteratorRandomized(TestConfig::CLOSE_AND_OPEN);
}

TEST_P(DBIteratorTestForPinnedData,
       PinnedDataIteratorRandomizedCompactBeforeRead) {
  PinnedDataIteratorRandomized(TestConfig::COMPACT_BEFORE_READ);
}

TEST_P(DBIteratorTestForPinnedData, PinnedDataIteratorRandomizedFlush) {
  PinnedDataIteratorRandomized(TestConfig::FLUSH_EVERY_1000);
}

#ifndef ROCKSDB_LITE
TEST_P(DBIteratorTest, PinnedDataIteratorMultipleFiles) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.disable_auto_compactions = true;
  options.write_buffer_size = 1024 * 1024 * 10;  // 10 Mb
  DestroyAndReopen(options);

  std::map<std::string, std::string> true_data;

  // Generate 4 sst files in L2
  Random rnd(301);
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i * 3);
    std::string v = RandomString(&rnd, 100);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
    if (i % 250 == 0) {
      ASSERT_OK(Flush());
    }
  }
  ASSERT_EQ(FilesPerLevel(0), "4");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(FilesPerLevel(0), "0,4");

  // Generate 4 sst files in L0
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i * 2);
    std::string v = RandomString(&rnd, 100);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
    if (i % 250 == 0) {
      ASSERT_OK(Flush());
    }
  }
  ASSERT_EQ(FilesPerLevel(0), "4,4");

  // Add some keys/values in memtables
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i);
    std::string v = RandomString(&rnd, 100);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
  }
  ASSERT_EQ(FilesPerLevel(0), "4,4");

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = NewIterator(ro);

  std::vector<std::pair<Slice, std::string>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key(), iter->value().ToString());
  }

  ASSERT_EQ(results.size(), true_data.size());
  auto data_iter = true_data.begin();
  for (size_t i = 0; i < results.size(); i++, data_iter++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, data_iter->first);
    ASSERT_EQ(kv.second, data_iter->second);
  }

  delete iter;
}
#endif

TEST_P(DBIteratorTest, PinnedDataIteratorMergeOperator) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  DestroyAndReopen(options);

  std::string numbers[7];
  for (int val = 0; val <= 6; val++) {
    PutFixed64(numbers + val, val);
  }

  // +1 all keys in range [ 0 => 999]
  for (int i = 0; i < 1000; i++) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[1]));
  }

  // +2 all keys divisible by 2 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 2) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[2]));
  }

  // +3 all keys divisible by 5 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 5) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[3]));
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = NewIterator(ro);

  std::vector<std::pair<Slice, std::string>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key(), iter->value().ToString());
  }

  ASSERT_EQ(results.size(), 1000);
  for (size_t i = 0; i < results.size(); i++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, Key(static_cast<int>(i)));
    int expected_val = 1;
    if (i % 2 == 0) {
      expected_val += 2;
    }
    if (i % 5 == 0) {
      expected_val += 3;
    }
    ASSERT_EQ(kv.second, numbers[expected_val]);
  }

  delete iter;
}

TEST_P(DBIteratorTest, PinnedDataIteratorReadAfterUpdate) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.write_buffer_size = 100000;
  DestroyAndReopen(options);

  Random rnd(301);

  std::map<std::string, std::string> true_data;
  for (int i = 0; i < 1000; i++) {
    std::string k = RandomString(&rnd, 10);
    std::string v = RandomString(&rnd, 1000);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = NewIterator(ro);

  // Delete 50% of the keys and update the other 50%
  for (auto& kv : true_data) {
    if (rnd.OneIn(2)) {
      ASSERT_OK(Delete(kv.first));
    } else {
      std::string new_val = RandomString(&rnd, 1000);
      ASSERT_OK(Put(kv.first, new_val));
    }
  }

  std::vector<std::pair<Slice, std::string>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key(), iter->value().ToString());
  }

  auto data_iter = true_data.begin();
  for (size_t i = 0; i < results.size(); i++, data_iter++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, data_iter->first);
    ASSERT_EQ(kv.second, data_iter->second);
  }

  delete iter;
}

class SliceTransformLimitedDomainGeneric : public SliceTransform {
  const char* Name() const override {
    return "SliceTransformLimitedDomainGeneric";
  }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 1);
  }

  bool InDomain(const Slice& src) const override {
    // prefix will be x????
    return src.size() >= 1;
  }

  bool InRange(const Slice& dst) const override {
    // prefix will be x????
    return dst.size() == 1;
  }
};

TEST_P(DBIteratorTest, IterSeekForPrevCrossingFiles) {
  Options options = CurrentOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.disable_auto_compactions = true;
  // Enable prefix bloom for SST files
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "va1"));
  ASSERT_OK(Put("a2", "va2"));
  ASSERT_OK(Put("a3", "va3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b1", "vb1"));
  ASSERT_OK(Put("b2", "vb2"));
  ASSERT_OK(Put("b3", "vb3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b4", "vb4"));
  ASSERT_OK(Put("d1", "vd1"));
  ASSERT_OK(Put("d2", "vd2"));
  ASSERT_OK(Put("d4", "vd4"));
  ASSERT_OK(Flush());

  MoveFilesToLevel(1);
  {
    ReadOptions ro;
    Iterator* iter = NewIterator(ro);

    iter->SeekForPrev("a4");
    ASSERT_EQ(iter->key().ToString(), "a3");
    ASSERT_EQ(iter->value().ToString(), "va3");

    iter->SeekForPrev("c2");
    ASSERT_EQ(iter->key().ToString(), "b3");
    iter->SeekForPrev("d3");
    ASSERT_EQ(iter->key().ToString(), "d2");
    iter->SeekForPrev("b5");
    ASSERT_EQ(iter->key().ToString(), "b4");
    delete iter;
  }

  {
    ReadOptions ro;
    ro.prefix_same_as_start = true;
    Iterator* iter = NewIterator(ro);
    iter->SeekForPrev("c2");
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  }
}

TEST_P(DBIteratorTest, IterSeekForPrevCrossingFilesCustomPrefixExtractor) {
  Options options = CurrentOptions();
  options.prefix_extractor =
      std::make_shared<SliceTransformLimitedDomainGeneric>();
  options.disable_auto_compactions = true;
  // Enable prefix bloom for SST files
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "va1"));
  ASSERT_OK(Put("a2", "va2"));
  ASSERT_OK(Put("a3", "va3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b1", "vb1"));
  ASSERT_OK(Put("b2", "vb2"));
  ASSERT_OK(Put("b3", "vb3"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b4", "vb4"));
  ASSERT_OK(Put("d1", "vd1"));
  ASSERT_OK(Put("d2", "vd2"));
  ASSERT_OK(Put("d4", "vd4"));
  ASSERT_OK(Flush());

  MoveFilesToLevel(1);
  {
    ReadOptions ro;
    Iterator* iter = NewIterator(ro);

    iter->SeekForPrev("a4");
    ASSERT_EQ(iter->key().ToString(), "a3");
    ASSERT_EQ(iter->value().ToString(), "va3");

    iter->SeekForPrev("c2");
    ASSERT_EQ(iter->key().ToString(), "b3");
    iter->SeekForPrev("d3");
    ASSERT_EQ(iter->key().ToString(), "d2");
    iter->SeekForPrev("b5");
    ASSERT_EQ(iter->key().ToString(), "b4");
    delete iter;
  }

  {
    ReadOptions ro;
    ro.prefix_same_as_start = true;
    Iterator* iter = NewIterator(ro);
    iter->SeekForPrev("c2");
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  }
}

TEST_P(DBIteratorTest, IterPrevKeyCrossingBlocks) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1;  // every block will contain one entry
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  options.disable_auto_compactions = true;
  options.max_sequential_skip_in_iterations = 8;

  DestroyAndReopen(options);

  // Putting such deletes will force DBIter::Prev() to fallback to a Seek
  for (int file_num = 0; file_num < 10; file_num++) {
    ASSERT_OK(Delete("key4"));
    ASSERT_OK(Flush());
  }

  // First File containing 5 blocks of puts
  ASSERT_OK(Put("key1", "val1.0"));
  ASSERT_OK(Put("key2", "val2.0"));
  ASSERT_OK(Put("key3", "val3.0"));
  ASSERT_OK(Put("key4", "val4.0"));
  ASSERT_OK(Put("key5", "val5.0"));
  ASSERT_OK(Flush());

  // Second file containing 9 blocks of merge operands
  ASSERT_OK(db_->Merge(WriteOptions(), "key1", "val1.1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key1", "val1.2"));

  ASSERT_OK(db_->Merge(WriteOptions(), "key2", "val2.1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key2", "val2.2"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key2", "val2.3"));

  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.2"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.3"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key3", "val3.4"));
  ASSERT_OK(Flush());

  {
    ReadOptions ro;
    ro.fill_cache = false;
    Iterator* iter = NewIterator(ro);

    iter->SeekToLast();
    ASSERT_EQ(iter->key().ToString(), "key5");
    ASSERT_EQ(iter->value().ToString(), "val5.0");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key4");
    ASSERT_EQ(iter->value().ToString(), "val4.0");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key3");
    ASSERT_EQ(iter->value().ToString(), "val3.0,val3.1,val3.2,val3.3,val3.4");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key2");
    ASSERT_EQ(iter->value().ToString(), "val2.0,val2.1,val2.2,val2.3");

    iter->Prev();
    ASSERT_EQ(iter->key().ToString(), "key1");
    ASSERT_EQ(iter->value().ToString(), "val1.0,val1.1,val1.2");

    delete iter;
  }
}

TEST_P(DBIteratorTest, IterPrevKeyCrossingBlocksRandomized) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  options.disable_auto_compactions = true;
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.max_sequential_skip_in_iterations = 8;
  DestroyAndReopen(options);

  const int kNumKeys = 500;
  // Small number of merge operands to make sure that DBIter::Prev() dont
  // fall back to Seek()
  const int kNumMergeOperands = 3;
  // Use value size that will make sure that every block contain 1 key
  const int kValSize =
      static_cast<int>(BlockBasedTableOptions().block_size) * 4;
  // Percentage of keys that wont get merge operations
  const int kNoMergeOpPercentage = 20;
  // Percentage of keys that will be deleted
  const int kDeletePercentage = 10;

  // For half of the key range we will write multiple deletes first to
  // force DBIter::Prev() to fall back to Seek()
  for (int file_num = 0; file_num < 10; file_num++) {
    for (int i = 0; i < kNumKeys; i += 2) {
      ASSERT_OK(Delete(Key(i)));
    }
    ASSERT_OK(Flush());
  }

  Random rnd(301);
  std::map<std::string, std::string> true_data;
  std::string gen_key;
  std::string gen_val;

  for (int i = 0; i < kNumKeys; i++) {
    gen_key = Key(i);
    gen_val = RandomString(&rnd, kValSize);

    ASSERT_OK(Put(gen_key, gen_val));
    true_data[gen_key] = gen_val;
  }
  ASSERT_OK(Flush());

  // Separate values and merge operands in different file so that we
  // make sure that we dont merge them while flushing but actually
  // merge them in the read path
  for (int i = 0; i < kNumKeys; i++) {
    if (rnd.OneIn(static_cast<int>(100.0 / kNoMergeOpPercentage))) {
      // Dont give merge operations for some keys
      continue;
    }

    for (int j = 0; j < kNumMergeOperands; j++) {
      gen_key = Key(i);
      gen_val = RandomString(&rnd, kValSize);

      ASSERT_OK(db_->Merge(WriteOptions(), gen_key, gen_val));
      true_data[gen_key] += "," + gen_val;
    }
  }
  ASSERT_OK(Flush());

  for (int i = 0; i < kNumKeys; i++) {
    if (rnd.OneIn(static_cast<int>(100.0 / kDeletePercentage))) {
      gen_key = Key(i);

      ASSERT_OK(Delete(gen_key));
      true_data.erase(gen_key);
    }
  }
  ASSERT_OK(Flush());

  {
    ReadOptions ro;
    ro.fill_cache = false;
    Iterator* iter = NewIterator(ro);
    auto data_iter = true_data.rbegin();

    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      ASSERT_EQ(iter->value().ToString(), data_iter->second);
      data_iter++;
    }
    ASSERT_EQ(data_iter, true_data.rend());

    delete iter;
  }

  {
    ReadOptions ro;
    ro.fill_cache = false;
    Iterator* iter = NewIterator(ro);
    auto data_iter = true_data.rbegin();

    int entries_right = 0;
    std::string seek_key;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      // Verify key/value of current position
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      ASSERT_EQ(iter->value().ToString(), data_iter->second);

      bool restore_position_with_seek = rnd.Uniform(2);
      if (restore_position_with_seek) {
        seek_key = iter->key().ToString();
      }

      // Do some Next() operations the restore the iterator to orignal position
      int next_count =
          entries_right > 0 ? rnd.Uniform(std::min(entries_right, 10)) : 0;
      for (int i = 0; i < next_count; i++) {
        iter->Next();
        data_iter--;

        ASSERT_EQ(iter->key().ToString(), data_iter->first);
        ASSERT_EQ(iter->value().ToString(), data_iter->second);
      }

      if (restore_position_with_seek) {
        // Restore orignal position using Seek()
        iter->Seek(seek_key);
        for (int i = 0; i < next_count; i++) {
          data_iter++;
        }

        ASSERT_EQ(iter->key().ToString(), data_iter->first);
        ASSERT_EQ(iter->value().ToString(), data_iter->second);
      } else {
        // Restore original position using Prev()
        for (int i = 0; i < next_count; i++) {
          iter->Prev();
          data_iter++;

          ASSERT_EQ(iter->key().ToString(), data_iter->first);
          ASSERT_EQ(iter->value().ToString(), data_iter->second);
        }
      }

      entries_right++;
      data_iter++;
    }
    ASSERT_EQ(data_iter, true_data.rend());

    delete iter;
  }
}

TEST_P(DBIteratorTest, IteratorWithLocalStatistics) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 1000; i++) {
    // Key 10 bytes / Value 10 bytes
    ASSERT_OK(Put(RandomString(&rnd, 10), RandomString(&rnd, 10)));
  }

  std::atomic<uint64_t> total_next(0);
  std::atomic<uint64_t> total_next_found(0);
  std::atomic<uint64_t> total_prev(0);
  std::atomic<uint64_t> total_prev_found(0);
  std::atomic<uint64_t> total_bytes(0);

  std::vector<port::Thread> threads;
  std::function<void()> reader_func_next = [&]() {
    SetPerfLevel(kEnableCount);
    get_perf_context()->Reset();
    Iterator* iter = NewIterator(ReadOptions());

    iter->SeekToFirst();
    // Seek will bump ITER_BYTES_READ
    uint64_t bytes = 0;
    bytes += iter->key().size();
    bytes += iter->value().size();
    while (true) {
      iter->Next();
      total_next++;

      if (!iter->Valid()) {
        break;
      }
      total_next_found++;
      bytes += iter->key().size();
      bytes += iter->value().size();
    }

    delete iter;
    ASSERT_EQ(bytes, get_perf_context()->iter_read_bytes);
    SetPerfLevel(kDisable);
    total_bytes += bytes;
  };

  std::function<void()> reader_func_prev = [&]() {
    SetPerfLevel(kEnableCount);
    Iterator* iter = NewIterator(ReadOptions());

    iter->SeekToLast();
    // Seek will bump ITER_BYTES_READ
    uint64_t bytes = 0;
    bytes += iter->key().size();
    bytes += iter->value().size();
    while (true) {
      iter->Prev();
      total_prev++;

      if (!iter->Valid()) {
        break;
      }
      total_prev_found++;
      bytes += iter->key().size();
      bytes += iter->value().size();
    }

    delete iter;
    ASSERT_EQ(bytes, get_perf_context()->iter_read_bytes);
    SetPerfLevel(kDisable);
    total_bytes += bytes;
  };

  for (int i = 0; i < 10; i++) {
    threads.emplace_back(reader_func_next);
  }
  for (int i = 0; i < 15; i++) {
    threads.emplace_back(reader_func_prev);
  }

  for (auto& t : threads) {
    t.join();
  }

  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_NEXT), (uint64_t)total_next);
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_NEXT_FOUND),
            (uint64_t)total_next_found);
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_PREV), (uint64_t)total_prev);
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_PREV_FOUND),
            (uint64_t)total_prev_found);
  ASSERT_EQ(TestGetTickerCount(options, ITER_BYTES_READ), (uint64_t)total_bytes);

}

TEST_P(DBIteratorTest, ReadAhead) {
  Options options;
  env_->count_random_reads_ = true;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 4 << 20;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  table_options.no_block_cache = true;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);

  std::string value(1024, 'a');
  for (int i = 0; i < 100; i++) {
    Put(Key(i), value);
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  for (int i = 0; i < 100; i++) {
    Put(Key(i), value);
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  for (int i = 0; i < 100; i++) {
    Put(Key(i), value);
  }
  ASSERT_OK(Flush());
#ifndef ROCKSDB_LITE
  ASSERT_EQ("1,1,1", FilesPerLevel());
#endif  // !ROCKSDB_LITE

  env_->random_read_bytes_counter_ = 0;
  options.statistics->setTickerCount(NO_FILE_OPENS, 0);
  ReadOptions read_options;
  auto* iter = NewIterator(read_options);
  iter->SeekToFirst();
  int64_t num_file_opens = TestGetTickerCount(options, NO_FILE_OPENS);
  size_t bytes_read = env_->random_read_bytes_counter_;
  delete iter;

  int64_t num_file_closes = TestGetTickerCount(options, NO_FILE_CLOSES);
  env_->random_read_bytes_counter_ = 0;
  options.statistics->setTickerCount(NO_FILE_OPENS, 0);
  read_options.readahead_size = 1024 * 10;
  iter = NewIterator(read_options);
  iter->SeekToFirst();
  int64_t num_file_opens_readahead = TestGetTickerCount(options, NO_FILE_OPENS);
  size_t bytes_read_readahead = env_->random_read_bytes_counter_;
  delete iter;
  int64_t num_file_closes_readahead =
      TestGetTickerCount(options, NO_FILE_CLOSES);
  ASSERT_EQ(num_file_opens + 3, num_file_opens_readahead);
  ASSERT_EQ(num_file_closes + 3, num_file_closes_readahead);
  ASSERT_GT(bytes_read_readahead, bytes_read);
  ASSERT_GT(bytes_read_readahead, read_options.readahead_size * 3);

  // Verify correctness.
  iter = NewIterator(read_options);
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(value, iter->value());
    count++;
  }
  ASSERT_EQ(100, count);
  for (int i = 0; i < 100; i++) {
    iter->Seek(Key(i));
    ASSERT_EQ(value, iter->value());
  }
  delete iter;
}

// Insert a key, create a snapshot iterator, overwrite key lots of times,
// seek to a smaller key. Expect DBIter to fall back to a seek instead of
// going through all the overwrites linearly.
TEST_P(DBIteratorTest, DBIteratorSkipRecentDuplicatesTest) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_sequential_skip_in_iterations = 3;
  options.prefix_extractor = nullptr;
  options.write_buffer_size = 1 << 27;  // big enough to avoid flush
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(options);

  // Insert.
  ASSERT_OK(Put("b", "0"));

  // Create iterator.
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(NewIterator(ro));

  // Insert a lot.
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("b", std::to_string(i + 1).c_str()));
  }

#ifndef ROCKSDB_LITE
  // Check that memtable wasn't flushed.
  std::string val;
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &val));
  EXPECT_EQ("0", val);
#endif

  // Seek iterator to a smaller key.
  get_perf_context()->Reset();
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("b", iter->key().ToString());
  EXPECT_EQ("0", iter->value().ToString());

  // Check that the seek didn't do too much work.
  // Checks are not tight, just make sure that everything is well below 100.
  EXPECT_LT(get_perf_context()->internal_key_skipped_count, 4);
  EXPECT_LT(get_perf_context()->internal_recent_skipped_count, 8);
  EXPECT_LT(get_perf_context()->seek_on_memtable_count, 10);
  EXPECT_LT(get_perf_context()->next_on_memtable_count, 10);
  EXPECT_LT(get_perf_context()->prev_on_memtable_count, 10);

  // Check that iterator did something like what we expect.
  EXPECT_EQ(get_perf_context()->internal_delete_skipped_count, 0);
  EXPECT_EQ(get_perf_context()->internal_merge_count, 0);
  EXPECT_GE(get_perf_context()->internal_recent_skipped_count, 2);
  EXPECT_GE(get_perf_context()->seek_on_memtable_count, 2);
  EXPECT_EQ(1, options.statistics->getTickerCount(
                 NUMBER_OF_RESEEKS_IN_ITERATION));
}

TEST_P(DBIteratorTest, Refresh) {
  ASSERT_OK(Put("x", "y"));

  std::unique_ptr<Iterator> iter(NewIterator(ReadOptions()));
  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  ASSERT_OK(Put("c", "d"));

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  iter->Refresh();

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("c")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  dbfull()->Flush(FlushOptions());

  ASSERT_OK(Put("m", "n"));

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("c")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  iter->Refresh();

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("c")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("m")), 0);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  iter.reset();
}

TEST_P(DBIteratorTest, RefreshWithSnapshot) {
  ASSERT_OK(Put("x", "y"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ReadOptions options;
  options.snapshot = snapshot;
  Iterator* iter = NewIterator(options);

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  ASSERT_OK(Put("c", "d"));

  iter->Seek(Slice("a"));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("x")), 0);
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  Status s;
  s = iter->Refresh();
  ASSERT_TRUE(s.IsNotSupported());
  db_->ReleaseSnapshot(snapshot);
  delete iter;
}

TEST_P(DBIteratorTest, CreationFailure) {
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::NewInternalIterator:StatusCallback", [](void* arg) {
        *(reinterpret_cast<Status*>(arg)) = Status::Corruption("test status");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Iterator* iter = NewIterator(ReadOptions());
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;
}

TEST_P(DBIteratorTest, UpperBoundWithChangeDirection) {
  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 3;
  DestroyAndReopen(options);

  // write a bunch of kvs to the database.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("y", "1"));
  ASSERT_OK(Put("y1", "1"));
  ASSERT_OK(Put("y2", "1"));
  ASSERT_OK(Put("y3", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Put("bar", "1"));
  ASSERT_OK(Put("foo", "1"));

  std::string upper_bound = "x";
  Slice ub_slice(upper_bound);
  ReadOptions ro;
  ro.iterate_upper_bound = &ub_slice;
  ro.max_skippable_internal_keys = 1000;

  Iterator* iter = NewIterator(ro);
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bar", iter->key().ToString());

  delete iter;
}

TEST_P(DBIteratorTest, TableFilter) {
  ASSERT_OK(Put("a", "1"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Put("c", "3"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("d", "4"));
  ASSERT_OK(Put("e", "5"));
  ASSERT_OK(Put("f", "6"));
  dbfull()->Flush(FlushOptions());

  // Ensure the table_filter callback is called once for each table.
  {
    std::set<uint64_t> unseen{1, 2, 3};
    ReadOptions opts;
    opts.table_filter = [&](const TableProperties& props) {
      auto it = unseen.find(props.num_entries);
      if (it == unseen.end()) {
        ADD_FAILURE() << "saw table properties with an unexpected "
                      << props.num_entries << " entries";
      } else {
        unseen.erase(it);
      }
      return true;
    };
    auto iter = NewIterator(opts);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->1");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->2");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->3");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->4");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->5");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "f->6");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(unseen.empty());
    delete iter;
  }

  // Ensure returning false in the table_filter hides the keys from that table
  // during iteration.
  {
    ReadOptions opts;
    opts.table_filter = [](const TableProperties& props) {
      return props.num_entries != 2;
    };
    auto iter = NewIterator(opts);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->1");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->4");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->5");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "f->6");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    delete iter;
  }
}

TEST_P(DBIteratorTest, UpperBoundWithPrevReseek) {
  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 3;
  DestroyAndReopen(options);

  // write a bunch of kvs to the database.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("y", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("z", "1"));
  ASSERT_OK(Put("bar", "1"));
  ASSERT_OK(Put("foo", "1"));
  ASSERT_OK(Put("foo", "2"));

  ASSERT_OK(Put("foo", "3"));
  ASSERT_OK(Put("foo", "4"));
  ASSERT_OK(Put("foo", "5"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("foo", "6"));

  std::string upper_bound = "x";
  Slice ub_slice(upper_bound);
  ReadOptions ro;
  ro.snapshot = snapshot;
  ro.iterate_upper_bound = &ub_slice;

  Iterator* iter = NewIterator(ro);
  iter->SeekForPrev("goo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());
  iter->Prev();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar", iter->key().ToString());

  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_P(DBIteratorTest, SkipStatistics) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(options);

  int skip_count = 0;

  // write a bunch of kvs to the database.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("b", "1"));
  ASSERT_OK(Put("c", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("d", "1"));
  ASSERT_OK(Put("e", "1"));
  ASSERT_OK(Put("f", "1"));
  ASSERT_OK(Put("a", "2"));
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("d"));
  ASSERT_OK(Delete("e"));
  ASSERT_OK(Delete("f"));

  Iterator* iter = NewIterator(ReadOptions());
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_EQ(count, 3);
  delete iter;
  skip_count += 8; // 3 deletes + 3 original keys + 2 lower in sequence
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));

  iter = NewIterator(ReadOptions());
  count = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_EQ(count, 3);
  delete iter;
  skip_count += 8; // Same as above, but in reverse order
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));

  ASSERT_OK(Put("aa", "1"));
  ASSERT_OK(Put("ab", "1"));
  ASSERT_OK(Put("ac", "1"));
  ASSERT_OK(Put("ad", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("ab"));
  ASSERT_OK(Delete("ac"));
  ASSERT_OK(Delete("ad"));

  ReadOptions ro;
  Slice prefix("b");
  ro.iterate_upper_bound = &prefix;

  iter = NewIterator(ro);
  count = 0;
  for(iter->Seek("aa"); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_EQ(count, 1);
  delete iter;
  skip_count += 6; // 3 deletes + 3 original keys
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));

  iter = NewIterator(ro);
  count = 0;
  for(iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    ASSERT_OK(iter->status());
    count++;
  }
  ASSERT_EQ(count, 2);
  delete iter;
  // 3 deletes + 3 original keys + lower sequence of "a"
  skip_count += 7;
  ASSERT_EQ(skip_count, TestGetTickerCount(options, NUMBER_ITER_SKIP));
}

TEST_P(DBIteratorTest, SeekAfterHittingManyInternalKeys) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ReadOptions ropts;
  ropts.max_skippable_internal_keys = 2;

  Put("1", "val_1");
  // Add more tombstones than max_skippable_internal_keys so that Next() fails.
  Delete("2");
  Delete("3");
  Delete("4");
  Delete("5");
  Put("6", "val_6");

  std::unique_ptr<Iterator> iter(NewIterator(ropts));
  iter->SeekToFirst();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "1");
  ASSERT_EQ(iter->value().ToString(), "val_1");

  // This should fail as incomplete due to too many non-visible internal keys on
  // the way to the next valid user key.
  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_TRUE(iter->status().IsIncomplete());

  // Get the internal key at which Next() failed.
  std::string prop_value;
  ASSERT_OK(iter->GetProperty("rocksdb.iterator.internal-key", &prop_value));
  ASSERT_EQ("4", prop_value);

  // Create a new iterator to seek to the internal key.
  std::unique_ptr<Iterator> iter2(NewIterator(ropts));
  iter2->Seek(prop_value);
  ASSERT_TRUE(iter2->Valid());
  ASSERT_OK(iter2->status());

  ASSERT_EQ(iter2->key().ToString(), "6");
  ASSERT_EQ(iter2->value().ToString(), "val_6");
}

// Reproduces a former bug where iterator would skip some records when DBIter
// re-seeks subiterator with Incomplete status.
TEST_P(DBIteratorTest, NonBlockingIterationBugRepro) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  // Make sure the sst file has more than one block.
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  // Two records in sst file, each in its own block.
  Put("b", "");
  Put("d", "");
  Flush();

  // Create a nonblocking iterator before writing to memtable.
  ReadOptions ropt;
  ropt.read_tier = kBlockCacheTier;
  std::unique_ptr<Iterator> iter(NewIterator(ropt));

  // Overwrite a key in memtable many times to hit
  // max_sequential_skip_in_iterations (which is 8 by default).
  for (int i = 0; i < 20; ++i) {
    Put("c", "");
  }

  // Load the second block in sst file into the block cache.
  {
    std::unique_ptr<Iterator> iter2(NewIterator(ReadOptions()));
    iter2->Seek("d");
  }

  // Finally seek the nonblocking iterator.
  iter->Seek("a");
  // With the bug, the status used to be OK, and the iterator used to point to
  // "d".
  EXPECT_TRUE(iter->status().IsIncomplete());
}

TEST_P(DBIteratorTest, SeekBackwardAfterOutOfUpperBound) {
  Put("a", "");
  Put("b", "");
  Flush();

  ReadOptions ropt;
  Slice ub = "b";
  ropt.iterate_upper_bound = &ub;

  std::unique_ptr<Iterator> it(dbfull()->NewIterator(ropt));
  it->SeekForPrev("a");
  ASSERT_TRUE(it->Valid());
  ASSERT_OK(it->status());
  ASSERT_EQ("a", it->key().ToString());
  it->Next();
  ASSERT_FALSE(it->Valid());
  ASSERT_OK(it->status());
  it->SeekForPrev("a");
  ASSERT_OK(it->status());

  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("a", it->key().ToString());
}

INSTANTIATE_TEST_CASE_P(DBIteratorTestInstance, DBIteratorTest,
                        testing::Values(true, false));

// Tests how DBIter work with ReadCallback
class DBIteratorWithReadCallbackTest : public DBIteratorTest {};

TEST_F(DBIteratorWithReadCallbackTest, ReadCallback) {
  class TestReadCallback : public ReadCallback {
   public:
    explicit TestReadCallback(SequenceNumber last_visible_seq)
        : last_visible_seq_(last_visible_seq) {}

    bool IsVisible(SequenceNumber seq) override {
      return seq <= last_visible_seq_;
    }

   private:
    SequenceNumber last_visible_seq_;
  };

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("foo", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("z", "vz"));
  SequenceNumber seq1 = db_->GetLatestSequenceNumber();
  TestReadCallback callback1(seq1);
  ASSERT_OK(Put("foo", "v4"));
  ASSERT_OK(Put("foo", "v5"));
  ASSERT_OK(Put("bar", "v7"));

  SequenceNumber seq2 = db_->GetLatestSequenceNumber();
  auto* cfd =
      reinterpret_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
          ->cfd();
  // The iterator are suppose to see data before seq1.
  Iterator* iter =
      dbfull()->NewIteratorImpl(ReadOptions(), cfd, seq2, &callback1);

  // Seek
  // The latest value of "foo" before seq1 is "v3"
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key
  // "foo".
  iter->Seek("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());

  // Next
  // Seek to "a"
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("va", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key
  // "foo".
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());

  // Prev
  // Seek to "z"
  iter->Seek("z");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("vz", iter->value());
  // The previous key is "foo", which is visible to the iterator.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key "a".
  iter->Prev();  // skipping "bar"
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("a", iter->key());
  ASSERT_EQ("va", iter->value());

  // SeekForPrev
  // The previous key is "foo", which is visible to the iterator.
  iter->SeekForPrev("y");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v3", iter->value());
  // "bar" is not visible to the iterator. It will move on to the next key "a".
  iter->SeekForPrev("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("a", iter->key());
  ASSERT_EQ("va", iter->value());

  delete iter;

  // Prev beyond max_sequential_skip_in_iterations
  uint64_t num_versions =
      CurrentOptions().max_sequential_skip_in_iterations + 10;
  for (uint64_t i = 0; i < num_versions; i++) {
    ASSERT_OK(Put("bar", ToString(i)));
  }
  SequenceNumber seq3 = db_->GetLatestSequenceNumber();
  TestReadCallback callback2(seq3);
  ASSERT_OK(Put("bar", "v8"));
  SequenceNumber seq4 = db_->GetLatestSequenceNumber();

  // The iterator is suppose to see data before seq3.
  iter = dbfull()->NewIteratorImpl(ReadOptions(), cfd, seq4, &callback2);
  // Seek to "z", which is visible.
  iter->Seek("z");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("vz", iter->value());
  // Previous key is "foo" and the last value "v5" is visible.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("foo", iter->key());
  ASSERT_EQ("v5", iter->value());
  // Since the number of values of "bar" is more than
  // max_sequential_skip_in_iterations, Prev() will ultimately fallback to
  // seek in forward direction. Here we test the fallback seek is correct.
  // The last visible value should be (num_versions - 1), as "v8" is not
  // visible.
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("bar", iter->key());
  ASSERT_EQ(ToString(num_versions - 1), iter->value());

  delete iter;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
