//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction_iterator.h"

#include <string>
#include <vector>

#include "port/port.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

// Expects no merging attempts.
class NoMergingMergeOp : public MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                   MergeOperationOutput* /*merge_out*/) const override {
    ADD_FAILURE();
    return false;
  }
  bool PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& /*operand_list*/,
                         std::string* /*new_value*/,
                         Logger* /*logger*/) const override {
    ADD_FAILURE();
    return false;
  }
  const char* Name() const override {
    return "CompactionIteratorTest NoMergingMergeOp";
  }
};

// Compaction filter that gets stuck when it sees a particular key,
// then gets unstuck when told to.
// Always returns Decition::kRemove.
class StallingFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /*level*/, const Slice& key, ValueType /*type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    int k = std::atoi(key.ToString().c_str());
    last_seen.store(k);
    while (k >= stall_at.load()) {
      std::this_thread::yield();
    }
    return Decision::kRemove;
  }

  const char* Name() const override {
    return "CompactionIteratorTest StallingFilter";
  }

  // Wait until the filter sees a key >= k and stalls at that key.
  // If `exact`, asserts that the seen key is equal to k.
  void WaitForStall(int k, bool exact = true) {
    stall_at.store(k);
    while (last_seen.load() < k) {
      std::this_thread::yield();
    }
    if (exact) {
      EXPECT_EQ(k, last_seen.load());
    }
  }

  // Filter will stall on key >= stall_at. Advance stall_at to unstall.
  mutable std::atomic<int> stall_at{0};
  // Last key the filter was called with.
  mutable std::atomic<int> last_seen{0};
};

// Compaction filter that filter out all keys.
class FilterAllKeysCompactionFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /*level*/, const Slice& /*key*/, ValueType /*type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    return Decision::kRemove;
  }

  const char* Name() const override { return "AllKeysCompactionFilter"; }
};

class LoggingForwardVectorIterator : public InternalIterator {
 public:
  struct Action {
    enum class Type {
      SEEK_TO_FIRST,
      SEEK,
      NEXT,
    };

    Type type;
    std::string arg;

    explicit Action(Type _type, std::string _arg = "")
        : type(_type), arg(_arg) {}

    bool operator==(const Action& rhs) const {
      return std::tie(type, arg) == std::tie(rhs.type, rhs.arg);
    }
  };

  LoggingForwardVectorIterator(const std::vector<std::string>& keys,
                               const std::vector<std::string>& values)
      : keys_(keys), values_(values), current_(keys.size()) {
    assert(keys_.size() == values_.size());
  }

  bool Valid() const override { return current_ < keys_.size(); }

  void SeekToFirst() override {
    log.emplace_back(Action::Type::SEEK_TO_FIRST);
    current_ = 0;
  }
  void SeekToLast() override { assert(false); }

  void Seek(const Slice& target) override {
    log.emplace_back(Action::Type::SEEK, target.ToString());
    current_ = std::lower_bound(keys_.begin(), keys_.end(), target.ToString()) -
               keys_.begin();
  }

  void SeekForPrev(const Slice& /*target*/) override { assert(false); }

  void Next() override {
    assert(Valid());
    log.emplace_back(Action::Type::NEXT);
    current_++;
  }
  void Prev() override { assert(false); }

  Slice key() const override {
    assert(Valid());
    return Slice(keys_[current_]);
  }
  Slice value() const override {
    assert(Valid());
    return Slice(values_[current_]);
  }

  Status status() const override { return Status::OK(); }

  std::vector<Action> log;

 private:
  std::vector<std::string> keys_;
  std::vector<std::string> values_;
  size_t current_;
};

class FakeCompaction : public CompactionIterator::CompactionProxy {
 public:
  FakeCompaction() = default;

  int level(size_t /*compaction_input_level*/) const override { return 0; }
  bool KeyNotExistsBeyondOutputLevel(
      const Slice& /*user_key*/,
      std::vector<size_t>* /*level_ptrs*/) const override {
    return is_bottommost_level || key_not_exists_beyond_output_level;
  }
  bool bottommost_level() const override { return is_bottommost_level; }
  int number_levels() const override { return 1; }
  Slice GetLargestUserKey() const override {
    return "\xff\xff\xff\xff\xff\xff\xff\xff\xff";
  }
  bool allow_ingest_behind() const override { return false; }

  bool preserve_deletes() const override { return false; }

  bool key_not_exists_beyond_output_level = false;

  bool is_bottommost_level = false;
};

// A simplifed snapshot checker which assumes each snapshot has a global
// last visible sequence.
class TestSnapshotChecker : public SnapshotChecker {
 public:
  explicit TestSnapshotChecker(
      SequenceNumber last_committed_sequence,
      const std::unordered_map<SequenceNumber, SequenceNumber>& snapshots = {})
      : last_committed_sequence_(last_committed_sequence),
        snapshots_(snapshots) {}

  SnapshotCheckerResult CheckInSnapshot(
      SequenceNumber seq, SequenceNumber snapshot_seq) const override {
    if (snapshot_seq == kMaxSequenceNumber) {
      return seq <= last_committed_sequence_
                 ? SnapshotCheckerResult::kInSnapshot
                 : SnapshotCheckerResult::kNotInSnapshot;
    }
    assert(snapshots_.count(snapshot_seq) > 0);
    return seq <= snapshots_.at(snapshot_seq)
               ? SnapshotCheckerResult::kInSnapshot
               : SnapshotCheckerResult::kNotInSnapshot;
  }

 private:
  SequenceNumber last_committed_sequence_;
  // A map of valid snapshot to last visible sequence to the snapshot.
  std::unordered_map<SequenceNumber, SequenceNumber> snapshots_;
};

// Test param:
//   bool: whether to pass snapshot_checker to compaction iterator.
class CompactionIteratorTest : public testing::TestWithParam<bool> {
 public:
  CompactionIteratorTest()
      : cmp_(BytewiseComparator()), icmp_(cmp_), snapshots_({}) {}

  void InitIterators(
      const std::vector<std::string>& ks, const std::vector<std::string>& vs,
      const std::vector<std::string>& range_del_ks,
      const std::vector<std::string>& range_del_vs,
      SequenceNumber last_sequence,
      SequenceNumber last_committed_sequence = kMaxSequenceNumber,
      MergeOperator* merge_op = nullptr, CompactionFilter* filter = nullptr,
      bool bottommost_level = false,
      SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber) {
    std::unique_ptr<InternalIterator> unfragmented_range_del_iter(
        new test::VectorIterator(range_del_ks, range_del_vs));
    auto tombstone_list = std::make_shared<FragmentedRangeTombstoneList>(
        std::move(unfragmented_range_del_iter), icmp_);
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        new FragmentedRangeTombstoneIterator(tombstone_list, icmp_,
                                             kMaxSequenceNumber));
    range_del_agg_.reset(new CompactionRangeDelAggregator(&icmp_, snapshots_));
    range_del_agg_->AddTombstones(std::move(range_del_iter));

    std::unique_ptr<CompactionIterator::CompactionProxy> compaction;
    if (filter || bottommost_level) {
      compaction_proxy_ = new FakeCompaction();
      compaction_proxy_->is_bottommost_level = bottommost_level;
      compaction.reset(compaction_proxy_);
    }
    bool use_snapshot_checker = UseSnapshotChecker() || GetParam();
    if (use_snapshot_checker || last_committed_sequence < kMaxSequenceNumber) {
      snapshot_checker_.reset(
          new TestSnapshotChecker(last_committed_sequence, snapshot_map_));
    }
    merge_helper_.reset(
        new MergeHelper(Env::Default(), cmp_, merge_op, filter, nullptr, false,
                        0 /*latest_snapshot*/, snapshot_checker_.get(),
                        0 /*level*/, nullptr /*statistics*/, &shutting_down_));

    iter_.reset(new LoggingForwardVectorIterator(ks, vs));
    iter_->SeekToFirst();
    c_iter_.reset(new CompactionIterator(
        iter_.get(), cmp_, merge_helper_.get(), last_sequence, &snapshots_,
        earliest_write_conflict_snapshot, snapshot_checker_.get(),
        Env::Default(), false /* report_detailed_time */, false,
        range_del_agg_.get(), std::move(compaction), filter, &shutting_down_));
  }

  void AddSnapshot(SequenceNumber snapshot,
                   SequenceNumber last_visible_seq = kMaxSequenceNumber) {
    snapshots_.push_back(snapshot);
    snapshot_map_[snapshot] = last_visible_seq;
  }

  virtual bool UseSnapshotChecker() const { return false; }

  void RunTest(
      const std::vector<std::string>& input_keys,
      const std::vector<std::string>& input_values,
      const std::vector<std::string>& expected_keys,
      const std::vector<std::string>& expected_values,
      SequenceNumber last_committed_seq = kMaxSequenceNumber,
      MergeOperator* merge_operator = nullptr,
      CompactionFilter* compaction_filter = nullptr,
      bool bottommost_level = false,
      SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber) {
    InitIterators(input_keys, input_values, {}, {}, kMaxSequenceNumber,
                  last_committed_seq, merge_operator, compaction_filter,
                  bottommost_level, earliest_write_conflict_snapshot);
    c_iter_->SeekToFirst();
    for (size_t i = 0; i < expected_keys.size(); i++) {
      std::string info = "i = " + ToString(i);
      ASSERT_TRUE(c_iter_->Valid()) << info;
      ASSERT_OK(c_iter_->status()) << info;
      ASSERT_EQ(expected_keys[i], c_iter_->key().ToString()) << info;
      ASSERT_EQ(expected_values[i], c_iter_->value().ToString()) << info;
      c_iter_->Next();
    }
    ASSERT_FALSE(c_iter_->Valid());
  }

  const Comparator* cmp_;
  const InternalKeyComparator icmp_;
  std::vector<SequenceNumber> snapshots_;
  // A map of valid snapshot to last visible sequence to the snapshot.
  std::unordered_map<SequenceNumber, SequenceNumber> snapshot_map_;
  std::unique_ptr<MergeHelper> merge_helper_;
  std::unique_ptr<LoggingForwardVectorIterator> iter_;
  std::unique_ptr<CompactionIterator> c_iter_;
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg_;
  std::unique_ptr<SnapshotChecker> snapshot_checker_;
  std::atomic<bool> shutting_down_{false};
  FakeCompaction* compaction_proxy_;
};

// It is possible that the output of the compaction iterator is empty even if
// the input is not.
TEST_P(CompactionIteratorTest, EmptyResult) {
  InitIterators({test::KeyStr("a", 5, kTypeSingleDeletion),
                 test::KeyStr("a", 3, kTypeValue)},
                {"", "val"}, {}, {}, 5);
  c_iter_->SeekToFirst();
  ASSERT_FALSE(c_iter_->Valid());
}

// If there is a corruption after a single deletion, the corrupted key should
// be preserved.
TEST_P(CompactionIteratorTest, CorruptionAfterSingleDeletion) {
  InitIterators({test::KeyStr("a", 5, kTypeSingleDeletion),
                 test::KeyStr("a", 3, kTypeValue, true),
                 test::KeyStr("b", 10, kTypeValue)},
                {"", "val", "val2"}, {}, {}, 10);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 5, kTypeSingleDeletion),
            c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 3, kTypeValue, true), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("b", 10, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(CompactionIteratorTest, SimpleRangeDeletion) {
  InitIterators({test::KeyStr("morning", 5, kTypeValue),
                 test::KeyStr("morning", 2, kTypeValue),
                 test::KeyStr("night", 3, kTypeValue)},
                {"zao", "zao", "wan"},
                {test::KeyStr("ma", 4, kTypeRangeDeletion)}, {"mz"}, 5);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("morning", 5, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 3, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(CompactionIteratorTest, RangeDeletionWithSnapshots) {
  AddSnapshot(10);
  std::vector<std::string> ks1;
  ks1.push_back(test::KeyStr("ma", 28, kTypeRangeDeletion));
  std::vector<std::string> vs1{"mz"};
  std::vector<std::string> ks2{test::KeyStr("morning", 15, kTypeValue),
                               test::KeyStr("morning", 5, kTypeValue),
                               test::KeyStr("night", 40, kTypeValue),
                               test::KeyStr("night", 20, kTypeValue)};
  std::vector<std::string> vs2{"zao 15", "zao 5", "wan 40", "wan 20"};
  InitIterators(ks2, vs2, ks1, vs1, 40);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("morning", 5, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 40, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(CompactionIteratorTest, CompactionFilterSkipUntil) {
  class Filter : public CompactionFilter {
    Decision FilterV2(int /*level*/, const Slice& key, ValueType t,
                      const Slice& existing_value, std::string* /*new_value*/,
                      std::string* skip_until) const override {
      std::string k = key.ToString();
      std::string v = existing_value.ToString();
      // See InitIterators() call below for the sequence of keys and their
      // filtering decisions. Here we closely assert that compaction filter is
      // called with the expected keys and only them, and with the right values.
      if (k == "a") {
        EXPECT_EQ(ValueType::kValue, t);
        EXPECT_EQ("av50", v);
        return Decision::kKeep;
      }
      if (k == "b") {
        EXPECT_EQ(ValueType::kValue, t);
        EXPECT_EQ("bv60", v);
        *skip_until = "d+";
        return Decision::kRemoveAndSkipUntil;
      }
      if (k == "e") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        EXPECT_EQ("em71", v);
        return Decision::kKeep;
      }
      if (k == "f") {
        if (v == "fm65") {
          EXPECT_EQ(ValueType::kMergeOperand, t);
          *skip_until = "f";
        } else {
          EXPECT_EQ("fm30", v);
          EXPECT_EQ(ValueType::kMergeOperand, t);
          *skip_until = "g+";
        }
        return Decision::kRemoveAndSkipUntil;
      }
      if (k == "h") {
        EXPECT_EQ(ValueType::kValue, t);
        EXPECT_EQ("hv91", v);
        return Decision::kKeep;
      }
      if (k == "i") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        EXPECT_EQ("im95", v);
        *skip_until = "z";
        return Decision::kRemoveAndSkipUntil;
      }
      ADD_FAILURE();
      return Decision::kKeep;
    }

    const char* Name() const override {
      return "CompactionIteratorTest.CompactionFilterSkipUntil::Filter";
    }
  };

  NoMergingMergeOp merge_op;
  Filter filter;
  InitIterators(
      {test::KeyStr("a", 50, kTypeValue),  // keep
       test::KeyStr("a", 45, kTypeMerge),
       test::KeyStr("b", 60, kTypeValue),  // skip to "d+"
       test::KeyStr("b", 40, kTypeValue), test::KeyStr("c", 35, kTypeValue),
       test::KeyStr("d", 70, kTypeMerge),
       test::KeyStr("e", 71, kTypeMerge),  // keep
       test::KeyStr("f", 65, kTypeMerge),  // skip to "f", aka keep
       test::KeyStr("f", 30, kTypeMerge),  // skip to "g+"
       test::KeyStr("f", 25, kTypeValue), test::KeyStr("g", 90, kTypeValue),
       test::KeyStr("h", 91, kTypeValue),  // keep
       test::KeyStr("i", 95, kTypeMerge),  // skip to "z"
       test::KeyStr("j", 99, kTypeValue)},
      {"av50", "am45", "bv60", "bv40", "cv35", "dm70", "em71", "fm65", "fm30",
       "fv25", "gv90", "hv91", "im95", "jv99"},
      {}, {}, kMaxSequenceNumber, kMaxSequenceNumber, &merge_op, &filter);

  // Compaction should output just "a", "e" and "h" keys.
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 50, kTypeValue), c_iter_->key().ToString());
  ASSERT_EQ("av50", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("e", 71, kTypeMerge), c_iter_->key().ToString());
  ASSERT_EQ("em71", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("h", 91, kTypeValue), c_iter_->key().ToString());
  ASSERT_EQ("hv91", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());

  // Check that the compaction iterator did the correct sequence of calls on
  // the underlying iterator.
  using A = LoggingForwardVectorIterator::Action;
  using T = A::Type;
  std::vector<A> expected_actions = {
      A(T::SEEK_TO_FIRST),
      A(T::NEXT),
      A(T::NEXT),
      A(T::SEEK, test::KeyStr("d+", kMaxSequenceNumber, kValueTypeForSeek)),
      A(T::NEXT),
      A(T::NEXT),
      A(T::SEEK, test::KeyStr("g+", kMaxSequenceNumber, kValueTypeForSeek)),
      A(T::NEXT),
      A(T::SEEK, test::KeyStr("z", kMaxSequenceNumber, kValueTypeForSeek))};
  ASSERT_EQ(expected_actions, iter_->log);
}

TEST_P(CompactionIteratorTest, ShuttingDownInFilter) {
  NoMergingMergeOp merge_op;
  StallingFilter filter;
  InitIterators(
      {test::KeyStr("1", 1, kTypeValue), test::KeyStr("2", 2, kTypeValue),
       test::KeyStr("3", 3, kTypeValue), test::KeyStr("4", 4, kTypeValue)},
      {"v1", "v2", "v3", "v4"}, {}, {}, kMaxSequenceNumber, kMaxSequenceNumber,
      &merge_op, &filter);
  // Don't leave tombstones (kTypeDeletion) for filtered keys.
  compaction_proxy_->key_not_exists_beyond_output_level = true;

  std::atomic<bool> seek_done{false};
  rocksdb::port::Thread compaction_thread([&] {
    c_iter_->SeekToFirst();
    EXPECT_FALSE(c_iter_->Valid());
    EXPECT_TRUE(c_iter_->status().IsShutdownInProgress());
    seek_done.store(true);
  });

  // Let key 1 through.
  filter.WaitForStall(1);

  // Shutdown during compaction filter call for key 2.
  filter.WaitForStall(2);
  shutting_down_.store(true);
  EXPECT_FALSE(seek_done.load());

  // Unstall filter and wait for SeekToFirst() to return.
  filter.stall_at.store(3);
  compaction_thread.join();
  assert(seek_done.load());

  // Check that filter was never called again.
  EXPECT_EQ(2, filter.last_seen.load());
}

// Same as ShuttingDownInFilter, but shutdown happens during filter call for
// a merge operand, not for a value.
TEST_P(CompactionIteratorTest, ShuttingDownInMerge) {
  NoMergingMergeOp merge_op;
  StallingFilter filter;
  InitIterators(
      {test::KeyStr("1", 1, kTypeValue), test::KeyStr("2", 2, kTypeMerge),
       test::KeyStr("3", 3, kTypeMerge), test::KeyStr("4", 4, kTypeValue)},
      {"v1", "v2", "v3", "v4"}, {}, {}, kMaxSequenceNumber, kMaxSequenceNumber,
      &merge_op, &filter);
  compaction_proxy_->key_not_exists_beyond_output_level = true;

  std::atomic<bool> seek_done{false};
  rocksdb::port::Thread compaction_thread([&] {
    c_iter_->SeekToFirst();
    ASSERT_FALSE(c_iter_->Valid());
    ASSERT_TRUE(c_iter_->status().IsShutdownInProgress());
    seek_done.store(true);
  });

  // Let key 1 through.
  filter.WaitForStall(1);

  // Shutdown during compaction filter call for key 2.
  filter.WaitForStall(2);
  shutting_down_.store(true);
  EXPECT_FALSE(seek_done.load());

  // Unstall filter and wait for SeekToFirst() to return.
  filter.stall_at.store(3);
  compaction_thread.join();
  assert(seek_done.load());

  // Check that filter was never called again.
  EXPECT_EQ(2, filter.last_seen.load());
}

TEST_P(CompactionIteratorTest, SingleMergeOperand) {
  class Filter : public CompactionFilter {
    Decision FilterV2(int /*level*/, const Slice& key, ValueType t,
                      const Slice& existing_value, std::string* /*new_value*/,
                      std::string* /*skip_until*/) const override {
      std::string k = key.ToString();
      std::string v = existing_value.ToString();

      // See InitIterators() call below for the sequence of keys and their
      // filtering decisions. Here we closely assert that compaction filter is
      // called with the expected keys and only them, and with the right values.
      if (k == "a") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        EXPECT_EQ("av1", v);
        return Decision::kKeep;
      } else if (k == "b") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        return Decision::kKeep;
      } else if (k == "c") {
        return Decision::kKeep;
      }

      ADD_FAILURE();
      return Decision::kKeep;
    }

    const char* Name() const override {
      return "CompactionIteratorTest.SingleMergeOperand::Filter";
    }
  };

  class SingleMergeOp : public MergeOperator {
   public:
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override {
      // See InitIterators() call below for why "c" is the only key for which
      // FullMergeV2 should be called.
      EXPECT_EQ("c", merge_in.key.ToString());

      std::string temp_value;
      if (merge_in.existing_value != nullptr) {
        temp_value = merge_in.existing_value->ToString();
      }

      for (auto& operand : merge_in.operand_list) {
        temp_value.append(operand.ToString());
      }
      merge_out->new_value = temp_value;

      return true;
    }

    bool PartialMergeMulti(const Slice& key,
                           const std::deque<Slice>& operand_list,
                           std::string* new_value,
                           Logger* /*logger*/) const override {
      std::string string_key = key.ToString();
      EXPECT_TRUE(string_key == "a" || string_key == "b");

      if (string_key == "a") {
        EXPECT_EQ(1, operand_list.size());
      } else if (string_key == "b") {
        EXPECT_EQ(2, operand_list.size());
      }

      std::string temp_value;
      for (auto& operand : operand_list) {
        temp_value.append(operand.ToString());
      }
      swap(temp_value, *new_value);

      return true;
    }

    const char* Name() const override {
      return "CompactionIteratorTest SingleMergeOp";
    }

    bool AllowSingleOperand() const override { return true; }
  };

  SingleMergeOp merge_op;
  Filter filter;
  InitIterators(
      // a should invoke PartialMergeMulti with a single merge operand.
      {test::KeyStr("a", 50, kTypeMerge),
       // b should invoke PartialMergeMulti with two operands.
       test::KeyStr("b", 70, kTypeMerge), test::KeyStr("b", 60, kTypeMerge),
       // c should invoke FullMerge due to kTypeValue at the beginning.
       test::KeyStr("c", 90, kTypeMerge), test::KeyStr("c", 80, kTypeValue)},
      {"av1", "bv2", "bv1", "cv2", "cv1"}, {}, {}, kMaxSequenceNumber,
      kMaxSequenceNumber, &merge_op, &filter);

  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 50, kTypeMerge), c_iter_->key().ToString());
  ASSERT_EQ("av1", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ("bv1bv2", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_EQ("cv1cv2", c_iter_->value().ToString());
}

// In bottommost level, values earlier than earliest snapshot can be output
// with sequence = 0.
TEST_P(CompactionIteratorTest, ZeroOutSequenceAtBottomLevel) {
  AddSnapshot(1);
  RunTest({test::KeyStr("a", 1, kTypeValue), test::KeyStr("b", 2, kTypeValue)},
          {"v1", "v2"},
          {test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 2, kTypeValue)},
          {"v1", "v2"}, kMaxSequenceNumber /*last_commited_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

// In bottommost level, deletions earlier than earliest snapshot can be removed
// permanently.
TEST_P(CompactionIteratorTest, RemoveDeletionAtBottomLevel) {
  AddSnapshot(1);
  RunTest({test::KeyStr("a", 1, kTypeDeletion),
           test::KeyStr("b", 3, kTypeDeletion),
           test::KeyStr("b", 1, kTypeValue)},
          {"", "", ""},
          {test::KeyStr("b", 3, kTypeDeletion),
           test::KeyStr("b", 0, kTypeValue)},
          {"", ""},
          kMaxSequenceNumber /*last_commited_seq*/, nullptr /*merge_operator*/,
          nullptr /*compaction_filter*/, true /*bottommost_level*/);
}

// In bottommost level, single deletions earlier than earliest snapshot can be
// removed permanently.
TEST_P(CompactionIteratorTest, RemoveSingleDeletionAtBottomLevel) {
  AddSnapshot(1);
  RunTest({test::KeyStr("a", 1, kTypeSingleDeletion),
           test::KeyStr("b", 2, kTypeSingleDeletion)},
          {"", ""}, {test::KeyStr("b", 2, kTypeSingleDeletion)}, {""},
          kMaxSequenceNumber /*last_commited_seq*/, nullptr /*merge_operator*/,
          nullptr /*compaction_filter*/, true /*bottommost_level*/);
}

INSTANTIATE_TEST_CASE_P(CompactionIteratorTestInstance, CompactionIteratorTest,
                        testing::Values(true, false));

// Tests how CompactionIterator work together with SnapshotChecker.
class CompactionIteratorWithSnapshotCheckerTest
    : public CompactionIteratorTest {
 public:
  bool UseSnapshotChecker() const override { return true; }
};

// Uncommitted keys (keys with seq > last_committed_seq) should be output as-is
// while committed version of these keys should get compacted as usual.

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_Value) {
  RunTest(
      {test::KeyStr("foo", 3, kTypeValue), test::KeyStr("foo", 2, kTypeValue),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v3", "v2", "v1"},
      {test::KeyStr("foo", 3, kTypeValue), test::KeyStr("foo", 2, kTypeValue)},
      {"v3", "v2"}, 2 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_Deletion) {
  RunTest({test::KeyStr("foo", 2, kTypeDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("foo", 2, kTypeDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"}, 1 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_Merge) {
  auto merge_op = MergeOperators::CreateStringAppendOperator();
  RunTest(
      {test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 2, kTypeMerge),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v3", "v2", "v1"},
      {test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 2, kTypeValue)},
      {"v3", "v1,v2"}, 2 /*last_committed_seq*/, merge_op.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_SingleDelete) {
  RunTest({test::KeyStr("foo", 2, kTypeSingleDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("foo", 2, kTypeSingleDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"}, 1 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_BlobIndex) {
  RunTest({test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 2, kTypeBlobIndex),
           test::KeyStr("foo", 1, kTypeBlobIndex)},
          {"v3", "v2", "v1"},
          {test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 2, kTypeBlobIndex)},
          {"v3", "v2"}, 2 /*last_committed_seq*/);
}

// Test compaction iterator dedup keys visible to the same snapshot.

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_Value) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("foo", 4, kTypeValue), test::KeyStr("foo", 3, kTypeValue),
       test::KeyStr("foo", 2, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "v3", "v2", "v1"},
      {test::KeyStr("foo", 4, kTypeValue), test::KeyStr("foo", 3, kTypeValue),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "v3", "v1"}, 3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_Deletion) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("foo", 4, kTypeValue),
       test::KeyStr("foo", 3, kTypeDeletion),
       test::KeyStr("foo", 2, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "", "v2", "v1"},
      {test::KeyStr("foo", 4, kTypeValue),
       test::KeyStr("foo", 3, kTypeDeletion),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "", "v1"}, 3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_Merge) {
  AddSnapshot(2, 1);
  AddSnapshot(4, 3);
  auto merge_op = MergeOperators::CreateStringAppendOperator();
  RunTest(
      {test::KeyStr("foo", 5, kTypeMerge), test::KeyStr("foo", 4, kTypeMerge),
       test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 2, kTypeMerge),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v5", "v4", "v3", "v2", "v1"},
      {test::KeyStr("foo", 5, kTypeMerge), test::KeyStr("foo", 4, kTypeMerge),
       test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 1, kTypeValue)},
      {"v5", "v4", "v2,v3", "v1"}, 4 /*last_committed_seq*/, merge_op.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       DedupSameSnapshot_SingleDeletion) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("foo", 4, kTypeValue),
       test::KeyStr("foo", 3, kTypeSingleDeletion),
       test::KeyStr("foo", 2, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "", "v2", "v1"},
      {test::KeyStr("foo", 4, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "v1"}, 3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_BlobIndex) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("foo", 4, kTypeBlobIndex),
           test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 2, kTypeBlobIndex),
           test::KeyStr("foo", 1, kTypeBlobIndex)},
          {"v4", "v3", "v2", "v1"},
          {test::KeyStr("foo", 4, kTypeBlobIndex),
           test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 1, kTypeBlobIndex)},
          {"v4", "v3", "v1"}, 3 /*last_committed_seq*/);
}

// At bottom level, sequence numbers can be zero out, and deletions can be
// removed, but only when they are visible to earliest snapshot.

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotZeroOutSequenceIfNotVisibleToEarliestSnapshot) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 1, kTypeValue), test::KeyStr("b", 2, kTypeValue),
           test::KeyStr("c", 3, kTypeValue)},
          {"v1", "v2", "v3"},
          {test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 2, kTypeValue),
           test::KeyStr("c", 3, kTypeValue)},
          {"v1", "v2", "v3"}, kMaxSequenceNumber /*last_commited_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotRemoveDeletionIfNotVisibleToEarliestSnapshot) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("a", 1, kTypeDeletion), test::KeyStr("b", 2, kTypeDeletion),
       test::KeyStr("c", 3, kTypeDeletion)},
      {"", "", ""},
      {},
      {"", ""}, kMaxSequenceNumber /*last_commited_seq*/,
      nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
      true /*bottommost_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotRemoveDeletionIfValuePresentToEarlierSnapshot) {
  AddSnapshot(2,1);
  RunTest(
      {test::KeyStr("a", 4, kTypeDeletion), test::KeyStr("a", 1, kTypeValue),
          test::KeyStr("b", 3, kTypeValue)},
      {"", "", ""},
      {test::KeyStr("a", 4, kTypeDeletion), test::KeyStr("a", 0, kTypeValue),
            test::KeyStr("b", 3, kTypeValue)},
      {"", "", ""}, kMaxSequenceNumber /*last_commited_seq*/,
      nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
      true /*bottommost_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotRemoveSingleDeletionIfNotVisibleToEarliestSnapshot) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 1, kTypeSingleDeletion),
           test::KeyStr("b", 2, kTypeSingleDeletion),
           test::KeyStr("c", 3, kTypeSingleDeletion)},
          {"", "", ""},
          {test::KeyStr("b", 2, kTypeSingleDeletion),
           test::KeyStr("c", 3, kTypeSingleDeletion)},
          {"", ""}, kMaxSequenceNumber /*last_commited_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

// Single delete should not cancel out values that not visible to the
// same set of snapshots
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       SingleDeleteAcrossSnapshotBoundary) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", "v1"}, 2 /*last_committed_seq*/);
}

// Single delete should be kept in case it is not visible to the
// earliest write conflict snapshot. If a single delete is kept for this reason,
// corresponding value can be trimmed to save space.
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       KeepSingleDeletionForWriteConflictChecking) {
  AddSnapshot(2, 0);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", ""}, 2 /*last_committed_seq*/, nullptr /*merge_operator*/,
          nullptr /*compaction_filter*/, false /*bottommost_level*/,
          2 /*earliest_write_conflict_snapshot*/);
}

// Compaction filter should keep uncommitted key as-is, and
//   * Convert the latest velue to deletion, and/or
//   * if latest value is a merge, apply filter to all suequent merges.

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_Value) {
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest(
      {test::KeyStr("a", 2, kTypeValue), test::KeyStr("a", 1, kTypeValue),
       test::KeyStr("b", 3, kTypeValue), test::KeyStr("c", 1, kTypeValue)},
      {"v2", "v1", "v3", "v4"},
      {test::KeyStr("a", 2, kTypeValue), test::KeyStr("a", 1, kTypeDeletion),
       test::KeyStr("b", 3, kTypeValue), test::KeyStr("c", 1, kTypeDeletion)},
      {"v2", "", "v3", ""}, 1 /*last_committed_seq*/,
      nullptr /*merge_operator*/, compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_Deletion) {
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest(
      {test::KeyStr("a", 2, kTypeDeletion), test::KeyStr("a", 1, kTypeValue)},
      {"", "v1"},
      {test::KeyStr("a", 2, kTypeDeletion),
       test::KeyStr("a", 1, kTypeDeletion)},
      {"", ""}, 1 /*last_committed_seq*/, nullptr /*merge_operator*/,
      compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       CompactionFilter_PartialMerge) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest({test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 2, kTypeMerge),
           test::KeyStr("a", 1, kTypeMerge)},
          {"v3", "v2", "v1"}, {test::KeyStr("a", 3, kTypeMerge)}, {"v3"},
          2 /*last_committed_seq*/, merge_op.get(), compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_FullMerge) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest(
      {test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 2, kTypeMerge),
       test::KeyStr("a", 1, kTypeValue)},
      {"v3", "v2", "v1"},
      {test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 1, kTypeDeletion)},
      {"v3", ""}, 2 /*last_committed_seq*/, merge_op.get(),
      compaction_filter.get());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
