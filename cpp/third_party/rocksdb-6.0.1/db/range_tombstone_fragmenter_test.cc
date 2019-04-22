//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_fragmenter.h"

#include "db/db_test_util.h"
#include "rocksdb/comparator.h"
#include "util/testutil.h"

namespace rocksdb {

class RangeTombstoneFragmenterTest : public testing::Test {};

namespace {

static auto bytewise_icmp = InternalKeyComparator(BytewiseComparator());

std::unique_ptr<InternalIterator> MakeRangeDelIter(
    const std::vector<RangeTombstone>& range_dels) {
  std::vector<std::string> keys, values;
  for (const auto& range_del : range_dels) {
    auto key_and_value = range_del.Serialize();
    keys.push_back(key_and_value.first.Encode().ToString());
    values.push_back(key_and_value.second.ToString());
  }
  return std::unique_ptr<test::VectorIterator>(
      new test::VectorIterator(keys, values));
}

void CheckIterPosition(const RangeTombstone& tombstone,
                       const FragmentedRangeTombstoneIterator* iter) {
  // Test InternalIterator interface.
  EXPECT_EQ(tombstone.start_key_, ExtractUserKey(iter->key()));
  EXPECT_EQ(tombstone.end_key_, iter->value());
  EXPECT_EQ(tombstone.seq_, iter->seq());

  // Test FragmentedRangeTombstoneIterator interface.
  EXPECT_EQ(tombstone.start_key_, iter->start_key());
  EXPECT_EQ(tombstone.end_key_, iter->end_key());
  EXPECT_EQ(tombstone.seq_, GetInternalKeySeqno(iter->key()));
}

void VerifyFragmentedRangeDels(
    FragmentedRangeTombstoneIterator* iter,
    const std::vector<RangeTombstone>& expected_tombstones) {
  iter->SeekToFirst();
  for (size_t i = 0; i < expected_tombstones.size(); i++, iter->Next()) {
    ASSERT_TRUE(iter->Valid());
    CheckIterPosition(expected_tombstones[i], iter);
  }
  EXPECT_FALSE(iter->Valid());
}

void VerifyVisibleTombstones(
    FragmentedRangeTombstoneIterator* iter,
    const std::vector<RangeTombstone>& expected_tombstones) {
  iter->SeekToTopFirst();
  for (size_t i = 0; i < expected_tombstones.size(); i++, iter->TopNext()) {
    ASSERT_TRUE(iter->Valid());
    CheckIterPosition(expected_tombstones[i], iter);
  }
  EXPECT_FALSE(iter->Valid());
}

struct SeekTestCase {
  Slice seek_target;
  RangeTombstone expected_position;
  bool out_of_range;
};

void VerifySeek(FragmentedRangeTombstoneIterator* iter,
                const std::vector<SeekTestCase>& cases) {
  for (const auto& testcase : cases) {
    iter->Seek(testcase.seek_target);
    if (testcase.out_of_range) {
      ASSERT_FALSE(iter->Valid());
    } else {
      ASSERT_TRUE(iter->Valid());
      CheckIterPosition(testcase.expected_position, iter);
    }
  }
}

void VerifySeekForPrev(FragmentedRangeTombstoneIterator* iter,
                       const std::vector<SeekTestCase>& cases) {
  for (const auto& testcase : cases) {
    iter->SeekForPrev(testcase.seek_target);
    if (testcase.out_of_range) {
      ASSERT_FALSE(iter->Valid());
    } else {
      ASSERT_TRUE(iter->Valid());
      CheckIterPosition(testcase.expected_position, iter);
    }
  }
}

struct MaxCoveringTombstoneSeqnumTestCase {
  Slice user_key;
  SequenceNumber result;
};

void VerifyMaxCoveringTombstoneSeqnum(
    FragmentedRangeTombstoneIterator* iter,
    const std::vector<MaxCoveringTombstoneSeqnumTestCase>& cases) {
  for (const auto& testcase : cases) {
    EXPECT_EQ(testcase.result,
              iter->MaxCoveringTombstoneSeqnum(testcase.user_key));
  }
}

}  // anonymous namespace

TEST_F(RangeTombstoneFragmenterTest, NonOverlappingTombstones) {
  auto range_del_iter = MakeRangeDelIter({{"a", "b", 10}, {"c", "d", 5}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber);
  ASSERT_EQ(0, iter.lower_bound());
  ASSERT_EQ(kMaxSequenceNumber, iter.upper_bound());
  VerifyFragmentedRangeDels(&iter, {{"a", "b", 10}, {"c", "d", 5}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"", 0}, {"a", 10}, {"b", 0}, {"c", 5}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlappingTombstones) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10}, {"c", "g", 15}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber);
  ASSERT_EQ(0, iter.lower_bound());
  ASSERT_EQ(kMaxSequenceNumber, iter.upper_bound());
  VerifyFragmentedRangeDels(
      &iter, {{"a", "c", 10}, {"c", "e", 15}, {"c", "e", 10}, {"e", "g", 15}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"a", 10}, {"c", 15}, {"e", 15}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, ContiguousTombstones) {
  auto range_del_iter = MakeRangeDelIter(
      {{"a", "c", 10}, {"c", "e", 20}, {"c", "e", 5}, {"e", "g", 15}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber);
  ASSERT_EQ(0, iter.lower_bound());
  ASSERT_EQ(kMaxSequenceNumber, iter.upper_bound());
  VerifyFragmentedRangeDels(
      &iter, {{"a", "c", 10}, {"c", "e", 20}, {"c", "e", 5}, {"e", "g", 15}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"a", 10}, {"c", 20}, {"e", 15}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartAndEndKey) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "c", 10}, {"a", "c", 7}, {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber);
  ASSERT_EQ(0, iter.lower_bound());
  ASSERT_EQ(kMaxSequenceNumber, iter.upper_bound());
  VerifyFragmentedRangeDels(&iter,
                            {{"a", "c", 10}, {"a", "c", 7}, {"a", "c", 3}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, {{"a", 10}, {"b", 10}, {"c", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartKeyDifferentEndKeys) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"a", "g", 7}, {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber);
  ASSERT_EQ(0, iter.lower_bound());
  ASSERT_EQ(kMaxSequenceNumber, iter.upper_bound());
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 10},
                                    {"a", "c", 7},
                                    {"a", "c", 3},
                                    {"c", "e", 10},
                                    {"c", "e", 7},
                                    {"e", "g", 7}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"a", 10}, {"c", 10}, {"e", 7}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartKeyMixedEndKeys) {
  auto range_del_iter = MakeRangeDelIter({{"a", "c", 30},
                                          {"a", "g", 20},
                                          {"a", "e", 10},
                                          {"a", "g", 7},
                                          {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber);
  ASSERT_EQ(0, iter.lower_bound());
  ASSERT_EQ(kMaxSequenceNumber, iter.upper_bound());
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 30},
                                    {"a", "c", 20},
                                    {"a", "c", 10},
                                    {"a", "c", 7},
                                    {"a", "c", 3},
                                    {"c", "e", 20},
                                    {"c", "e", 10},
                                    {"c", "e", 7},
                                    {"e", "g", 20},
                                    {"e", "g", 7}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"a", 30}, {"c", 20}, {"e", 20}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlapAndRepeatedStartKey) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter1(&fragment_list, bytewise_icmp,
                                         kMaxSequenceNumber);
  FragmentedRangeTombstoneIterator iter2(&fragment_list, bytewise_icmp,
                                         9 /* upper_bound */);
  FragmentedRangeTombstoneIterator iter3(&fragment_list, bytewise_icmp,
                                         7 /* upper_bound */);
  FragmentedRangeTombstoneIterator iter4(&fragment_list, bytewise_icmp,
                                         5 /* upper_bound */);
  FragmentedRangeTombstoneIterator iter5(&fragment_list, bytewise_icmp,
                                         3 /* upper_bound */);
  for (auto* iter : {&iter1, &iter2, &iter3, &iter4, &iter5}) {
    VerifyFragmentedRangeDels(iter, {{"a", "c", 10},
                                     {"c", "e", 10},
                                     {"c", "e", 8},
                                     {"c", "e", 6},
                                     {"e", "g", 8},
                                     {"e", "g", 6},
                                     {"g", "i", 6},
                                     {"j", "l", 4},
                                     {"j", "l", 2},
                                     {"l", "n", 4}});
  }

  ASSERT_EQ(0, iter1.lower_bound());
  ASSERT_EQ(kMaxSequenceNumber, iter1.upper_bound());
  VerifyVisibleTombstones(&iter1, {{"a", "c", 10},
                                   {"c", "e", 10},
                                   {"e", "g", 8},
                                   {"g", "i", 6},
                                   {"j", "l", 4},
                                   {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter1, {{"a", 10}, {"c", 10}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});

  ASSERT_EQ(0, iter2.lower_bound());
  ASSERT_EQ(9, iter2.upper_bound());
  VerifyVisibleTombstones(&iter2, {{"c", "e", 8},
                                   {"e", "g", 8},
                                   {"g", "i", 6},
                                   {"j", "l", 4},
                                   {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter2, {{"a", 0}, {"c", 8}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});

  ASSERT_EQ(0, iter3.lower_bound());
  ASSERT_EQ(7, iter3.upper_bound());
  VerifyVisibleTombstones(&iter3, {{"c", "e", 6},
                                   {"e", "g", 6},
                                   {"g", "i", 6},
                                   {"j", "l", 4},
                                   {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter3, {{"a", 0}, {"c", 6}, {"e", 6}, {"i", 0}, {"j", 4}, {"m", 4}});

  ASSERT_EQ(0, iter4.lower_bound());
  ASSERT_EQ(5, iter4.upper_bound());
  VerifyVisibleTombstones(&iter4, {{"j", "l", 4}, {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter4, {{"a", 0}, {"c", 0}, {"e", 0}, {"i", 0}, {"j", 4}, {"m", 4}});

  ASSERT_EQ(0, iter5.lower_bound());
  ASSERT_EQ(3, iter5.upper_bound());
  VerifyVisibleTombstones(&iter5, {{"j", "l", 2}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter5, {{"a", 0}, {"c", 0}, {"e", 0}, {"i", 0}, {"j", 2}, {"m", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlapAndRepeatedStartKeyUnordered) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"j", "n", 4},
                                          {"c", "i", 6},
                                          {"c", "g", 8},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        9 /* upper_bound */);
  ASSERT_EQ(0, iter.lower_bound());
  ASSERT_EQ(9, iter.upper_bound());
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 10},
                                    {"c", "e", 10},
                                    {"c", "e", 8},
                                    {"c", "e", 6},
                                    {"e", "g", 8},
                                    {"e", "g", 6},
                                    {"g", "i", 6},
                                    {"j", "l", 4},
                                    {"j", "l", 2},
                                    {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter, {{"a", 0}, {"c", 8}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlapAndRepeatedStartKeyForCompaction) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"j", "n", 4},
                                          {"c", "i", 6},
                                          {"c", "g", 8},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* for_compaction */,
      {} /* snapshots */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber /* upper_bound */);
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 10},
                                    {"c", "e", 10},
                                    {"e", "g", 8},
                                    {"g", "i", 6},
                                    {"j", "l", 4},
                                    {"l", "n", 4}});
}

TEST_F(RangeTombstoneFragmenterTest,
       OverlapAndRepeatedStartKeyForCompactionWithSnapshot) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"j", "n", 4},
                                          {"c", "i", 6},
                                          {"c", "g", 8},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* for_compaction */,
      {20, 9} /* upper_bounds */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber /* upper_bound */);
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 10},
                                    {"c", "e", 10},
                                    {"c", "e", 8},
                                    {"e", "g", 8},
                                    {"g", "i", 6},
                                    {"j", "l", 4},
                                    {"l", "n", 4}});
}

TEST_F(RangeTombstoneFragmenterTest, IteratorSplitNoSnapshots) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"j", "n", 4},
                                          {"c", "i", 6},
                                          {"c", "g", 8},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber /* upper_bound */);

  auto split_iters = iter.SplitBySnapshot({} /* snapshots */);
  ASSERT_EQ(1, split_iters.size());

  auto* split_iter = split_iters[kMaxSequenceNumber].get();
  ASSERT_EQ(0, split_iter->lower_bound());
  ASSERT_EQ(kMaxSequenceNumber, split_iter->upper_bound());
  VerifyVisibleTombstones(split_iter, {{"a", "c", 10},
                                       {"c", "e", 10},
                                       {"e", "g", 8},
                                       {"g", "i", 6},
                                       {"j", "l", 4},
                                       {"l", "n", 4}});
}

TEST_F(RangeTombstoneFragmenterTest, IteratorSplitWithSnapshots) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"j", "n", 4},
                                          {"c", "i", 6},
                                          {"c", "g", 8},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber /* upper_bound */);

  auto split_iters = iter.SplitBySnapshot({3, 5, 7, 9} /* snapshots */);
  ASSERT_EQ(5, split_iters.size());

  auto* split_iter1 = split_iters[3].get();
  ASSERT_EQ(0, split_iter1->lower_bound());
  ASSERT_EQ(3, split_iter1->upper_bound());
  VerifyVisibleTombstones(split_iter1, {{"j", "l", 2}});

  auto* split_iter2 = split_iters[5].get();
  ASSERT_EQ(4, split_iter2->lower_bound());
  ASSERT_EQ(5, split_iter2->upper_bound());
  VerifyVisibleTombstones(split_iter2, {{"j", "l", 4}, {"l", "n", 4}});

  auto* split_iter3 = split_iters[7].get();
  ASSERT_EQ(6, split_iter3->lower_bound());
  ASSERT_EQ(7, split_iter3->upper_bound());
  VerifyVisibleTombstones(split_iter3,
                          {{"c", "e", 6}, {"e", "g", 6}, {"g", "i", 6}});

  auto* split_iter4 = split_iters[9].get();
  ASSERT_EQ(8, split_iter4->lower_bound());
  ASSERT_EQ(9, split_iter4->upper_bound());
  VerifyVisibleTombstones(split_iter4, {{"c", "e", 8}, {"e", "g", 8}});

  auto* split_iter5 = split_iters[kMaxSequenceNumber].get();
  ASSERT_EQ(10, split_iter5->lower_bound());
  ASSERT_EQ(kMaxSequenceNumber, split_iter5->upper_bound());
  VerifyVisibleTombstones(split_iter5, {{"a", "c", 10}, {"c", "e", 10}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekStartKey) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);

  FragmentedRangeTombstoneIterator iter1(&fragment_list, bytewise_icmp,
                                         kMaxSequenceNumber);
  VerifySeek(
      &iter1,
      {{"a", {"a", "c", 10}}, {"e", {"e", "g", 8}}, {"l", {"l", "n", 4}}});
  VerifySeekForPrev(
      &iter1,
      {{"a", {"a", "c", 10}}, {"e", {"e", "g", 8}}, {"l", {"l", "n", 4}}});

  FragmentedRangeTombstoneIterator iter2(&fragment_list, bytewise_icmp,
                                         3 /* upper_bound */);
  VerifySeek(&iter2, {{"a", {"j", "l", 2}},
                      {"e", {"j", "l", 2}},
                      {"l", {}, true /* out of range */}});
  VerifySeekForPrev(&iter2, {{"a", {}, true /* out of range */},
                             {"e", {}, true /* out of range */},
                             {"l", {"j", "l", 2}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekCovered) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);

  FragmentedRangeTombstoneIterator iter1(&fragment_list, bytewise_icmp,
                                         kMaxSequenceNumber);
  VerifySeek(
      &iter1,
      {{"b", {"a", "c", 10}}, {"f", {"e", "g", 8}}, {"m", {"l", "n", 4}}});
  VerifySeekForPrev(
      &iter1,
      {{"b", {"a", "c", 10}}, {"f", {"e", "g", 8}}, {"m", {"l", "n", 4}}});

  FragmentedRangeTombstoneIterator iter2(&fragment_list, bytewise_icmp,
                                         3 /* upper_bound */);
  VerifySeek(&iter2, {{"b", {"j", "l", 2}},
                      {"f", {"j", "l", 2}},
                      {"m", {}, true /* out of range */}});
  VerifySeekForPrev(&iter2, {{"b", {}, true /* out of range */},
                             {"f", {}, true /* out of range */},
                             {"m", {"j", "l", 2}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekEndKey) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);

  FragmentedRangeTombstoneIterator iter1(&fragment_list, bytewise_icmp,
                                         kMaxSequenceNumber);
  VerifySeek(&iter1, {{"c", {"c", "e", 10}},
                      {"g", {"g", "i", 6}},
                      {"i", {"j", "l", 4}},
                      {"n", {}, true /* out of range */}});
  VerifySeekForPrev(&iter1, {{"c", {"c", "e", 10}},
                             {"g", {"g", "i", 6}},
                             {"i", {"g", "i", 6}},
                             {"n", {"l", "n", 4}}});

  FragmentedRangeTombstoneIterator iter2(&fragment_list, bytewise_icmp,
                                         3 /* upper_bound */);
  VerifySeek(&iter2, {{"c", {"j", "l", 2}},
                      {"g", {"j", "l", 2}},
                      {"i", {"j", "l", 2}},
                      {"n", {}, true /* out of range */}});
  VerifySeekForPrev(&iter2, {{"c", {}, true /* out of range */},
                             {"g", {}, true /* out of range */},
                             {"i", {}, true /* out of range */},
                             {"n", {"j", "l", 2}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekOutOfBounds) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);

  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp,
                                        kMaxSequenceNumber);
  VerifySeek(&iter, {{"", {"a", "c", 10}}, {"z", {}, true /* out of range */}});
  VerifySeekForPrev(&iter,
                    {{"", {}, true /* out of range */}, {"z", {"l", "n", 4}}});
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
