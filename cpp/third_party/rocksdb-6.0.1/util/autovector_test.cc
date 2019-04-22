//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <iostream>
#include <string>
#include <utility>

#include "rocksdb/env.h"
#include "util/autovector.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

using std::cout;
using std::endl;

namespace rocksdb {

class AutoVectorTest : public testing::Test {};
const unsigned long kSize = 8;

namespace {
template <class T>
void AssertAutoVectorOnlyInStack(autovector<T, kSize>* vec, bool result) {
#ifndef ROCKSDB_LITE
  ASSERT_EQ(vec->only_in_stack(), result);
#else
  (void) vec;
  (void) result;
#endif  // !ROCKSDB_LITE
}
}  // namespace

TEST_F(AutoVectorTest, PushBackAndPopBack) {
  autovector<size_t, kSize> vec;
  ASSERT_TRUE(vec.empty());
  ASSERT_EQ(0ul, vec.size());

  for (size_t i = 0; i < 1000 * kSize; ++i) {
    vec.push_back(i);
    ASSERT_TRUE(!vec.empty());
    if (i < kSize) {
      AssertAutoVectorOnlyInStack(&vec, true);
    } else {
      AssertAutoVectorOnlyInStack(&vec, false);
    }
    ASSERT_EQ(i + 1, vec.size());
    ASSERT_EQ(i, vec[i]);
    ASSERT_EQ(i, vec.at(i));
  }

  size_t size = vec.size();
  while (size != 0) {
    vec.pop_back();
    // will always be in heap
    AssertAutoVectorOnlyInStack(&vec, false);
    ASSERT_EQ(--size, vec.size());
  }

  ASSERT_TRUE(vec.empty());
}

TEST_F(AutoVectorTest, EmplaceBack) {
  typedef std::pair<size_t, std::string> ValType;
  autovector<ValType, kSize> vec;

  for (size_t i = 0; i < 1000 * kSize; ++i) {
    vec.emplace_back(i, ToString(i + 123));
    ASSERT_TRUE(!vec.empty());
    if (i < kSize) {
      AssertAutoVectorOnlyInStack(&vec, true);
    } else {
      AssertAutoVectorOnlyInStack(&vec, false);
    }

    ASSERT_EQ(i + 1, vec.size());
    ASSERT_EQ(i, vec[i].first);
    ASSERT_EQ(ToString(i + 123), vec[i].second);
  }

  vec.clear();
  ASSERT_TRUE(vec.empty());
  AssertAutoVectorOnlyInStack(&vec, false);
}

TEST_F(AutoVectorTest, Resize) {
  autovector<size_t, kSize> vec;

  vec.resize(kSize);
  AssertAutoVectorOnlyInStack(&vec, true);
  for (size_t i = 0; i < kSize; ++i) {
    vec[i] = i;
  }

  vec.resize(kSize * 2);
  AssertAutoVectorOnlyInStack(&vec, false);
  for (size_t i = 0; i < kSize; ++i) {
    ASSERT_EQ(vec[i], i);
  }
  for (size_t i = 0; i < kSize; ++i) {
    vec[i + kSize] = i;
  }

  vec.resize(1);
  ASSERT_EQ(1U, vec.size());
}

namespace {
void AssertEqual(
    const autovector<size_t, kSize>& a, const autovector<size_t, kSize>& b) {
  ASSERT_EQ(a.size(), b.size());
  ASSERT_EQ(a.empty(), b.empty());
#ifndef ROCKSDB_LITE
  ASSERT_EQ(a.only_in_stack(), b.only_in_stack());
#endif  // !ROCKSDB_LITE
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], b[i]);
  }
}
}  // namespace

TEST_F(AutoVectorTest, CopyAndAssignment) {
  // Test both heap-allocated and stack-allocated cases.
  for (auto size : { kSize / 2, kSize * 1000 }) {
    autovector<size_t, kSize> vec;
    for (size_t i = 0; i < size; ++i) {
      vec.push_back(i);
    }

    {
      autovector<size_t, kSize> other;
      other = vec;
      AssertEqual(other, vec);
    }

    {
      autovector<size_t, kSize> other(vec);
      AssertEqual(other, vec);
    }
  }
}

TEST_F(AutoVectorTest, Iterators) {
  autovector<std::string, kSize> vec;
  for (size_t i = 0; i < kSize * 1000; ++i) {
    vec.push_back(ToString(i));
  }

  // basic operator test
  ASSERT_EQ(vec.front(), *vec.begin());
  ASSERT_EQ(vec.back(), *(vec.end() - 1));
  ASSERT_TRUE(vec.begin() < vec.end());

  // non-const iterator
  size_t index = 0;
  for (const auto& item : vec) {
    ASSERT_EQ(vec[index++], item);
  }

  index = vec.size() - 1;
  for (auto pos = vec.rbegin(); pos != vec.rend(); ++pos) {
    ASSERT_EQ(vec[index--], *pos);
  }

  // const iterator
  const auto& cvec = vec;
  index = 0;
  for (const auto& item : cvec) {
    ASSERT_EQ(cvec[index++], item);
  }

  index = vec.size() - 1;
  for (auto pos = cvec.rbegin(); pos != cvec.rend(); ++pos) {
    ASSERT_EQ(cvec[index--], *pos);
  }

  // forward and backward
  auto pos = vec.begin();
  while (pos != vec.end()) {
    auto old_val = *pos;
    auto old = pos++;
    // HACK: make sure -> works
    ASSERT_TRUE(!old->empty());
    ASSERT_EQ(old_val, *old);
    ASSERT_TRUE(pos == vec.end() || old_val != *pos);
  }

  pos = vec.begin();
  for (size_t i = 0; i < vec.size(); i += 2) {
    // Cannot use ASSERT_EQ since that macro depends on iostream serialization
    ASSERT_TRUE(pos + 2 - 2 == pos);
    pos += 2;
    ASSERT_TRUE(pos >= vec.begin());
    ASSERT_TRUE(pos <= vec.end());

    size_t diff = static_cast<size_t>(pos - vec.begin());
    ASSERT_EQ(i + 2, diff);
  }
}

namespace {
std::vector<std::string> GetTestKeys(size_t size) {
  std::vector<std::string> keys;
  keys.resize(size);

  int index = 0;
  for (auto& key : keys) {
    key = "item-" + rocksdb::ToString(index++);
  }
  return keys;
}
}  // namespace

template <class TVector>
void BenchmarkVectorCreationAndInsertion(
    std::string name, size_t ops, size_t item_size,
    const std::vector<typename TVector::value_type>& items) {
  auto env = Env::Default();

  int index = 0;
  auto start_time = env->NowNanos();
  auto ops_remaining = ops;
  while(ops_remaining--) {
    TVector v;
    for (size_t i = 0; i < item_size; ++i) {
      v.push_back(items[index++]);
    }
  }
  auto elapsed = env->NowNanos() - start_time;
  cout << "created " << ops << " " << name << " instances:\n\t"
       << "each was inserted with " << item_size << " elements\n\t"
       << "total time elapsed: " << elapsed << " (ns)" << endl;
}

template <class TVector>
size_t BenchmarkSequenceAccess(std::string name, size_t ops, size_t elem_size) {
  TVector v;
  for (const auto& item : GetTestKeys(elem_size)) {
    v.push_back(item);
  }
  auto env = Env::Default();

  auto ops_remaining = ops;
  auto start_time = env->NowNanos();
  size_t total = 0;
  while (ops_remaining--) {
    auto end = v.end();
    for (auto pos = v.begin(); pos != end; ++pos) {
      total += pos->size();
    }
  }
  auto elapsed = env->NowNanos() - start_time;
  cout << "performed " << ops << " sequence access against " << name << "\n\t"
       << "size: " << elem_size << "\n\t"
       << "total time elapsed: " << elapsed << " (ns)" << endl;
  // HACK avoid compiler's optimization to ignore total
  return total;
}

// This test case only reports the performance between std::vector<std::string>
// and autovector<std::string>. We chose string for comparison because in most
// of our use cases we used std::vector<std::string>.
TEST_F(AutoVectorTest, PerfBench) {
  // We run same operations for kOps times in order to get a more fair result.
  size_t kOps = 100000;

  // Creation and insertion test
  // Test the case when there is:
  //  * no element inserted: internal array of std::vector may not really get
  //    initialize.
  //  * one element inserted: internal array of std::vector must have
  //    initialized.
  //  * kSize elements inserted. This shows the most time we'll spend if we
  //    keep everything in stack.
  //  * 2 * kSize elements inserted. The internal vector of
  //    autovector must have been initialized.
  cout << "=====================================================" << endl;
  cout << "Creation and Insertion Test (value type: std::string)" << endl;
  cout << "=====================================================" << endl;

  // pre-generated unique keys
  auto string_keys = GetTestKeys(kOps * 2 * kSize);
  for (auto insertions : { 0ul, 1ul, kSize / 2, kSize, 2 * kSize }) {
    BenchmarkVectorCreationAndInsertion<std::vector<std::string>>(
        "std::vector<std::string>", kOps, insertions, string_keys);
    BenchmarkVectorCreationAndInsertion<autovector<std::string, kSize>>(
        "autovector<std::string>", kOps, insertions, string_keys);
    cout << "-----------------------------------" << endl;
  }

  cout << "=====================================================" << endl;
  cout << "Creation and Insertion Test (value type: uint64_t)" << endl;
  cout << "=====================================================" << endl;

  // pre-generated unique keys
  std::vector<uint64_t> int_keys(kOps * 2 * kSize);
  for (size_t i = 0; i < kOps * 2 * kSize; ++i) {
    int_keys[i] = i;
  }
  for (auto insertions : { 0ul, 1ul, kSize / 2, kSize, 2 * kSize }) {
    BenchmarkVectorCreationAndInsertion<std::vector<uint64_t>>(
        "std::vector<uint64_t>", kOps, insertions, int_keys);
    BenchmarkVectorCreationAndInsertion<autovector<uint64_t, kSize>>(
      "autovector<uint64_t>", kOps, insertions, int_keys
    );
    cout << "-----------------------------------" << endl;
  }

  // Sequence Access Test
  cout << "=====================================================" << endl;
  cout << "Sequence Access Test" << endl;
  cout << "=====================================================" << endl;
  for (auto elem_size : { kSize / 2, kSize, 2 * kSize }) {
    BenchmarkSequenceAccess<std::vector<std::string>>("std::vector", kOps,
                                                      elem_size);
    BenchmarkSequenceAccess<autovector<std::string, kSize>>("autovector", kOps,
                                                            elem_size);
    cout << "-----------------------------------" << endl;
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
