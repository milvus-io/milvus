//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_iter.h"
#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "utilities/merge_operators.h"

#ifdef GFLAGS

#include "util/gflags_compat.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_bool(verbose, false,
            "Print huge, detailed trace. Intended for debugging failures.");

#else

void ParseCommandLineFlags(int*, char***, bool) {}
bool FLAGS_verbose = false;

#endif

namespace rocksdb {

class DBIteratorStressTest : public testing::Test {
 public:
  Env* env_;

  DBIteratorStressTest() : env_(Env::Default()) {}
};

namespace {

struct Entry {
  std::string key;
  ValueType type;  // kTypeValue, kTypeDeletion, kTypeMerge
  uint64_t sequence;
  std::string ikey;  // internal key, made from `key`, `sequence` and `type`
  std::string value;
  // If false, we'll pretend that this entry doesn't exist.
  bool visible = true;

  bool operator<(const Entry& e) const {
    if (key != e.key) return key < e.key;
    return std::tie(sequence, type) > std::tie(e.sequence, e.type);
  }
};

struct Data {
  std::vector<Entry> entries;

  // Indices in `entries` with `visible` = false.
  std::vector<size_t> hidden;
  // Keys of entries whose `visible` changed since the last seek of iterators.
  std::set<std::string> recently_touched_keys;
};

struct StressTestIterator : public InternalIterator {
  Data* data;
  Random64* rnd;
  InternalKeyComparator cmp;

  // Each operation will return error with this probability...
  double error_probability = 0;
  // ... and add/remove entries with this probability.
  double mutation_probability = 0;
  // The probability of adding vs removing entries will be chosen so that the
  // amount of removed entries stays somewhat close to this number.
  double target_hidden_fraction = 0;
  // If true, print all mutations to stdout for debugging.
  bool trace = false;

  int iter = -1;
  Status status_;

  StressTestIterator(Data* _data, Random64* _rnd, const Comparator* _cmp)
      : data(_data), rnd(_rnd), cmp(_cmp) {}

  bool Valid() const override {
    if (iter >= 0 && iter < (int)data->entries.size()) {
      assert(status_.ok());
      return true;
    }
    return false;
  }

  Status status() const override { return status_; }

  bool MaybeFail() {
    if (rnd->Next() >=
        std::numeric_limits<uint64_t>::max() * error_probability) {
      return false;
    }
    if (rnd->Next() % 2) {
      status_ = Status::Incomplete("test");
    } else {
      status_ = Status::IOError("test");
    }
    if (trace) {
      std::cout << "injecting " << status_.ToString() << std::endl;
    }
    iter = -1;
    return true;
  }

  void MaybeMutate() {
    if (rnd->Next() >=
        std::numeric_limits<uint64_t>::max() * mutation_probability) {
      return;
    }
    do {
      // If too many entries are hidden, hide less, otherwise hide more.
      double hide_probability =
          data->hidden.size() > data->entries.size() * target_hidden_fraction
              ? 1. / 3
              : 2. / 3;
      if (data->hidden.empty()) {
        hide_probability = 1;
      }
      bool do_hide =
          rnd->Next() < std::numeric_limits<uint64_t>::max() * hide_probability;
      if (do_hide) {
        // Hide a random entry.
        size_t idx = rnd->Next() % data->entries.size();
        Entry& e = data->entries[idx];
        if (e.visible) {
          if (trace) {
            std::cout << "hiding idx " << idx << std::endl;
          }
          e.visible = false;
          data->hidden.push_back(idx);
          data->recently_touched_keys.insert(e.key);
        } else {
          // Already hidden. Let's go unhide something instead, just because
          // it's easy and it doesn't really matter what we do.
          do_hide = false;
        }
      }
      if (!do_hide) {
        // Unhide a random entry.
        size_t hi = rnd->Next() % data->hidden.size();
        size_t idx = data->hidden[hi];
        if (trace) {
          std::cout << "unhiding idx " << idx << std::endl;
        }
        Entry& e = data->entries[idx];
        assert(!e.visible);
        e.visible = true;
        data->hidden[hi] = data->hidden.back();
        data->hidden.pop_back();
        data->recently_touched_keys.insert(e.key);
      }
    } while (rnd->Next() % 3 != 0);  // do 3 mutations on average
  }

  void SkipForward() {
    while (iter < (int)data->entries.size() && !data->entries[iter].visible) {
      ++iter;
    }
  }
  void SkipBackward() {
    while (iter >= 0 && !data->entries[iter].visible) {
      --iter;
    }
  }

  void SeekToFirst() override {
    if (MaybeFail()) return;
    MaybeMutate();

    status_ = Status::OK();
    iter = 0;
    SkipForward();
  }
  void SeekToLast() override {
    if (MaybeFail()) return;
    MaybeMutate();

    status_ = Status::OK();
    iter = (int)data->entries.size() - 1;
    SkipBackward();
  }

  void Seek(const Slice& target) override {
    if (MaybeFail()) return;
    MaybeMutate();

    status_ = Status::OK();
    // Binary search.
    auto it = std::partition_point(
        data->entries.begin(), data->entries.end(),
        [&](const Entry& e) { return cmp.Compare(e.ikey, target) < 0; });
    iter = (int)(it - data->entries.begin());
    SkipForward();
  }
  void SeekForPrev(const Slice& target) override {
    if (MaybeFail()) return;
    MaybeMutate();

    status_ = Status::OK();
    // Binary search.
    auto it = std::partition_point(
        data->entries.begin(), data->entries.end(),
        [&](const Entry& e) { return cmp.Compare(e.ikey, target) <= 0; });
    iter = (int)(it - data->entries.begin());
    --iter;
    SkipBackward();
  }

  void Next() override {
    assert(Valid());
    if (MaybeFail()) return;
    MaybeMutate();
    ++iter;
    SkipForward();
  }
  void Prev() override {
    assert(Valid());
    if (MaybeFail()) return;
    MaybeMutate();
    --iter;
    SkipBackward();
  }

  Slice key() const override {
    assert(Valid());
    return data->entries[iter].ikey;
  }
  Slice value() const override {
    assert(Valid());
    return data->entries[iter].value;
  }

  bool IsKeyPinned() const override { return true; }
  bool IsValuePinned() const override { return true; }
};

// A small reimplementation of DBIter, supporting only some of the features,
// and doing everything in O(log n).
// Skips all keys that are in recently_touched_keys.
struct ReferenceIterator {
  Data* data;
  uint64_t sequence;  // ignore entries with sequence number below this

  bool valid = false;
  std::string key;
  std::string value;

  ReferenceIterator(Data* _data, uint64_t _sequence)
      : data(_data), sequence(_sequence) {}

  bool Valid() const { return valid; }

  // Finds the first entry with key
  // greater/less/greater-or-equal/less-or-equal than `key`, depending on
  // arguments: if `skip`, inequality is strict; if `forward`, it's
  // greater/greater-or-equal, otherwise less/less-or-equal.
  // Sets `key` to the result.
  // If no such key exists, returns false. Doesn't check `visible`.
  bool FindNextKey(bool skip, bool forward) {
    valid = false;
    auto it = std::partition_point(data->entries.begin(), data->entries.end(),
                                   [&](const Entry& e) {
                                     if (forward != skip) {
                                       return e.key < key;
                                     } else {
                                       return e.key <= key;
                                     }
                                   });
    if (forward) {
      if (it != data->entries.end()) {
        key = it->key;
        return true;
      }
    } else {
      if (it != data->entries.begin()) {
        --it;
        key = it->key;
        return true;
      }
    }
    return false;
  }

  bool FindValueForCurrentKey() {
    if (data->recently_touched_keys.count(key)) {
      return false;
    }

    // Find the first entry for the key. The caller promises that it exists.
    auto it = std::partition_point(data->entries.begin(), data->entries.end(),
                                   [&](const Entry& e) {
                                     if (e.key != key) {
                                       return e.key < key;
                                     }
                                     return e.sequence > sequence;
                                   });

    // Find the first visible entry.
    for (;; ++it) {
      if (it == data->entries.end()) {
        return false;
      }
      Entry& e = *it;
      if (e.key != key) {
        return false;
      }
      assert(e.sequence <= sequence);
      if (!e.visible) continue;
      if (e.type == kTypeDeletion) {
        return false;
      }
      if (e.type == kTypeValue) {
        value = e.value;
        valid = true;
        return true;
      }
      assert(e.type == kTypeMerge);
      break;
    }

    // Collect merge operands.
    std::vector<Slice> operands;
    for (; it != data->entries.end(); ++it) {
      Entry& e = *it;
      if (e.key != key) {
        break;
      }
      assert(e.sequence <= sequence);
      if (!e.visible) continue;
      if (e.type == kTypeDeletion) {
        break;
      }
      operands.push_back(e.value);
      if (e.type == kTypeValue) {
        break;
      }
    }

    // Do a merge.
    value = operands.back().ToString();
    for (int i = (int)operands.size() - 2; i >= 0; --i) {
      value.append(",");
      value.append(operands[i].data(), operands[i].size());
    }

    valid = true;
    return true;
  }

  // Start at `key` and move until we encounter a valid value.
  // `forward` defines the direction of movement.
  // If `skip` is true, we're looking for key not equal to `key`.
  void DoTheThing(bool skip, bool forward) {
    while (FindNextKey(skip, forward) && !FindValueForCurrentKey()) {
      skip = true;
    }
  }

  void Seek(const Slice& target) {
    key = target.ToString();
    DoTheThing(false, true);
  }
  void SeekForPrev(const Slice& target) {
    key = target.ToString();
    DoTheThing(false, false);
  }
  void SeekToFirst() { Seek(""); }
  void SeekToLast() {
    key = data->entries.back().key;
    DoTheThing(false, false);
  }
  void Next() {
    assert(Valid());
    DoTheThing(true, true);
  }
  void Prev() {
    assert(Valid());
    DoTheThing(true, false);
  }
};

}  // namespace

// Use an internal iterator that sometimes returns errors and sometimes
// adds/removes entries on the fly. Do random operations on a DBIter and
// check results.
// TODO: can be improved for more coverage:
//   * Override IsKeyPinned() and IsValuePinned() to actually use
//     PinnedIteratorManager and check that there's no use-after free.
//   * Try different combinations of prefix_extractor, total_order_seek,
//     prefix_same_as_start, iterate_lower_bound, iterate_upper_bound.
TEST_F(DBIteratorStressTest, StressTest) {
  // We use a deterministic RNG, and everything happens in a single thread.
  Random64 rnd(826909345792864532ll);

  auto gen_key = [&](int max_key) {
    assert(max_key > 0);
    int len = 0;
    int a = max_key;
    while (a) {
      a /= 10;
      ++len;
    }
    std::string s = ToString(rnd.Next() % static_cast<uint64_t>(max_key));
    s.insert(0, len - (int)s.size(), '0');
    return s;
  };

  Options options;
  options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
  ReadOptions ropt;

  size_t num_matching = 0;
  size_t num_at_end = 0;
  size_t num_not_ok = 0;
  size_t num_recently_removed = 0;

  // Number of iterations for each combination of parameters
  // (there are ~250 of those).
  // Tweak this to change the test run time.
  // As of the time of writing, the test takes ~4 seconds for value of 5000.
  const int num_iterations = 5000;
  // Enable this to print all the operations for debugging.
  bool trace = FLAGS_verbose;

  for (int num_entries : {5, 10, 100}) {
    for (double key_space : {0.1, 1.0, 3.0}) {
      for (ValueType prevalent_entry_type :
           {kTypeValue, kTypeDeletion, kTypeMerge}) {
        for (double error_probability : {0.01, 0.1}) {
          for (double mutation_probability : {0.01, 0.5}) {
            for (double target_hidden_fraction : {0.1, 0.5}) {
              std::string trace_str =
                  "entries: " + ToString(num_entries) +
                  ", key_space: " + ToString(key_space) +
                  ", error_probability: " + ToString(error_probability) +
                  ", mutation_probability: " + ToString(mutation_probability) +
                  ", target_hidden_fraction: " +
                  ToString(target_hidden_fraction);
              SCOPED_TRACE(trace_str);
              if (trace) {
                std::cout << trace_str << std::endl;
              }

              // Generate data.
              Data data;
              int max_key = (int)(num_entries * key_space) + 1;
              for (int i = 0; i < num_entries; ++i) {
                Entry e;
                e.key = gen_key(max_key);
                if (rnd.Next() % 10 != 0) {
                  e.type = prevalent_entry_type;
                } else {
                  const ValueType types[] = {kTypeValue, kTypeDeletion,
                                             kTypeMerge};
                  e.type =
                      types[rnd.Next() % (sizeof(types) / sizeof(types[0]))];
                }
                e.sequence = i;
                e.value = "v" + ToString(i);
                ParsedInternalKey internal_key(e.key, e.sequence, e.type);
                AppendInternalKey(&e.ikey, internal_key);

                data.entries.push_back(e);
              }
              std::sort(data.entries.begin(), data.entries.end());
              if (trace) {
                std::cout << "entries:";
                for (size_t i = 0; i < data.entries.size(); ++i) {
                  Entry& e = data.entries[i];
                  std::cout
                      << "\n  idx " << i << ": \"" << e.key << "\": \""
                      << e.value << "\" seq: " << e.sequence << " type: "
                      << (e.type == kTypeValue
                              ? "val"
                              : e.type == kTypeDeletion ? "del" : "merge");
                }
                std::cout << std::endl;
              }

              std::unique_ptr<Iterator> db_iter;
              std::unique_ptr<ReferenceIterator> ref_iter;
              for (int iteration = 0; iteration < num_iterations; ++iteration) {
                SCOPED_TRACE(iteration);
                // Create a new iterator every ~30 operations.
                if (db_iter == nullptr || rnd.Next() % 30 == 0) {
                  uint64_t sequence = rnd.Next() % (data.entries.size() + 2);
                  ref_iter.reset(new ReferenceIterator(&data, sequence));
                  if (trace) {
                    std::cout << "new iterator, seq: " << sequence << std::endl;
                  }

                  auto internal_iter =
                      new StressTestIterator(&data, &rnd, BytewiseComparator());
                  internal_iter->error_probability = error_probability;
                  internal_iter->mutation_probability = mutation_probability;
                  internal_iter->target_hidden_fraction =
                      target_hidden_fraction;
                  internal_iter->trace = trace;
                  db_iter.reset(NewDBIterator(
                      env_, ropt, ImmutableCFOptions(options),
                      MutableCFOptions(options), BytewiseComparator(),
                      internal_iter, sequence,
                      options.max_sequential_skip_in_iterations,
                      nullptr /*read_callback*/));
                }

                // Do a random operation. It's important to do it on ref_it
                // later than on db_iter to make sure ref_it sees the correct
                // recently_touched_keys.
                std::string old_key;
                bool forward = rnd.Next() % 2 > 0;
                // Do Next()/Prev() ~90% of the time.
                bool seek = !ref_iter->Valid() || rnd.Next() % 10 == 0;
                if (trace) {
                  std::cout << iteration << ": ";
                }

                if (!seek) {
                  assert(db_iter->Valid());
                  old_key = ref_iter->key;
                  if (trace) {
                    std::cout << (forward ? "Next" : "Prev") << std::endl;
                  }

                  if (forward) {
                    db_iter->Next();
                    ref_iter->Next();
                  } else {
                    db_iter->Prev();
                    ref_iter->Prev();
                  }
                } else {
                  data.recently_touched_keys.clear();
                  // Do SeekToFirst less often than Seek.
                  if (rnd.Next() % 4 == 0) {
                    if (trace) {
                      std::cout << (forward ? "SeekToFirst" : "SeekToLast")
                                << std::endl;
                    }

                    if (forward) {
                      old_key = "";
                      db_iter->SeekToFirst();
                      ref_iter->SeekToFirst();
                    } else {
                      old_key = data.entries.back().key;
                      db_iter->SeekToLast();
                      ref_iter->SeekToLast();
                    }
                  } else {
                    old_key = gen_key(max_key);
                    if (trace) {
                      std::cout << (forward ? "Seek" : "SeekForPrev") << " \""
                                << old_key << '"' << std::endl;
                    }
                    if (forward) {
                      db_iter->Seek(old_key);
                      ref_iter->Seek(old_key);
                    } else {
                      db_iter->SeekForPrev(old_key);
                      ref_iter->SeekForPrev(old_key);
                    }
                  }
                }

                // Check the result.
                if (db_iter->Valid()) {
                  ASSERT_TRUE(db_iter->status().ok());
                  if (data.recently_touched_keys.count(
                          db_iter->key().ToString())) {
                    // Ended on a key that may have been mutated during the
                    // operation. Reference iterator skips such keys, so we
                    // can't check the exact result.

                    // Check that the key moved in the right direction.
                    if (forward) {
                      if (seek)
                        ASSERT_GE(db_iter->key().ToString(), old_key);
                      else
                        ASSERT_GT(db_iter->key().ToString(), old_key);
                    } else {
                      if (seek)
                        ASSERT_LE(db_iter->key().ToString(), old_key);
                      else
                        ASSERT_LT(db_iter->key().ToString(), old_key);
                    }

                    if (ref_iter->Valid()) {
                      // Check that DBIter didn't miss any non-mutated key.
                      if (forward) {
                        ASSERT_LT(db_iter->key().ToString(), ref_iter->key);
                      } else {
                        ASSERT_GT(db_iter->key().ToString(), ref_iter->key);
                      }
                    }
                    // Tell the next iteration of the loop to reseek the
                    // iterators.
                    ref_iter->valid = false;

                    ++num_recently_removed;
                  } else {
                    ASSERT_TRUE(ref_iter->Valid());
                    ASSERT_EQ(ref_iter->key, db_iter->key().ToString());
                    ASSERT_EQ(ref_iter->value, db_iter->value());
                    ++num_matching;
                  }
                } else if (db_iter->status().ok()) {
                  ASSERT_FALSE(ref_iter->Valid());
                  ++num_at_end;
                } else {
                  // Non-ok status. Nothing to check here.
                  // Tell the next iteration of the loop to reseek the
                  // iterators.
                  ref_iter->valid = false;
                  ++num_not_ok;
                }
              }
            }
          }
        }
      }
    }
  }

  // Check that all cases were hit many times.
  EXPECT_GT(num_matching, 10000);
  EXPECT_GT(num_at_end, 10000);
  EXPECT_GT(num_not_ok, 10000);
  EXPECT_GT(num_recently_removed, 10000);

  std::cout << "stats:\n  exact matches: " << num_matching
            << "\n  end reached: " << num_at_end
            << "\n  non-ok status: " << num_not_ok
            << "\n  mutated on the fly: " << num_recently_removed << std::endl;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
