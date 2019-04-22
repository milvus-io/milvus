//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#if !defined(OS_WIN) && !defined(ROCKSDB_LITE)

#ifndef GFLAGS
#include <cstdio>
int main() { fprintf(stderr, "Please install gflags to run tools\n"); }
#else

#include <atomic>
#include <functional>
#include <string>
#include <unordered_map>
#include <unistd.h>
#include <sys/time.h>

#include "port/port_posix.h"
#include "rocksdb/env.h"
#include "util/gflags_compat.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "utilities/persistent_cache/hash_table.h"

using std::string;

DEFINE_int32(nsec, 10, "nsec");
DEFINE_int32(nthread_write, 1, "insert %");
DEFINE_int32(nthread_read, 1, "lookup %");
DEFINE_int32(nthread_erase, 1, "erase %");

namespace rocksdb {

//
// HashTableImpl interface
//
// Abstraction of a hash table implementation
template <class Key, class Value>
class HashTableImpl {
 public:
  virtual ~HashTableImpl() {}

  virtual bool Insert(const Key& key, const Value& val) = 0;
  virtual bool Erase(const Key& key) = 0;
  virtual bool Lookup(const Key& key, Value* val) = 0;
};

// HashTableBenchmark
//
// Abstraction to test a given hash table implementation. The test mostly
// focus on insert, lookup and erase. The test can operate in test mode and
// benchmark mode.
class HashTableBenchmark {
 public:
  explicit HashTableBenchmark(HashTableImpl<size_t, std::string>* impl,
                              const size_t sec = 10,
                              const size_t nthread_write = 1,
                              const size_t nthread_read = 1,
                              const size_t nthread_erase = 1)
      : impl_(impl),
        sec_(sec),
        ninserts_(0),
        nreads_(0),
        nerases_(0),
        nerases_failed_(0),
        quit_(false) {
    Prepop();

    StartThreads(nthread_write, WriteMain);
    StartThreads(nthread_read, ReadMain);
    StartThreads(nthread_erase, EraseMain);

    uint64_t start = NowInMillSec();
    while (!quit_) {
      quit_ = NowInMillSec() - start > sec_ * 1000;
      /* sleep override */ sleep(1);
    }

    Env* env = Env::Default();
    env->WaitForJoin();

    if (sec_) {
      printf("Result \n");
      printf("====== \n");
      printf("insert/sec = %f \n", ninserts_ / static_cast<double>(sec_));
      printf("read/sec = %f \n", nreads_ / static_cast<double>(sec_));
      printf("erases/sec = %f \n", nerases_ / static_cast<double>(sec_));
      const uint64_t ops = ninserts_ + nreads_ + nerases_;
      printf("ops/sec = %f \n", ops / static_cast<double>(sec_));
      printf("erase fail = %d (%f%%)\n", static_cast<int>(nerases_failed_),
             static_cast<float>(nerases_failed_ / nerases_ * 100));
      printf("====== \n");
    }
  }

  void RunWrite() {
    while (!quit_) {
      size_t k = insert_key_++;
      std::string tmp(1000, k % 255);
      bool status = impl_->Insert(k, tmp);
      assert(status);
      ninserts_++;
    }
  }

  void RunRead() {
    Random64 rgen(time(nullptr));
    while (!quit_) {
      std::string s;
      size_t k = rgen.Next() % max_prepop_key;
      bool status = impl_->Lookup(k, &s);
      assert(status);
      assert(s == std::string(1000, k % 255));
      nreads_++;
    }
  }

  void RunErase() {
    while (!quit_) {
      size_t k = erase_key_++;
      bool status = impl_->Erase(k);
      nerases_failed_ += !status;
      nerases_++;
    }
  }

 private:
  // Start threads for a given function
  void StartThreads(const size_t n, void (*fn)(void*)) {
    Env* env = Env::Default();
    for (size_t i = 0; i < n; ++i) {
      env->StartThread(fn, this);
    }
  }

  // Prepop the hash table with 1M keys
  void Prepop() {
    for (size_t i = 0; i < max_prepop_key; ++i) {
      bool status = impl_->Insert(i, std::string(1000, i % 255));
      assert(status);
    }

    erase_key_ = insert_key_ = max_prepop_key;

    for (size_t i = 0; i < 10 * max_prepop_key; ++i) {
      bool status = impl_->Insert(insert_key_++, std::string(1000, 'x'));
      assert(status);
    }
  }

  static uint64_t NowInMillSec() {
    timeval tv;
    gettimeofday(&tv, /*tz=*/nullptr);
    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
  }

  //
  //  Wrapper functions for thread entry
  //
  static void WriteMain(void* args) {
    reinterpret_cast<HashTableBenchmark*>(args)->RunWrite();
  }

  static void ReadMain(void* args) {
    reinterpret_cast<HashTableBenchmark*>(args)->RunRead();
  }

  static void EraseMain(void* args) {
    reinterpret_cast<HashTableBenchmark*>(args)->RunErase();
  }

  HashTableImpl<size_t, std::string>* impl_;         // Implementation to test
  const size_t sec_;                                 // Test time
  const size_t max_prepop_key = 1ULL * 1024 * 1024;  // Max prepop key
  std::atomic<size_t> insert_key_;                   // Last inserted key
  std::atomic<size_t> erase_key_;                    // Erase key
  std::atomic<size_t> ninserts_;                     // Number of inserts
  std::atomic<size_t> nreads_;                       // Number of reads
  std::atomic<size_t> nerases_;                      // Number of erases
  std::atomic<size_t> nerases_failed_;               // Number of erases failed
  bool quit_;  // Should the threads quit ?
};

//
// SimpleImpl
// Lock safe unordered_map implementation
class SimpleImpl : public HashTableImpl<size_t, string> {
 public:
  bool Insert(const size_t& key, const string& val) override {
    WriteLock _(&rwlock_);
    map_.insert(make_pair(key, val));
    return true;
  }

  bool Erase(const size_t& key) override {
    WriteLock _(&rwlock_);
    auto it = map_.find(key);
    if (it == map_.end()) {
      return false;
    }
    map_.erase(it);
    return true;
  }

  bool Lookup(const size_t& key, string* val) override {
    ReadLock _(&rwlock_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      *val = it->second;
    }
    return it != map_.end();
  }

 private:
  port::RWMutex rwlock_;
  std::unordered_map<size_t, string> map_;
};

//
// GranularLockImpl
// Thread safe custom RocksDB implementation of hash table with granular
// locking
class GranularLockImpl : public HashTableImpl<size_t, string> {
 public:
  bool Insert(const size_t& key, const string& val) override {
    Node n(key, val);
    return impl_.Insert(n);
  }

  bool Erase(const size_t& key) override {
    Node n(key, string());
    return impl_.Erase(n, nullptr);
  }

  bool Lookup(const size_t& key, string* val) override {
    Node n(key, string());
    port::RWMutex* rlock;
    bool status = impl_.Find(n, &n, &rlock);
    if (status) {
      ReadUnlock _(rlock);
      *val = n.val_;
    }
    return status;
  }

 private:
  struct Node {
    explicit Node(const size_t key, const string& val) : key_(key), val_(val) {}

    size_t key_ = 0;
    string val_;
  };

  struct Hash {
    uint64_t operator()(const Node& node) {
      return std::hash<uint64_t>()(node.key_);
    }
  };

  struct Equal {
    bool operator()(const Node& lhs, const Node& rhs) {
      return lhs.key_ == rhs.key_;
    }
  };

  HashTable<Node, Hash, Equal> impl_;
};

}  // namespace rocksdb

//
// main
//
int main(int argc, char** argv) {
  GFLAGS_NAMESPACE::SetUsageMessage(std::string("\nUSAGE:\n") +
                                    std::string(argv[0]) + " [OPTIONS]...");
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, false);

  //
  // Micro benchmark unordered_map
  //
  printf("Micro benchmarking std::unordered_map \n");
  {
    rocksdb::SimpleImpl impl;
    rocksdb::HashTableBenchmark _(&impl, FLAGS_nsec, FLAGS_nthread_write,
                                  FLAGS_nthread_read, FLAGS_nthread_erase);
  }
  //
  // Micro benchmark scalable hash table
  //
  printf("Micro benchmarking scalable hash map \n");
  {
    rocksdb::GranularLockImpl impl;
    rocksdb::HashTableBenchmark _(&impl, FLAGS_nsec, FLAGS_nthread_write,
                                  FLAGS_nthread_read, FLAGS_nthread_erase);
  }

  return 0;
}
#endif  // #ifndef GFLAGS
#else
int main(int /*argc*/, char** /*argv*/) { return 0; }
#endif
