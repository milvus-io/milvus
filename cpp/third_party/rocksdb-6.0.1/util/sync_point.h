//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <assert.h>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// This is only set from db_stress.cc and for testing only.
// If non-zero, kill at various points in source code with probability 1/this
extern int rocksdb_kill_odds;
// If kill point has a prefix on this list, will skip killing.
extern std::vector<std::string> rocksdb_kill_prefix_blacklist;

#ifdef NDEBUG
// empty in release build
#define TEST_KILL_RANDOM(kill_point, rocksdb_kill_odds)
#else

namespace rocksdb {
// Kill the process with probability 1/odds for testing.
extern void TestKillRandom(std::string kill_point, int odds,
                           const std::string& srcfile, int srcline);

// To avoid crashing always at some frequently executed codepaths (during
// kill random test), use this factor to reduce odds
#define REDUCE_ODDS 2
#define REDUCE_ODDS2 4

#define TEST_KILL_RANDOM(kill_point, rocksdb_kill_odds)                  \
  {                                                                      \
    if (rocksdb_kill_odds > 0) {                                         \
      TestKillRandom(kill_point, rocksdb_kill_odds, __FILE__, __LINE__); \
    }                                                                    \
  }
}  // namespace rocksdb
#endif

#ifdef NDEBUG
#define TEST_SYNC_POINT(x)
#define TEST_IDX_SYNC_POINT(x, index)
#define TEST_SYNC_POINT_CALLBACK(x, y)
#define INIT_SYNC_POINT_SINGLETONS()
#else

namespace rocksdb {

// This class provides facility to reproduce race conditions deterministically
// in unit tests.
// Developer could specify sync points in the codebase via TEST_SYNC_POINT.
// Each sync point represents a position in the execution stream of a thread.
// In the unit test, 'Happens After' relationship among sync points could be
// setup via SyncPoint::LoadDependency, to reproduce a desired interleave of
// threads execution.
// Refer to (DBTest,TransactionLogIteratorRace), for an example use case.

class SyncPoint {
 public:
  static SyncPoint* GetInstance();

  SyncPoint(const SyncPoint&) = delete;
  SyncPoint& operator=(const SyncPoint&) = delete;
  ~SyncPoint();

  struct SyncPointPair {
    std::string predecessor;
    std::string successor;
  };

  // call once at the beginning of a test to setup the dependency between
  // sync points
  void LoadDependency(const std::vector<SyncPointPair>& dependencies);

  // call once at the beginning of a test to setup the dependency between
  // sync points and setup markers indicating the successor is only enabled
  // when it is processed on the same thread as the predecessor.
  // When adding a marker, it implicitly adds a dependency for the marker pair.
  void LoadDependencyAndMarkers(const std::vector<SyncPointPair>& dependencies,
                                const std::vector<SyncPointPair>& markers);

  // The argument to the callback is passed through from
  // TEST_SYNC_POINT_CALLBACK(); nullptr if TEST_SYNC_POINT or
  // TEST_IDX_SYNC_POINT was used.
  void SetCallBack(const std::string& point,
                   const std::function<void(void*)>& callback);

  // Clear callback function by point
  void ClearCallBack(const std::string& point);

  // Clear all call back functions.
  void ClearAllCallBacks();

  // enable sync point processing (disabled on startup)
  void EnableProcessing();

  // disable sync point processing
  void DisableProcessing();

  // remove the execution trace of all sync points
  void ClearTrace();

  // triggered by TEST_SYNC_POINT, blocking execution until all predecessors
  // are executed.
  // And/or call registered callback function, with argument `cb_arg`
  void Process(const std::string& point, void* cb_arg = nullptr);

  // TODO: it might be useful to provide a function that blocks until all
  // sync points are cleared.

  // We want this to be public so we can
  // subclass the implementation
  struct Data;

 private:
   // Singleton
  SyncPoint();
  Data*  impl_;
};

}  // namespace rocksdb

// Use TEST_SYNC_POINT to specify sync points inside code base.
// Sync points can have happens-after depedency on other sync points,
// configured at runtime via SyncPoint::LoadDependency. This could be
// utilized to re-produce race conditions between threads.
// See TransactionLogIteratorRace in db_test.cc for an example use case.
// TEST_SYNC_POINT is no op in release build.
#define TEST_SYNC_POINT(x) rocksdb::SyncPoint::GetInstance()->Process(x)
#define TEST_IDX_SYNC_POINT(x, index) \
  rocksdb::SyncPoint::GetInstance()->Process(x + std::to_string(index))
#define TEST_SYNC_POINT_CALLBACK(x, y) \
  rocksdb::SyncPoint::GetInstance()->Process(x, y)
#define INIT_SYNC_POINT_SINGLETONS() \
  (void)rocksdb::SyncPoint::GetInstance();
#endif  // NDEBUG
