//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <memory>

#include "db/db_test_util.h"
#include "util/repeatable_thread.h"
#include "util/testharness.h"

class RepeatableThreadTest : public testing::Test {
 public:
  RepeatableThreadTest()
      : mock_env_(new rocksdb::MockTimeEnv(rocksdb::Env::Default())) {}

 protected:
  std::unique_ptr<rocksdb::MockTimeEnv> mock_env_;
};

TEST_F(RepeatableThreadTest, TimedTest) {
  constexpr uint64_t kSecond = 1000000;  // 1s = 1000000us
  constexpr int kIteration = 3;
  rocksdb::Env* env = rocksdb::Env::Default();
  rocksdb::port::Mutex mutex;
  rocksdb::port::CondVar test_cv(&mutex);
  int count = 0;
  uint64_t prev_time = env->NowMicros();
  rocksdb::RepeatableThread thread(
      [&] {
        rocksdb::MutexLock l(&mutex);
        count++;
        uint64_t now = env->NowMicros();
        assert(count == 1 || prev_time + 1 * kSecond <= now);
        prev_time = now;
        if (count >= kIteration) {
          test_cv.SignalAll();
        }
      },
      "rt_test", env, 1 * kSecond);
  // Wait for execution finish.
  {
    rocksdb::MutexLock l(&mutex);
    while (count < kIteration) {
      test_cv.Wait();
    }
  }

  // Test cancel
  thread.cancel();
}

TEST_F(RepeatableThreadTest, MockEnvTest) {
  constexpr uint64_t kSecond = 1000000;  // 1s = 1000000us
  constexpr int kIteration = 3;
  mock_env_->set_current_time(0);  // in seconds
  std::atomic<int> count{0};
  rocksdb::RepeatableThread thread([&] { count++; }, "rt_test", mock_env_.get(),
                                   1 * kSecond, 1 * kSecond);
  for (int i = 1; i <= kIteration; i++) {
    // Bump current time
    thread.TEST_WaitForRun([&] { mock_env_->set_current_time(i); });
  }
  // Test function should be exectued exactly kIteraion times.
  ASSERT_EQ(kIteration, count.load());

  // Test cancel
  thread.cancel();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
