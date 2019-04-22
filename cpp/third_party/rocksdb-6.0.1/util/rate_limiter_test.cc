//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util/rate_limiter.h"

#include <inttypes.h>
#include <chrono>
#include <limits>

#include "db/db_test_util.h"
#include "rocksdb/env.h"
#include "util/random.h"
#include "util/sync_point.h"
#include "util/testharness.h"

namespace rocksdb {

// TODO(yhchiang): the rate will not be accurate when we run test in parallel.
class RateLimiterTest : public testing::Test {};

TEST_F(RateLimiterTest, OverflowRate) {
  GenericRateLimiter limiter(port::kMaxInt64, 1000, 10,
                             RateLimiter::Mode::kWritesOnly, Env::Default(),
                             false /* auto_tuned */);
  ASSERT_GT(limiter.GetSingleBurstBytes(), 1000000000ll);
}

TEST_F(RateLimiterTest, StartStop) {
  std::unique_ptr<RateLimiter> limiter(NewGenericRateLimiter(100, 100, 10));
}

TEST_F(RateLimiterTest, Modes) {
  for (auto mode : {RateLimiter::Mode::kWritesOnly,
                    RateLimiter::Mode::kReadsOnly, RateLimiter::Mode::kAllIo}) {
    GenericRateLimiter limiter(
        2000 /* rate_bytes_per_sec */, 1000 * 1000 /* refill_period_us */,
        10 /* fairness */, mode, Env::Default(), false /* auto_tuned */);
    limiter.Request(1000 /* bytes */, Env::IO_HIGH, nullptr /* stats */,
                    RateLimiter::OpType::kRead);
    if (mode == RateLimiter::Mode::kWritesOnly) {
      ASSERT_EQ(0, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    } else {
      ASSERT_EQ(1000, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    }

    limiter.Request(1000 /* bytes */, Env::IO_HIGH, nullptr /* stats */,
                    RateLimiter::OpType::kWrite);
    if (mode == RateLimiter::Mode::kAllIo) {
      ASSERT_EQ(2000, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    } else {
      ASSERT_EQ(1000, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    }
  }
}

#if !(defined(TRAVIS) && defined(OS_MACOSX))
TEST_F(RateLimiterTest, Rate) {
  auto* env = Env::Default();
  struct Arg {
    Arg(int32_t _target_rate, int _burst)
        : limiter(NewGenericRateLimiter(_target_rate, 100 * 1000, 10)),
          request_size(_target_rate / 10),
          burst(_burst) {}
    std::unique_ptr<RateLimiter> limiter;
    int32_t request_size;
    int burst;
  };

  auto writer = [](void* p) {
    auto* thread_env = Env::Default();
    auto* arg = static_cast<Arg*>(p);
    // Test for 2 seconds
    auto until = thread_env->NowMicros() + 2 * 1000000;
    Random r((uint32_t)(thread_env->NowNanos() %
                        std::numeric_limits<uint32_t>::max()));
    while (thread_env->NowMicros() < until) {
      for (int i = 0; i < static_cast<int>(r.Skewed(arg->burst) + 1); ++i) {
        arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1,
                              Env::IO_HIGH, nullptr /* stats */,
                              RateLimiter::OpType::kWrite);
      }
      arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1, Env::IO_LOW,
                            nullptr /* stats */, RateLimiter::OpType::kWrite);
    }
  };

  for (int i = 1; i <= 16; i *= 2) {
    int32_t target = i * 1024 * 10;
    Arg arg(target, i / 4 + 1);
    int64_t old_total_bytes_through = 0;
    for (int iter = 1; iter <= 2; ++iter) {
      // second iteration changes the target dynamically
      if (iter == 2) {
        target *= 2;
        arg.limiter->SetBytesPerSecond(target);
      }
      auto start = env->NowMicros();
      for (int t = 0; t < i; ++t) {
        env->StartThread(writer, &arg);
      }
      env->WaitForJoin();

      auto elapsed = env->NowMicros() - start;
      double rate =
          (arg.limiter->GetTotalBytesThrough() - old_total_bytes_through) *
          1000000.0 / elapsed;
      old_total_bytes_through = arg.limiter->GetTotalBytesThrough();
      fprintf(stderr,
              "request size [1 - %" PRIi32 "], limit %" PRIi32
              " KB/sec, actual rate: %lf KB/sec, elapsed %.2lf seconds\n",
              arg.request_size - 1, target / 1024, rate / 1024,
              elapsed / 1000000.0);

      ASSERT_GE(rate / target, 0.80);
      ASSERT_LE(rate / target, 1.25);
    }
  }
}
#endif

TEST_F(RateLimiterTest, LimitChangeTest) {
  // starvation test when limit changes to a smaller value
  int64_t refill_period = 1000 * 1000;
  auto* env = Env::Default();
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  struct Arg {
    Arg(int32_t _request_size, Env::IOPriority _pri,
        std::shared_ptr<RateLimiter> _limiter)
        : request_size(_request_size), pri(_pri), limiter(_limiter) {}
    int32_t request_size;
    Env::IOPriority pri;
    std::shared_ptr<RateLimiter> limiter;
  };

  auto writer = [](void* p) {
    auto* arg = static_cast<Arg*>(p);
    arg->limiter->Request(arg->request_size, arg->pri, nullptr /* stats */,
                          RateLimiter::OpType::kWrite);
  };

  for (uint32_t i = 1; i <= 16; i <<= 1) {
    int32_t target = i * 1024 * 10;
    // refill per second
    for (int iter = 0; iter < 2; iter++) {
      std::shared_ptr<RateLimiter> limiter =
          std::make_shared<GenericRateLimiter>(
              target, refill_period, 10, RateLimiter::Mode::kWritesOnly,
              Env::Default(), false /* auto_tuned */);
      rocksdb::SyncPoint::GetInstance()->LoadDependency(
          {{"GenericRateLimiter::Request",
            "RateLimiterTest::LimitChangeTest:changeLimitStart"},
           {"RateLimiterTest::LimitChangeTest:changeLimitEnd",
            "GenericRateLimiter::Refill"}});
      Arg arg(target, Env::IO_HIGH, limiter);
      // The idea behind is to start a request first, then before it refills,
      // update limit to a different value (2X/0.5X). No starvation should
      // be guaranteed under any situation
      // TODO(lightmark): more test cases are welcome.
      env->StartThread(writer, &arg);
      int32_t new_limit = (target << 1) >> (iter << 1);
      TEST_SYNC_POINT("RateLimiterTest::LimitChangeTest:changeLimitStart");
      arg.limiter->SetBytesPerSecond(new_limit);
      TEST_SYNC_POINT("RateLimiterTest::LimitChangeTest:changeLimitEnd");
      env->WaitForJoin();
      fprintf(stderr,
              "[COMPLETE] request size %" PRIi32 " KB, new limit %" PRIi32
              "KB/sec, refill period %" PRIi64 " ms\n",
              target / 1024, new_limit / 1024, refill_period / 1000);
    }
  }
}

TEST_F(RateLimiterTest, AutoTuneIncreaseWhenFull) {
  const std::chrono::seconds kTimePerRefill(1);
  const int kRefillsPerTune = 100;  // needs to match util/rate_limiter.cc

  SpecialEnv special_env(Env::Default());
  special_env.no_slowdown_ = true;
  special_env.time_elapse_only_sleep_ = true;

  auto stats = CreateDBStatistics();
  std::unique_ptr<RateLimiter> rate_limiter(new GenericRateLimiter(
      1000 /* rate_bytes_per_sec */,
      std::chrono::microseconds(kTimePerRefill).count(), 10 /* fairness */,
      RateLimiter::Mode::kWritesOnly, &special_env, true /* auto_tuned */));

  // Use callback to advance time because we need to advance (1) after Request()
  // has determined the bytes are not available; and (2) before Refill()
  // computes the next refill time (ensuring refill time in the future allows
  // the next request to drain the rate limiter).
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "GenericRateLimiter::Refill", [&](void* /*arg*/) {
        special_env.SleepForMicroseconds(static_cast<int>(
            std::chrono::microseconds(kTimePerRefill).count()));
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // verify rate limit increases after a sequence of periods where rate limiter
  // is always drained
  int64_t orig_bytes_per_sec = rate_limiter->GetSingleBurstBytes();
  rate_limiter->Request(orig_bytes_per_sec, Env::IO_HIGH, stats.get(),
                        RateLimiter::OpType::kWrite);
  while (std::chrono::microseconds(special_env.NowMicros()) <=
         kRefillsPerTune * kTimePerRefill) {
    rate_limiter->Request(orig_bytes_per_sec, Env::IO_HIGH, stats.get(),
                          RateLimiter::OpType::kWrite);
  }
  int64_t new_bytes_per_sec = rate_limiter->GetSingleBurstBytes();
  ASSERT_GT(new_bytes_per_sec, orig_bytes_per_sec);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  // decreases after a sequence of periods where rate limiter is not drained
  orig_bytes_per_sec = new_bytes_per_sec;
  special_env.SleepForMicroseconds(static_cast<int>(
      kRefillsPerTune * std::chrono::microseconds(kTimePerRefill).count()));
  // make a request so tuner can be triggered
  rate_limiter->Request(1 /* bytes */, Env::IO_HIGH, stats.get(),
                        RateLimiter::OpType::kWrite);
  new_bytes_per_sec = rate_limiter->GetSingleBurstBytes();
  ASSERT_LT(new_bytes_per_sec, orig_bytes_per_sec);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
