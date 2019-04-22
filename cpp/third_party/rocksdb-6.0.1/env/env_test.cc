//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef OS_WIN
#include <sys/ioctl.h>
#endif

#ifdef ROCKSDB_MALLOC_USABLE_SIZE
#ifdef OS_FREEBSD
#include <malloc_np.h>
#else
#include <malloc.h>
#endif
#endif
#include <sys/types.h>

#include <iostream>
#include <unordered_set>
#include <atomic>
#include <list>

#ifdef OS_LINUX
#include <fcntl.h>
#include <linux/fs.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#ifdef ROCKSDB_FALLOCATE_PRESENT
#include <errno.h>
#endif

#include "env/env_chroot.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/log_buffer.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"

#ifdef OS_LINUX
static const size_t kPageSize = sysconf(_SC_PAGESIZE);
#else
static const size_t kPageSize = 4 * 1024;
#endif

namespace rocksdb {

static const int kDelayMicros = 100000;

struct Deleter {
  explicit Deleter(void (*fn)(void*)) : fn_(fn) {}

  void operator()(void* ptr) {
    assert(fn_);
    assert(ptr);
    (*fn_)(ptr);
  }

  void (*fn_)(void*);
};

std::unique_ptr<char, Deleter> NewAligned(const size_t size, const char ch) {
  char* ptr = nullptr;
#ifdef OS_WIN
  if (nullptr == (ptr = reinterpret_cast<char*>(_aligned_malloc(size, kPageSize)))) {
    return std::unique_ptr<char, Deleter>(nullptr, Deleter(_aligned_free));
  }
  std::unique_ptr<char, Deleter> uptr(ptr, Deleter(_aligned_free));
#else
  if (posix_memalign(reinterpret_cast<void**>(&ptr), kPageSize, size) != 0) {
    return std::unique_ptr<char, Deleter>(nullptr, Deleter(free));
  }
  std::unique_ptr<char, Deleter> uptr(ptr, Deleter(free));
#endif
  memset(uptr.get(), ch, size);
  return uptr;
}

class EnvPosixTest : public testing::Test {
 private:
  port::Mutex mu_;
  std::string events_;

 public:
  Env* env_;
  bool direct_io_;
  EnvPosixTest() : env_(Env::Default()), direct_io_(false) {}
};

class EnvPosixTestWithParam
    : public EnvPosixTest,
      public ::testing::WithParamInterface<std::pair<Env*, bool>> {
 public:
  EnvPosixTestWithParam() {
    std::pair<Env*, bool> param_pair = GetParam();
    env_ = param_pair.first;
    direct_io_ = param_pair.second;
  }

  void WaitThreadPoolsEmpty() {
    // Wait until the thread pools are empty.
    while (env_->GetThreadPoolQueueLen(Env::Priority::LOW) != 0) {
      Env::Default()->SleepForMicroseconds(kDelayMicros);
    }
    while (env_->GetThreadPoolQueueLen(Env::Priority::HIGH) != 0) {
      Env::Default()->SleepForMicroseconds(kDelayMicros);
    }
  }

  ~EnvPosixTestWithParam() override { WaitThreadPoolsEmpty(); }
};

static void SetBool(void* ptr) {
  reinterpret_cast<std::atomic<bool>*>(ptr)->store(true);
}

TEST_F(EnvPosixTest, DISABLED_RunImmediately) {
  for (int pri = Env::BOTTOM; pri < Env::TOTAL; ++pri) {
    std::atomic<bool> called(false);
    env_->SetBackgroundThreads(1, static_cast<Env::Priority>(pri));
    env_->Schedule(&SetBool, &called, static_cast<Env::Priority>(pri));
    Env::Default()->SleepForMicroseconds(kDelayMicros);
    ASSERT_TRUE(called.load());
  }
}

TEST_F(EnvPosixTest, RunEventually) {
  std::atomic<bool> called(false);
  env_->StartThread(&SetBool, &called);
  env_->WaitForJoin();
  ASSERT_TRUE(called.load());
}

#ifdef OS_WIN
TEST_F(EnvPosixTest, AreFilesSame) {
  {
    bool tmp;
    if (env_->AreFilesSame("", "", &tmp).IsNotSupported()) {
      fprintf(stderr,
              "skipping EnvBasicTestWithParam.AreFilesSame due to "
              "unsupported Env::AreFilesSame\n");
      return;
    }
  }

  const EnvOptions soptions;
  auto* env = Env::Default();
  std::string same_file_name = test::PerThreadDBPath(env, "same_file");
  std::string same_file_link_name = same_file_name + "_link";

  std::unique_ptr<WritableFile> same_file;
  ASSERT_OK(env->NewWritableFile(same_file_name,
    &same_file, soptions));
  same_file->Append("random_data");
  ASSERT_OK(same_file->Flush());
  same_file.reset();

  ASSERT_OK(env->LinkFile(same_file_name, same_file_link_name));
  bool result = false;
  ASSERT_OK(env->AreFilesSame(same_file_name, same_file_link_name, &result));
  ASSERT_TRUE(result);
}
#endif

#ifdef OS_LINUX
TEST_F(EnvPosixTest, DISABLED_FilePermission) {
  // Only works for Linux environment
  if (env_ == Env::Default()) {
    EnvOptions soptions;
    std::vector<std::string> fileNames{
        test::PerThreadDBPath(env_, "testfile"),
        test::PerThreadDBPath(env_, "testfile1")};
    std::unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fileNames[0], &wfile, soptions));
    ASSERT_OK(env_->NewWritableFile(fileNames[1], &wfile, soptions));
    wfile.reset();
    std::unique_ptr<RandomRWFile> rwfile;
    ASSERT_OK(env_->NewRandomRWFile(fileNames[1], &rwfile, soptions));

    struct stat sb;
    for (const auto& filename : fileNames) {
      if (::stat(filename.c_str(), &sb) == 0) {
        ASSERT_EQ(sb.st_mode & 0777, 0644);
      }
      env_->DeleteFile(filename);
    }

    env_->SetAllowNonOwnerAccess(false);
    ASSERT_OK(env_->NewWritableFile(fileNames[0], &wfile, soptions));
    ASSERT_OK(env_->NewWritableFile(fileNames[1], &wfile, soptions));
    wfile.reset();
    ASSERT_OK(env_->NewRandomRWFile(fileNames[1], &rwfile, soptions));

    for (const auto& filename : fileNames) {
      if (::stat(filename.c_str(), &sb) == 0) {
        ASSERT_EQ(sb.st_mode & 0777, 0600);
      }
      env_->DeleteFile(filename);
    }
  }
}
#endif

TEST_F(EnvPosixTest, MemoryMappedFileBuffer) {
  const int kFileBytes = 1 << 15;  // 32 KB
  std::string expected_data;
  std::string fname = test::PerThreadDBPath(env_, "testfile");
  {
    std::unique_ptr<WritableFile> wfile;
    const EnvOptions soptions;
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));

    Random rnd(301);
    test::RandomString(&rnd, kFileBytes, &expected_data);
    ASSERT_OK(wfile->Append(expected_data));
  }

  std::unique_ptr<MemoryMappedFileBuffer> mmap_buffer;
  Status status = env_->NewMemoryMappedFileBuffer(fname, &mmap_buffer);
  // it should be supported at least on linux
#if !defined(OS_LINUX)
  if (status.IsNotSupported()) {
    fprintf(stderr,
            "skipping EnvPosixTest.MemoryMappedFileBuffer due to "
            "unsupported Env::NewMemoryMappedFileBuffer\n");
    return;
  }
#endif  // !defined(OS_LINUX)

  ASSERT_OK(status);
  ASSERT_NE(nullptr, mmap_buffer.get());
  ASSERT_NE(nullptr, mmap_buffer->GetBase());
  ASSERT_EQ(kFileBytes, mmap_buffer->GetLen());
  std::string actual_data(reinterpret_cast<const char*>(mmap_buffer->GetBase()),
                          mmap_buffer->GetLen());
  ASSERT_EQ(expected_data, actual_data);
}

TEST_P(EnvPosixTestWithParam, UnSchedule) {
  std::atomic<bool> called(false);
  env_->SetBackgroundThreads(1, Env::LOW);

  /* Block the low priority queue */
  test::SleepingBackgroundTask sleeping_task, sleeping_task1;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::LOW);

  /* Schedule another task */
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task1,
                 Env::Priority::LOW, &sleeping_task1);

  /* Remove it with a different tag  */
  ASSERT_EQ(0, env_->UnSchedule(&called, Env::Priority::LOW));

  /* Remove it from the queue with the right tag */
  ASSERT_EQ(1, env_->UnSchedule(&sleeping_task1, Env::Priority::LOW));

  // Unblock background thread
  sleeping_task.WakeUp();

  /* Schedule another task */
  env_->Schedule(&SetBool, &called);
  for (int i = 0; i < kDelayMicros; i++) {
    if (called.load()) {
      break;
    }
    Env::Default()->SleepForMicroseconds(1);
  }
  ASSERT_TRUE(called.load());

  ASSERT_TRUE(!sleeping_task.IsSleeping() && !sleeping_task1.IsSleeping());
  WaitThreadPoolsEmpty();
}

// This tests assumes that the last scheduled
// task will run last. In fact, in the allotted
// sleeping time nothing may actually run or they may
// run in any order. The purpose of the test is unclear.
#ifndef OS_WIN
TEST_P(EnvPosixTestWithParam, RunMany) {
  std::atomic<int> last_id(0);

  struct CB {
    std::atomic<int>* last_id_ptr;  // Pointer to shared slot
    int id;                         // Order# for the execution of this callback

    CB(std::atomic<int>* p, int i) : last_id_ptr(p), id(i) {}

    static void Run(void* v) {
      CB* cb = reinterpret_cast<CB*>(v);
      int cur = cb->last_id_ptr->load();
      ASSERT_EQ(cb->id - 1, cur);
      cb->last_id_ptr->store(cb->id);
    }
  };

  // Schedule in different order than start time
  CB cb1(&last_id, 1);
  CB cb2(&last_id, 2);
  CB cb3(&last_id, 3);
  CB cb4(&last_id, 4);
  env_->Schedule(&CB::Run, &cb1);
  env_->Schedule(&CB::Run, &cb2);
  env_->Schedule(&CB::Run, &cb3);
  env_->Schedule(&CB::Run, &cb4);

  Env::Default()->SleepForMicroseconds(kDelayMicros);
  int cur = last_id.load(std::memory_order_acquire);
  ASSERT_EQ(4, cur);
  WaitThreadPoolsEmpty();
}
#endif

struct State {
  port::Mutex mu;
  int val;
  int num_running;
};

static void ThreadBody(void* arg) {
  State* s = reinterpret_cast<State*>(arg);
  s->mu.Lock();
  s->val += 1;
  s->num_running -= 1;
  s->mu.Unlock();
}

TEST_P(EnvPosixTestWithParam, StartThread) {
  State state;
  state.val = 0;
  state.num_running = 3;
  for (int i = 0; i < 3; i++) {
    env_->StartThread(&ThreadBody, &state);
  }
  while (true) {
    state.mu.Lock();
    int num = state.num_running;
    state.mu.Unlock();
    if (num == 0) {
      break;
    }
    Env::Default()->SleepForMicroseconds(kDelayMicros);
  }
  ASSERT_EQ(state.val, 3);
  WaitThreadPoolsEmpty();
}

TEST_P(EnvPosixTestWithParam, TwoPools) {
  // Data structures to signal tasks to run.
  port::Mutex mutex;
  port::CondVar cv(&mutex);
  bool should_start = false;

  class CB {
   public:
    CB(const std::string& pool_name, int pool_size, port::Mutex* trigger_mu,
       port::CondVar* trigger_cv, bool* _should_start)
        : mu_(),
          num_running_(0),
          num_finished_(0),
          pool_size_(pool_size),
          pool_name_(pool_name),
          trigger_mu_(trigger_mu),
          trigger_cv_(trigger_cv),
          should_start_(_should_start) {}

    static void Run(void* v) {
      CB* cb = reinterpret_cast<CB*>(v);
      cb->Run();
    }

    void Run() {
      {
        MutexLock l(&mu_);
        num_running_++;
        // make sure we don't have more than pool_size_ jobs running.
        ASSERT_LE(num_running_, pool_size_.load());
      }

      {
        MutexLock l(trigger_mu_);
        while (!(*should_start_)) {
          trigger_cv_->Wait();
        }
      }

      {
        MutexLock l(&mu_);
        num_running_--;
        num_finished_++;
      }
    }

    int NumFinished() {
      MutexLock l(&mu_);
      return num_finished_;
    }

    void Reset(int pool_size) {
      pool_size_.store(pool_size);
      num_finished_ = 0;
    }

   private:
    port::Mutex mu_;
    int num_running_;
    int num_finished_;
    std::atomic<int> pool_size_;
    std::string pool_name_;
    port::Mutex* trigger_mu_;
    port::CondVar* trigger_cv_;
    bool* should_start_;
  };

  const int kLowPoolSize = 2;
  const int kHighPoolSize = 4;
  const int kJobs = 8;

  CB low_pool_job("low", kLowPoolSize, &mutex, &cv, &should_start);
  CB high_pool_job("high", kHighPoolSize, &mutex, &cv, &should_start);

  env_->SetBackgroundThreads(kLowPoolSize);
  env_->SetBackgroundThreads(kHighPoolSize, Env::Priority::HIGH);

  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::LOW));
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  // schedule same number of jobs in each pool
  for (int i = 0; i < kJobs; i++) {
    env_->Schedule(&CB::Run, &low_pool_job);
    env_->Schedule(&CB::Run, &high_pool_job, Env::Priority::HIGH);
  }
  // Wait a short while for the jobs to be dispatched.
  int sleep_count = 0;
  while ((unsigned int)(kJobs - kLowPoolSize) !=
             env_->GetThreadPoolQueueLen(Env::Priority::LOW) ||
         (unsigned int)(kJobs - kHighPoolSize) !=
             env_->GetThreadPoolQueueLen(Env::Priority::HIGH)) {
    env_->SleepForMicroseconds(kDelayMicros);
    if (++sleep_count > 100) {
      break;
    }
  }

  ASSERT_EQ((unsigned int)(kJobs - kLowPoolSize),
            env_->GetThreadPoolQueueLen());
  ASSERT_EQ((unsigned int)(kJobs - kLowPoolSize),
            env_->GetThreadPoolQueueLen(Env::Priority::LOW));
  ASSERT_EQ((unsigned int)(kJobs - kHighPoolSize),
            env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  // Trigger jobs to run.
  {
    MutexLock l(&mutex);
    should_start = true;
    cv.SignalAll();
  }

  // wait for all jobs to finish
  while (low_pool_job.NumFinished() < kJobs ||
         high_pool_job.NumFinished() < kJobs) {
    env_->SleepForMicroseconds(kDelayMicros);
  }

  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::LOW));
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  // Hold jobs to schedule;
  should_start = false;

  // call IncBackgroundThreadsIfNeeded to two pools. One increasing and
  // the other decreasing
  env_->IncBackgroundThreadsIfNeeded(kLowPoolSize - 1, Env::Priority::LOW);
  env_->IncBackgroundThreadsIfNeeded(kHighPoolSize + 1, Env::Priority::HIGH);
  high_pool_job.Reset(kHighPoolSize + 1);
  low_pool_job.Reset(kLowPoolSize);

  // schedule same number of jobs in each pool
  for (int i = 0; i < kJobs; i++) {
    env_->Schedule(&CB::Run, &low_pool_job);
    env_->Schedule(&CB::Run, &high_pool_job, Env::Priority::HIGH);
  }
  // Wait a short while for the jobs to be dispatched.
  sleep_count = 0;
  while ((unsigned int)(kJobs - kLowPoolSize) !=
             env_->GetThreadPoolQueueLen(Env::Priority::LOW) ||
         (unsigned int)(kJobs - (kHighPoolSize + 1)) !=
             env_->GetThreadPoolQueueLen(Env::Priority::HIGH)) {
    env_->SleepForMicroseconds(kDelayMicros);
    if (++sleep_count > 100) {
      break;
    }
  }
  ASSERT_EQ((unsigned int)(kJobs - kLowPoolSize),
            env_->GetThreadPoolQueueLen());
  ASSERT_EQ((unsigned int)(kJobs - kLowPoolSize),
            env_->GetThreadPoolQueueLen(Env::Priority::LOW));
  ASSERT_EQ((unsigned int)(kJobs - (kHighPoolSize + 1)),
            env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  // Trigger jobs to run.
  {
    MutexLock l(&mutex);
    should_start = true;
    cv.SignalAll();
  }

  // wait for all jobs to finish
  while (low_pool_job.NumFinished() < kJobs ||
         high_pool_job.NumFinished() < kJobs) {
    env_->SleepForMicroseconds(kDelayMicros);
  }

  env_->SetBackgroundThreads(kHighPoolSize, Env::Priority::HIGH);
  WaitThreadPoolsEmpty();
}

TEST_P(EnvPosixTestWithParam, DecreaseNumBgThreads) {
  std::vector<test::SleepingBackgroundTask> tasks(10);

  // Set number of thread to 1 first.
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);

  // Schedule 3 tasks. 0 running; Task 1, 2 waiting.
  for (size_t i = 0; i < 3; i++) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[i],
                   Env::Priority::HIGH);
    Env::Default()->SleepForMicroseconds(kDelayMicros);
  }
  ASSERT_EQ(2U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(!tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());

  // Increase to 2 threads. Task 0, 1 running; 2 waiting
  env_->SetBackgroundThreads(2, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());

  // Shrink back to 1 thread. Still task 0, 1 running, 2 waiting
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());

  // The last task finishes. Task 0 running, 2 waiting.
  tasks[1].WakeUp();
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(!tasks[1].IsSleeping());
  ASSERT_TRUE(!tasks[2].IsSleeping());

  // Increase to 5 threads. Task 0 and 2 running.
  env_->SetBackgroundThreads(5, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ((unsigned int)0, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[0].IsSleeping());
  ASSERT_TRUE(tasks[2].IsSleeping());

  // Change number of threads a couple of times while there is no sufficient
  // tasks.
  env_->SetBackgroundThreads(7, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  tasks[2].WakeUp();
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  env_->SetBackgroundThreads(3, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  env_->SetBackgroundThreads(4, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  env_->SetBackgroundThreads(5, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  env_->SetBackgroundThreads(4, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(0U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));

  Env::Default()->SleepForMicroseconds(kDelayMicros * 50);

  // Enqueue 5 more tasks. Thread pool size now is 4.
  // Task 0, 3, 4, 5 running;6, 7 waiting.
  for (size_t i = 3; i < 8; i++) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[i],
                   Env::Priority::HIGH);
  }
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ(2U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[3].IsSleeping());
  ASSERT_TRUE(tasks[4].IsSleeping());
  ASSERT_TRUE(tasks[5].IsSleeping());
  ASSERT_TRUE(!tasks[6].IsSleeping());
  ASSERT_TRUE(!tasks[7].IsSleeping());

  // Wake up task 0, 3 and 4. Task 5, 6, 7 running.
  tasks[0].WakeUp();
  tasks[3].WakeUp();
  tasks[4].WakeUp();

  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ((unsigned int)0, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  for (size_t i = 5; i < 8; i++) {
    ASSERT_TRUE(tasks[i].IsSleeping());
  }

  // Shrink back to 1 thread. Still task 5, 6, 7 running
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(tasks[5].IsSleeping());
  ASSERT_TRUE(tasks[6].IsSleeping());
  ASSERT_TRUE(tasks[7].IsSleeping());

  // Wake up task  6. Task 5, 7 running
  tasks[6].WakeUp();
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(tasks[5].IsSleeping());
  ASSERT_TRUE(!tasks[6].IsSleeping());
  ASSERT_TRUE(tasks[7].IsSleeping());

  // Wake up threads 7. Task 5 running
  tasks[7].WakeUp();
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(!tasks[7].IsSleeping());

  // Enqueue thread 8 and 9. Task 5 running; one of 8, 9 might be running.
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[8],
                 Env::Priority::HIGH);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &tasks[9],
                 Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_GT(env_->GetThreadPoolQueueLen(Env::Priority::HIGH), (unsigned int)0);
  ASSERT_TRUE(!tasks[8].IsSleeping() || !tasks[9].IsSleeping());

  // Increase to 4 threads. Task 5, 8, 9 running.
  env_->SetBackgroundThreads(4, Env::Priority::HIGH);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_EQ((unsigned int)0, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
  ASSERT_TRUE(tasks[8].IsSleeping());
  ASSERT_TRUE(tasks[9].IsSleeping());

  // Shrink to 1 thread
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);

  // Wake up thread 9.
  tasks[9].WakeUp();
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(!tasks[9].IsSleeping());
  ASSERT_TRUE(tasks[8].IsSleeping());

  // Wake up thread 8
  tasks[8].WakeUp();
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(!tasks[8].IsSleeping());

  // Wake up the last thread
  tasks[5].WakeUp();

  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(!tasks[5].IsSleeping());
  WaitThreadPoolsEmpty();
}

#if (defined OS_LINUX || defined OS_WIN)
// Travis doesn't support fallocate or getting unique ID from files for whatever
// reason.
#ifndef TRAVIS

namespace {
bool IsSingleVarint(const std::string& s) {
  Slice slice(s);

  uint64_t v;
  if (!GetVarint64(&slice, &v)) {
    return false;
  }

  return slice.size() == 0;
}

bool IsUniqueIDValid(const std::string& s) {
  return !s.empty() && !IsSingleVarint(s);
}

const size_t MAX_ID_SIZE = 100;
char temp_id[MAX_ID_SIZE];


}  // namespace

// Determine whether we can use the FS_IOC_GETVERSION ioctl
// on a file in directory DIR.  Create a temporary file therein,
// try to apply the ioctl (save that result), cleanup and
// return the result.  Return true if it is supported, and
// false if anything fails.
// Note that this function "knows" that dir has just been created
// and is empty, so we create a simply-named test file: "f".
bool ioctl_support__FS_IOC_GETVERSION(const std::string& dir) {
#ifdef OS_WIN
  return true;
#else
  const std::string file = dir + "/f";
  int fd;
  do {
    fd = open(file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
  } while (fd < 0 && errno == EINTR);
  long int version;
  bool ok = (fd >= 0 && ioctl(fd, FS_IOC_GETVERSION, &version) >= 0);

  close(fd);
  unlink(file.c_str());

  return ok;
#endif
}

// To ensure that Env::GetUniqueId-related tests work correctly, the files
// should be stored in regular storage like "hard disk" or "flash device",
// and not on a tmpfs file system (like /dev/shm and /tmp on some systems).
// Otherwise we cannot get the correct id.
//
// This function serves as the replacement for test::TmpDir(), which may be
// customized to be on a file system that doesn't work with GetUniqueId().

class IoctlFriendlyTmpdir {
 public:
  explicit IoctlFriendlyTmpdir() {
    char dir_buf[100];

    const char *fmt = "%s/rocksdb.XXXXXX";
    const char *tmp = getenv("TEST_IOCTL_FRIENDLY_TMPDIR");

#ifdef OS_WIN
#define rmdir _rmdir
    if(tmp == nullptr) {
      tmp = getenv("TMP");
    }

    snprintf(dir_buf, sizeof dir_buf, fmt, tmp);
    auto result = _mktemp(dir_buf);
    assert(result != nullptr);
    BOOL ret = CreateDirectory(dir_buf, NULL);
    assert(ret == TRUE);
    dir_ = dir_buf;
#else
    std::list<std::string> candidate_dir_list = {"/var/tmp", "/tmp"};

    // If $TEST_IOCTL_FRIENDLY_TMPDIR/rocksdb.XXXXXX fits, use
    // $TEST_IOCTL_FRIENDLY_TMPDIR; subtract 2 for the "%s", and
    // add 1 for the trailing NUL byte.
    if (tmp && strlen(tmp) + strlen(fmt) - 2 + 1 <= sizeof dir_buf) {
      // use $TEST_IOCTL_FRIENDLY_TMPDIR value
      candidate_dir_list.push_front(tmp);
    }

    for (const std::string& d : candidate_dir_list) {
      snprintf(dir_buf, sizeof dir_buf, fmt, d.c_str());
      if (mkdtemp(dir_buf)) {
        if (ioctl_support__FS_IOC_GETVERSION(dir_buf)) {
          dir_ = dir_buf;
          return;
        } else {
          // Diagnose ioctl-related failure only if this is the
          // directory specified via that envvar.
          if (tmp && tmp == d) {
            fprintf(stderr, "TEST_IOCTL_FRIENDLY_TMPDIR-specified directory is "
                    "not suitable: %s\n", d.c_str());
          }
          rmdir(dir_buf);  // ignore failure
        }
      } else {
        // mkdtemp failed: diagnose it, but don't give up.
        fprintf(stderr, "mkdtemp(%s/...) failed: %s\n", d.c_str(),
                strerror(errno));
      }
    }

    fprintf(stderr, "failed to find an ioctl-friendly temporary directory;"
            " specify one via the TEST_IOCTL_FRIENDLY_TMPDIR envvar\n");
    std::abort();
#endif
}

  ~IoctlFriendlyTmpdir() {
    rmdir(dir_.c_str());
  }

  const std::string& name() const {
    return dir_;
  }

 private:
  std::string dir_;
};

#ifndef ROCKSDB_LITE
TEST_F(EnvPosixTest, PositionedAppend) {
  std::unique_ptr<WritableFile> writable_file;
  EnvOptions options;
  options.use_direct_writes = true;
  options.use_mmap_writes = false;
  IoctlFriendlyTmpdir ift;
  ASSERT_OK(env_->NewWritableFile(ift.name() + "/f", &writable_file, options));
  const size_t kBlockSize = 4096;
  const size_t kDataSize = kPageSize;
  // Write a page worth of 'a'
  auto data_ptr = NewAligned(kDataSize, 'a');
  Slice data_a(data_ptr.get(), kDataSize);
  ASSERT_OK(writable_file->PositionedAppend(data_a, 0U));
  // Write a page worth of 'b' right after the first sector
  data_ptr = NewAligned(kDataSize, 'b');
  Slice data_b(data_ptr.get(), kDataSize);
  ASSERT_OK(writable_file->PositionedAppend(data_b, kBlockSize));
  ASSERT_OK(writable_file->Close());
  // The file now has 1 sector worth of a followed by a page worth of b

  // Verify the above
  std::unique_ptr<SequentialFile> seq_file;
  ASSERT_OK(env_->NewSequentialFile(ift.name() + "/f", &seq_file, options));
  char scratch[kPageSize * 2];
  Slice result;
  ASSERT_OK(seq_file->Read(sizeof(scratch), &result, scratch));
  ASSERT_EQ(kPageSize + kBlockSize, result.size());
  ASSERT_EQ('a', result[kBlockSize - 1]);
  ASSERT_EQ('b', result[kBlockSize]);
}
#endif  // !ROCKSDB_LITE

// Only works in linux platforms
TEST_P(EnvPosixTestWithParam, RandomAccessUniqueID) {
  // Create file.
  if (env_ == Env::Default()) {
    EnvOptions soptions;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
    IoctlFriendlyTmpdir ift;
    std::string fname = ift.name() + "/testfile";
    std::unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));

    std::unique_ptr<RandomAccessFile> file;

    // Get Unique ID
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    size_t id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
    ASSERT_TRUE(id_size > 0);
    std::string unique_id1(temp_id, id_size);
    ASSERT_TRUE(IsUniqueIDValid(unique_id1));

    // Get Unique ID again
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
    ASSERT_TRUE(id_size > 0);
    std::string unique_id2(temp_id, id_size);
    ASSERT_TRUE(IsUniqueIDValid(unique_id2));

    // Get Unique ID again after waiting some time.
    env_->SleepForMicroseconds(1000000);
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
    id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
    ASSERT_TRUE(id_size > 0);
    std::string unique_id3(temp_id, id_size);
    ASSERT_TRUE(IsUniqueIDValid(unique_id3));

    // Check IDs are the same.
    ASSERT_EQ(unique_id1, unique_id2);
    ASSERT_EQ(unique_id2, unique_id3);

    // Delete the file
    env_->DeleteFile(fname);
  }
}

// only works in linux platforms
#ifdef ROCKSDB_FALLOCATE_PRESENT
TEST_P(EnvPosixTestWithParam, AllocateTest) {
  if (env_ == Env::Default()) {
    IoctlFriendlyTmpdir ift;
    std::string fname = ift.name() + "/preallocate_testfile";

    // Try fallocate in a file to see whether the target file system supports
    // it.
    // Skip the test if fallocate is not supported.
    std::string fname_test_fallocate = ift.name() + "/preallocate_testfile_2";
    int fd = -1;
    do {
      fd = open(fname_test_fallocate.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    } while (fd < 0 && errno == EINTR);
    ASSERT_GT(fd, 0);

    int alloc_status = fallocate(fd, 0, 0, 1);

    int err_number = 0;
    if (alloc_status != 0) {
      err_number = errno;
      fprintf(stderr, "Warning: fallocate() fails, %s\n", strerror(err_number));
    }
    close(fd);
    ASSERT_OK(env_->DeleteFile(fname_test_fallocate));
    if (alloc_status != 0 && err_number == EOPNOTSUPP) {
      // The filesystem containing the file does not support fallocate
      return;
    }

    EnvOptions soptions;
    soptions.use_mmap_writes = false;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
    std::unique_ptr<WritableFile> wfile;
    ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));

    // allocate 100 MB
    size_t kPreallocateSize = 100 * 1024 * 1024;
    size_t kBlockSize = 512;
    size_t kPageSize = 4096;
    size_t kDataSize = 1024 * 1024;
    auto data_ptr = NewAligned(kDataSize, 'A');
    Slice data(data_ptr.get(), kDataSize);
    wfile->SetPreallocationBlockSize(kPreallocateSize);
    wfile->PrepareWrite(wfile->GetFileSize(), kDataSize);
    ASSERT_OK(wfile->Append(data));
    ASSERT_OK(wfile->Flush());

    struct stat f_stat;
    ASSERT_EQ(stat(fname.c_str(), &f_stat), 0);
    ASSERT_EQ((unsigned int)kDataSize, f_stat.st_size);
    // verify that blocks are preallocated
    // Note here that we don't check the exact number of blocks preallocated --
    // we only require that number of allocated blocks is at least what we
    // expect.
    // It looks like some FS give us more blocks that we asked for. That's fine.
    // It might be worth investigating further.
    ASSERT_LE((unsigned int)(kPreallocateSize / kBlockSize), f_stat.st_blocks);

    // close the file, should deallocate the blocks
    wfile.reset();

    stat(fname.c_str(), &f_stat);
    ASSERT_EQ((unsigned int)kDataSize, f_stat.st_size);
    // verify that preallocated blocks were deallocated on file close
    // Because the FS might give us more blocks, we add a full page to the size
    // and expect the number of blocks to be less or equal to that.
    ASSERT_GE((f_stat.st_size + kPageSize + kBlockSize - 1) / kBlockSize,
              (unsigned int)f_stat.st_blocks);
  }
}
#endif  // ROCKSDB_FALLOCATE_PRESENT

// Returns true if any of the strings in ss are the prefix of another string.
bool HasPrefix(const std::unordered_set<std::string>& ss) {
  for (const std::string& s: ss) {
    if (s.empty()) {
      return true;
    }
    for (size_t i = 1; i < s.size(); ++i) {
      if (ss.count(s.substr(0, i)) != 0) {
        return true;
      }
    }
  }
  return false;
}

// Only works in linux and WIN platforms
TEST_P(EnvPosixTestWithParam, RandomAccessUniqueIDConcurrent) {
  if (env_ == Env::Default()) {
    // Check whether a bunch of concurrently existing files have unique IDs.
    EnvOptions soptions;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;

    // Create the files
    IoctlFriendlyTmpdir ift;
    std::vector<std::string> fnames;
    for (int i = 0; i < 1000; ++i) {
      fnames.push_back(ift.name() + "/" + "testfile" + ToString(i));

      // Create file.
      std::unique_ptr<WritableFile> wfile;
      ASSERT_OK(env_->NewWritableFile(fnames[i], &wfile, soptions));
    }

    // Collect and check whether the IDs are unique.
    std::unordered_set<std::string> ids;
    for (const std::string fname : fnames) {
      std::unique_ptr<RandomAccessFile> file;
      std::string unique_id;
      ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
      size_t id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
      ASSERT_TRUE(id_size > 0);
      unique_id = std::string(temp_id, id_size);
      ASSERT_TRUE(IsUniqueIDValid(unique_id));

      ASSERT_TRUE(ids.count(unique_id) == 0);
      ids.insert(unique_id);
    }

    // Delete the files
    for (const std::string fname : fnames) {
      ASSERT_OK(env_->DeleteFile(fname));
    }

    ASSERT_TRUE(!HasPrefix(ids));
  }
}

// Only works in linux and WIN platforms
TEST_P(EnvPosixTestWithParam, RandomAccessUniqueIDDeletes) {
  if (env_ == Env::Default()) {
    EnvOptions soptions;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;

    IoctlFriendlyTmpdir ift;
    std::string fname = ift.name() + "/" + "testfile";

    // Check that after file is deleted we don't get same ID again in a new
    // file.
    std::unordered_set<std::string> ids;
    for (int i = 0; i < 1000; ++i) {
      // Create file.
      {
        std::unique_ptr<WritableFile> wfile;
        ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));
      }

      // Get Unique ID
      std::string unique_id;
      {
        std::unique_ptr<RandomAccessFile> file;
        ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
        size_t id_size = file->GetUniqueId(temp_id, MAX_ID_SIZE);
        ASSERT_TRUE(id_size > 0);
        unique_id = std::string(temp_id, id_size);
      }

      ASSERT_TRUE(IsUniqueIDValid(unique_id));
      ASSERT_TRUE(ids.count(unique_id) == 0);
      ids.insert(unique_id);

      // Delete the file
      ASSERT_OK(env_->DeleteFile(fname));
    }

    ASSERT_TRUE(!HasPrefix(ids));
  }
}

// Only works in linux platforms
#ifdef OS_WIN
TEST_P(EnvPosixTestWithParam, DISABLED_InvalidateCache) {
#else
TEST_P(EnvPosixTestWithParam, InvalidateCache) {
#endif
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    EnvOptions soptions;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
    std::string fname = test::PerThreadDBPath(env_, "testfile");

    const size_t kSectorSize = 512;
    auto data = NewAligned(kSectorSize, 0);
    Slice slice(data.get(), kSectorSize);

    // Create file.
    {
      std::unique_ptr<WritableFile> wfile;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && !defined(OS_AIX)
      if (soptions.use_direct_writes) {
        soptions.use_direct_writes = false;
      }
#endif
      ASSERT_OK(env_->NewWritableFile(fname, &wfile, soptions));
      ASSERT_OK(wfile->Append(slice));
      ASSERT_OK(wfile->InvalidateCache(0, 0));
      ASSERT_OK(wfile->Close());
    }

    // Random Read
    {
      std::unique_ptr<RandomAccessFile> file;
      auto scratch = NewAligned(kSectorSize, 0);
      Slice result;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && !defined(OS_AIX)
      if (soptions.use_direct_reads) {
        soptions.use_direct_reads = false;
      }
#endif
      ASSERT_OK(env_->NewRandomAccessFile(fname, &file, soptions));
      ASSERT_OK(file->Read(0, kSectorSize, &result, scratch.get()));
      ASSERT_EQ(memcmp(scratch.get(), data.get(), kSectorSize), 0);
      ASSERT_OK(file->InvalidateCache(0, 11));
      ASSERT_OK(file->InvalidateCache(0, 0));
    }

    // Sequential Read
    {
      std::unique_ptr<SequentialFile> file;
      auto scratch = NewAligned(kSectorSize, 0);
      Slice result;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && !defined(OS_AIX)
      if (soptions.use_direct_reads) {
        soptions.use_direct_reads = false;
      }
#endif
      ASSERT_OK(env_->NewSequentialFile(fname, &file, soptions));
      if (file->use_direct_io()) {
        ASSERT_OK(file->PositionedRead(0, kSectorSize, &result, scratch.get()));
      } else {
        ASSERT_OK(file->Read(kSectorSize, &result, scratch.get()));
      }
      ASSERT_EQ(memcmp(scratch.get(), data.get(), kSectorSize), 0);
      ASSERT_OK(file->InvalidateCache(0, 11));
      ASSERT_OK(file->InvalidateCache(0, 0));
    }
    // Delete the file
    ASSERT_OK(env_->DeleteFile(fname));
  rocksdb::SyncPoint::GetInstance()->ClearTrace();
}
#endif  // not TRAVIS
#endif  // OS_LINUX || OS_WIN

class TestLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    log_count++;

    char new_format[550];
    std::fill_n(new_format, sizeof(new_format), '2');
    {
      va_list backup_ap;
      va_copy(backup_ap, ap);
      int n = vsnprintf(new_format, sizeof(new_format) - 1, format, backup_ap);
      // 48 bytes for extra information + bytes allocated

// When we have n == -1 there is not a terminating zero expected
#ifdef OS_WIN
      if (n < 0) {
        char_0_count++;
      }
#endif

      if (new_format[0] == '[') {
        // "[DEBUG] "
        ASSERT_TRUE(n <= 56 + (512 - static_cast<int>(sizeof(struct timeval))));
      } else {
        ASSERT_TRUE(n <= 48 + (512 - static_cast<int>(sizeof(struct timeval))));
      }
      va_end(backup_ap);
    }

    for (size_t i = 0; i < sizeof(new_format); i++) {
      if (new_format[i] == 'x') {
        char_x_count++;
      } else if (new_format[i] == '\0') {
        char_0_count++;
      }
    }
  }
  int log_count;
  int char_x_count;
  int char_0_count;
};

TEST_P(EnvPosixTestWithParam, LogBufferTest) {
  TestLogger test_logger;
  test_logger.SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
  test_logger.log_count = 0;
  test_logger.char_x_count = 0;
  test_logger.char_0_count = 0;
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, &test_logger);
  LogBuffer log_buffer_debug(DEBUG_LEVEL, &test_logger);

  char bytes200[200];
  std::fill_n(bytes200, sizeof(bytes200), '1');
  bytes200[sizeof(bytes200) - 1] = '\0';
  char bytes600[600];
  std::fill_n(bytes600, sizeof(bytes600), '1');
  bytes600[sizeof(bytes600) - 1] = '\0';
  char bytes9000[9000];
  std::fill_n(bytes9000, sizeof(bytes9000), '1');
  bytes9000[sizeof(bytes9000) - 1] = '\0';

  ROCKS_LOG_BUFFER(&log_buffer, "x%sx", bytes200);
  ROCKS_LOG_BUFFER(&log_buffer, "x%sx", bytes600);
  ROCKS_LOG_BUFFER(&log_buffer, "x%sx%sx%sx", bytes200, bytes200, bytes200);
  ROCKS_LOG_BUFFER(&log_buffer, "x%sx%sx", bytes200, bytes600);
  ROCKS_LOG_BUFFER(&log_buffer, "x%sx%sx", bytes600, bytes9000);

  ROCKS_LOG_BUFFER(&log_buffer_debug, "x%sx", bytes200);
  test_logger.SetInfoLogLevel(DEBUG_LEVEL);
  ROCKS_LOG_BUFFER(&log_buffer_debug, "x%sx%sx%sx", bytes600, bytes9000,
                   bytes200);

  ASSERT_EQ(0, test_logger.log_count);
  log_buffer.FlushBufferToLog();
  log_buffer_debug.FlushBufferToLog();
  ASSERT_EQ(6, test_logger.log_count);
  ASSERT_EQ(6, test_logger.char_0_count);
  ASSERT_EQ(10, test_logger.char_x_count);
}

class TestLogger2 : public Logger {
 public:
  explicit TestLogger2(size_t max_log_size) : max_log_size_(max_log_size) {}
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char new_format[2000];
    std::fill_n(new_format, sizeof(new_format), '2');
    {
      va_list backup_ap;
      va_copy(backup_ap, ap);
      int n = vsnprintf(new_format, sizeof(new_format) - 1, format, backup_ap);
      // 48 bytes for extra information + bytes allocated
      ASSERT_TRUE(
          n <= 48 + static_cast<int>(max_log_size_ - sizeof(struct timeval)));
      ASSERT_TRUE(n > static_cast<int>(max_log_size_ - sizeof(struct timeval)));
      va_end(backup_ap);
    }
  }
  size_t max_log_size_;
};

TEST_P(EnvPosixTestWithParam, LogBufferMaxSizeTest) {
  char bytes9000[9000];
  std::fill_n(bytes9000, sizeof(bytes9000), '1');
  bytes9000[sizeof(bytes9000) - 1] = '\0';

  for (size_t max_log_size = 256; max_log_size <= 1024;
       max_log_size += 1024 - 256) {
    TestLogger2 test_logger(max_log_size);
    test_logger.SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, &test_logger);
    ROCKS_LOG_BUFFER_MAX_SZ(&log_buffer, max_log_size, "%s", bytes9000);
    log_buffer.FlushBufferToLog();
  }
}

TEST_P(EnvPosixTestWithParam, Preallocation) {
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  const std::string src = test::PerThreadDBPath(env_, "testfile");
  std::unique_ptr<WritableFile> srcfile;
  EnvOptions soptions;
  soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && !defined(OS_AIX) && !defined(OS_OPENBSD) && !defined(OS_FREEBSD)
    if (soptions.use_direct_writes) {
      rocksdb::SyncPoint::GetInstance()->SetCallBack(
          "NewWritableFile:O_DIRECT", [&](void* arg) {
            int* val = static_cast<int*>(arg);
            *val &= ~O_DIRECT;
          });
    }
#endif
    ASSERT_OK(env_->NewWritableFile(src, &srcfile, soptions));
    srcfile->SetPreallocationBlockSize(1024 * 1024);

    // No writes should mean no preallocation
    size_t block_size, last_allocated_block;
    srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
    ASSERT_EQ(last_allocated_block, 0UL);

    // Small write should preallocate one block
    size_t kStrSize = 4096;
    auto data = NewAligned(kStrSize, 'A');
    Slice str(data.get(), kStrSize);
    srcfile->PrepareWrite(srcfile->GetFileSize(), kStrSize);
    srcfile->Append(str);
    srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
    ASSERT_EQ(last_allocated_block, 1UL);

    // Write an entire preallocation block, make sure we increased by two.
    {
      auto buf_ptr = NewAligned(block_size, ' ');
      Slice buf(buf_ptr.get(), block_size);
      srcfile->PrepareWrite(srcfile->GetFileSize(), block_size);
      srcfile->Append(buf);
      srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
      ASSERT_EQ(last_allocated_block, 2UL);
    }

    // Write five more blocks at once, ensure we're where we need to be.
    {
      auto buf_ptr = NewAligned(block_size * 5, ' ');
      Slice buf = Slice(buf_ptr.get(), block_size * 5);
      srcfile->PrepareWrite(srcfile->GetFileSize(), buf.size());
      srcfile->Append(buf);
      srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
      ASSERT_EQ(last_allocated_block, 7UL);
    }
  rocksdb::SyncPoint::GetInstance()->ClearTrace();
}

// Test that the two ways to get children file attributes (in bulk or
// individually) behave consistently.
TEST_P(EnvPosixTestWithParam, ConsistentChildrenAttributes) {
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    EnvOptions soptions;
    soptions.use_direct_reads = soptions.use_direct_writes = direct_io_;
    const int kNumChildren = 10;

    std::string data;
    for (int i = 0; i < kNumChildren; ++i) {
      const std::string path =
          test::TmpDir(env_) + "/" + "testfile_" + std::to_string(i);
      std::unique_ptr<WritableFile> file;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && !defined(OS_AIX) && !defined(OS_OPENBSD) && !defined(OS_FREEBSD)
      if (soptions.use_direct_writes) {
        rocksdb::SyncPoint::GetInstance()->SetCallBack(
            "NewWritableFile:O_DIRECT", [&](void* arg) {
              int* val = static_cast<int*>(arg);
              *val &= ~O_DIRECT;
            });
      }
#endif
      ASSERT_OK(env_->NewWritableFile(path, &file, soptions));
      auto buf_ptr = NewAligned(data.size(), 'T');
      Slice buf(buf_ptr.get(), data.size());
      file->Append(buf);
      data.append(std::string(4096, 'T'));
    }

    std::vector<Env::FileAttributes> file_attrs;
    ASSERT_OK(env_->GetChildrenFileAttributes(test::TmpDir(env_), &file_attrs));
    for (int i = 0; i < kNumChildren; ++i) {
      const std::string name = "testfile_" + std::to_string(i);
      const std::string path = test::TmpDir(env_) + "/" + name;

      auto file_attrs_iter = std::find_if(
          file_attrs.begin(), file_attrs.end(),
          [&name](const Env::FileAttributes& fm) { return fm.name == name; });
      ASSERT_TRUE(file_attrs_iter != file_attrs.end());
      uint64_t size;
      ASSERT_OK(env_->GetFileSize(path, &size));
      ASSERT_EQ(size, 4096 * i);
      ASSERT_EQ(size, file_attrs_iter->size_bytes);
    }
    rocksdb::SyncPoint::GetInstance()->ClearTrace();
}

// Test that all WritableFileWrapper forwards all calls to WritableFile.
TEST_P(EnvPosixTestWithParam, WritableFileWrapper) {
  class Base : public WritableFile {
   public:
    mutable int *step_;

    void inc(int x) const {
      EXPECT_EQ(x, (*step_)++);
    }

    explicit Base(int* step) : step_(step) {
      inc(0);
    }

    Status Append(const Slice& /*data*/) override {
      inc(1);
      return Status::OK();
    }

    Status PositionedAppend(const Slice& /*data*/,
                            uint64_t /*offset*/) override {
      inc(2);
      return Status::OK();
    }

    Status Truncate(uint64_t /*size*/) override {
      inc(3);
      return Status::OK();
    }

    Status Close() override {
      inc(4);
      return Status::OK();
    }

    Status Flush() override {
      inc(5);
      return Status::OK();
    }

    Status Sync() override {
      inc(6);
      return Status::OK();
    }

    Status Fsync() override {
      inc(7);
      return Status::OK();
    }

    bool IsSyncThreadSafe() const override {
      inc(8);
      return true;
    }

    bool use_direct_io() const override {
      inc(9);
      return true;
    }

    size_t GetRequiredBufferAlignment() const override {
      inc(10);
      return 0;
    }

    void SetIOPriority(Env::IOPriority /*pri*/) override { inc(11); }

    Env::IOPriority GetIOPriority() override {
      inc(12);
      return Env::IOPriority::IO_LOW;
    }

    void SetWriteLifeTimeHint(Env::WriteLifeTimeHint /*hint*/) override {
      inc(13);
    }

    Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
      inc(14);
      return Env::WriteLifeTimeHint::WLTH_NOT_SET;
    }

    uint64_t GetFileSize() override {
      inc(15);
      return 0;
    }

    void SetPreallocationBlockSize(size_t /*size*/) override { inc(16); }

    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {
      inc(17);
    }

    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      inc(18);
      return 0;
    }

    Status InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      inc(19);
      return Status::OK();
    }

    Status RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/) override {
      inc(20);
      return Status::OK();
    }

    void PrepareWrite(size_t /*offset*/, size_t /*len*/) override { inc(21); }

    Status Allocate(uint64_t /*offset*/, uint64_t /*len*/) override {
      inc(22);
      return Status::OK();
    }

   public:
    ~Base() override { inc(23); }
  };

  class Wrapper : public WritableFileWrapper {
   public:
    explicit Wrapper(WritableFile* target) : WritableFileWrapper(target) {}
  };

  int step = 0;

  {
    Base b(&step);
    Wrapper w(&b);
    w.Append(Slice());
    w.PositionedAppend(Slice(), 0);
    w.Truncate(0);
    w.Close();
    w.Flush();
    w.Sync();
    w.Fsync();
    w.IsSyncThreadSafe();
    w.use_direct_io();
    w.GetRequiredBufferAlignment();
    w.SetIOPriority(Env::IOPriority::IO_HIGH);
    w.GetIOPriority();
    w.SetWriteLifeTimeHint(Env::WriteLifeTimeHint::WLTH_NOT_SET);
    w.GetWriteLifeTimeHint();
    w.GetFileSize();
    w.SetPreallocationBlockSize(0);
    w.GetPreallocationStatus(nullptr, nullptr);
    w.GetUniqueId(nullptr, 0);
    w.InvalidateCache(0, 0);
    w.RangeSync(0, 0);
    w.PrepareWrite(0, 0);
    w.Allocate(0, 0);
  }

  EXPECT_EQ(24, step);
}

TEST_P(EnvPosixTestWithParam, PosixRandomRWFile) {
  const std::string path = test::PerThreadDBPath(env_, "random_rw_file");

  env_->DeleteFile(path);

  std::unique_ptr<RandomRWFile> file;

  // Cannot open non-existing file.
  ASSERT_NOK(env_->NewRandomRWFile(path, &file, EnvOptions()));

  // Create the file using WriteableFile
  {
    std::unique_ptr<WritableFile> wf;
    ASSERT_OK(env_->NewWritableFile(path, &wf, EnvOptions()));
  }

  ASSERT_OK(env_->NewRandomRWFile(path, &file, EnvOptions()));

  char buf[10000];
  Slice read_res;

  ASSERT_OK(file->Write(0, "ABCD"));
  ASSERT_OK(file->Read(0, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABCD");

  ASSERT_OK(file->Write(2, "XXXX"));
  ASSERT_OK(file->Read(0, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABXXXX");

  ASSERT_OK(file->Write(10, "ZZZ"));
  ASSERT_OK(file->Read(10, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ZZZ");

  ASSERT_OK(file->Write(11, "Y"));
  ASSERT_OK(file->Read(10, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ZYZ");

  ASSERT_OK(file->Write(200, "FFFFF"));
  ASSERT_OK(file->Read(200, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "FFFFF");

  ASSERT_OK(file->Write(205, "XXXX"));
  ASSERT_OK(file->Read(200, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "FFFFFXXXX");

  ASSERT_OK(file->Write(5, "QQQQ"));
  ASSERT_OK(file->Read(0, 9, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABXXXQQQQ");

  ASSERT_OK(file->Read(2, 4, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "XXXQ");

  // Close file and reopen it
  file->Close();
  ASSERT_OK(env_->NewRandomRWFile(path, &file, EnvOptions()));

  ASSERT_OK(file->Read(0, 9, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABXXXQQQQ");

  ASSERT_OK(file->Read(10, 3, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ZYZ");

  ASSERT_OK(file->Read(200, 9, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "FFFFFXXXX");

  ASSERT_OK(file->Write(4, "TTTTTTTTTTTTTTTT"));
  ASSERT_OK(file->Read(0, 10, &read_res, buf));
  ASSERT_EQ(read_res.ToString(), "ABXXTTTTTT");

  // Clean up
  env_->DeleteFile(path);
}

class RandomRWFileWithMirrorString {
 public:
  explicit RandomRWFileWithMirrorString(RandomRWFile* _file) : file_(_file) {}

  void Write(size_t offset, const std::string& data) {
    // Write to mirror string
    StringWrite(offset, data);

    // Write to file
    Status s = file_->Write(offset, data);
    ASSERT_OK(s) << s.ToString();
  }

  void Read(size_t offset = 0, size_t n = 1000000) {
    Slice str_res(nullptr, 0);
    if (offset < file_mirror_.size()) {
      size_t str_res_sz = std::min(file_mirror_.size() - offset, n);
      str_res = Slice(file_mirror_.data() + offset, str_res_sz);
      StopSliceAtNull(&str_res);
    }

    Slice file_res;
    Status s = file_->Read(offset, n, &file_res, buf_);
    ASSERT_OK(s) << s.ToString();
    StopSliceAtNull(&file_res);

    ASSERT_EQ(str_res.ToString(), file_res.ToString()) << offset << " " << n;
  }

  void SetFile(RandomRWFile* _file) { file_ = _file; }

 private:
  void StringWrite(size_t offset, const std::string& src) {
    if (offset + src.size() > file_mirror_.size()) {
      file_mirror_.resize(offset + src.size(), '\0');
    }

    char* pos = const_cast<char*>(file_mirror_.data() + offset);
    memcpy(pos, src.data(), src.size());
  }

  void StopSliceAtNull(Slice* slc) {
    for (size_t i = 0; i < slc->size(); i++) {
      if ((*slc)[i] == '\0') {
        *slc = Slice(slc->data(), i);
        break;
      }
    }
  }

  char buf_[10000];
  RandomRWFile* file_;
  std::string file_mirror_;
};

TEST_P(EnvPosixTestWithParam, PosixRandomRWFileRandomized) {
  const std::string path = test::PerThreadDBPath(env_, "random_rw_file_rand");
  env_->DeleteFile(path);

  std::unique_ptr<RandomRWFile> file;

#ifdef OS_LINUX
  // Cannot open non-existing file.
  ASSERT_NOK(env_->NewRandomRWFile(path, &file, EnvOptions()));
#endif

  // Create the file using WriteableFile
  {
    std::unique_ptr<WritableFile> wf;
    ASSERT_OK(env_->NewWritableFile(path, &wf, EnvOptions()));
  }

  ASSERT_OK(env_->NewRandomRWFile(path, &file, EnvOptions()));
  RandomRWFileWithMirrorString file_with_mirror(file.get());

  Random rnd(301);
  std::string buf;
  for (int i = 0; i < 10000; i++) {
    // Genrate random data
    test::RandomString(&rnd, 10, &buf);

    // Pick random offset for write
    size_t write_off = rnd.Next() % 1000;
    file_with_mirror.Write(write_off, buf);

    // Pick random offset for read
    size_t read_off = rnd.Next() % 1000;
    size_t read_sz = rnd.Next() % 20;
    file_with_mirror.Read(read_off, read_sz);

    if (i % 500 == 0) {
      // Reopen the file every 500 iters
      ASSERT_OK(env_->NewRandomRWFile(path, &file, EnvOptions()));
      file_with_mirror.SetFile(file.get());
    }
  }

  // clean up
  env_->DeleteFile(path);
}

class TestEnv : public EnvWrapper {
  public:
    explicit TestEnv() : EnvWrapper(Env::Default()),
                close_count(0) { }

  class TestLogger : public Logger {
   public:
    using Logger::Logv;
    TestLogger(TestEnv* env_ptr) : Logger() { env = env_ptr; }
    ~TestLogger() override {
      if (!closed_) {
        CloseHelper();
      }
    }
    void Logv(const char* /*format*/, va_list /*ap*/) override{};

   protected:
    Status CloseImpl() override { return CloseHelper(); }

   private:
    Status CloseHelper() {
      env->CloseCountInc();;
      return Status::OK();
    }
    TestEnv* env;
  };

  void CloseCountInc() { close_count++; }

  int GetCloseCount() { return close_count; }

  Status NewLogger(const std::string& /*fname*/,
                   std::shared_ptr<Logger>* result) override {
    result->reset(new TestLogger(this));
    return Status::OK();
  }

 private:
  int close_count;
};

class EnvTest : public testing::Test {};

TEST_F(EnvTest, Close) {
  TestEnv* env = new TestEnv();
  std::shared_ptr<Logger> logger;
  Status s;

  s = env->NewLogger("", &logger);
  ASSERT_EQ(s, Status::OK());
  logger.get()->Close();
  ASSERT_EQ(env->GetCloseCount(), 1);
  // Call Close() again. CloseHelper() should not be called again
  logger.get()->Close();
  ASSERT_EQ(env->GetCloseCount(), 1);
  logger.reset();
  ASSERT_EQ(env->GetCloseCount(), 1);

  s = env->NewLogger("", &logger);
  ASSERT_EQ(s, Status::OK());
  logger.reset();
  ASSERT_EQ(env->GetCloseCount(), 2);

  delete env;
}

INSTANTIATE_TEST_CASE_P(DefaultEnvWithoutDirectIO, EnvPosixTestWithParam,
                        ::testing::Values(std::pair<Env*, bool>(Env::Default(),
                                                                false)));
#if !defined(ROCKSDB_LITE)
INSTANTIATE_TEST_CASE_P(DefaultEnvWithDirectIO, EnvPosixTestWithParam,
                        ::testing::Values(std::pair<Env*, bool>(Env::Default(),
                                                                true)));
#endif  // !defined(ROCKSDB_LITE)

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)
static std::unique_ptr<Env> chroot_env(
    NewChrootEnv(Env::Default(), test::TmpDir(Env::Default())));
INSTANTIATE_TEST_CASE_P(
    ChrootEnvWithoutDirectIO, EnvPosixTestWithParam,
    ::testing::Values(std::pair<Env*, bool>(chroot_env.get(), false)));
INSTANTIATE_TEST_CASE_P(
    ChrootEnvWithDirectIO, EnvPosixTestWithParam,
    ::testing::Values(std::pair<Env*, bool>(chroot_env.get(), true)));
#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
