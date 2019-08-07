// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_UTIL_THREAD_POOL_H
#define ARROW_UTIL_THREAD_POOL_H

#ifndef _WIN32
#include <unistd.h>
#endif

#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \brief Get the capacity of the global thread pool
///
/// Return the number of worker threads in the thread pool to which
/// Arrow dispatches various CPU-bound tasks.  This is an ideal number,
/// not necessarily the exact number of threads at a given point in time.
///
/// You can change this number using SetCpuThreadPoolCapacity().
ARROW_EXPORT int GetCpuThreadPoolCapacity();

/// \brief Set the capacity of the global thread pool
///
/// Set the number of worker threads int the thread pool to which
/// Arrow dispatches various CPU-bound tasks.
///
/// The current number is returned by GetCpuThreadPoolCapacity().
ARROW_EXPORT Status SetCpuThreadPoolCapacity(int threads);

namespace internal {

namespace detail {

// Needed because std::packaged_task is not copyable and hence not convertible
// to std::function.
template <typename R, typename... Args>
struct packaged_task_wrapper {
  using PackagedTask = std::packaged_task<R(Args...)>;

  explicit packaged_task_wrapper(PackagedTask&& task)
      : task_(std::make_shared<PackagedTask>(std::forward<PackagedTask>(task))) {}

  void operator()(Args&&... args) { return (*task_)(std::forward<Args>(args)...); }
  std::shared_ptr<PackagedTask> task_;
};

}  // namespace detail

class ARROW_EXPORT ThreadPool {
 public:
  // Construct a thread pool with the given number of worker threads
  static Status Make(int threads, std::shared_ptr<ThreadPool>* out);

  // Destroy thread pool; the pool will first be shut down
  ~ThreadPool();

  // Return the desired number of worker threads.
  // The actual number of workers may lag a bit before being adjusted to
  // match this value.
  int GetCapacity();

  // Dynamically change the number of worker threads.
  // This function returns quickly, but it may take more time before the
  // thread count is fully adjusted.
  Status SetCapacity(int threads);

  // Heuristic for the default capacity of a thread pool for CPU-bound tasks.
  // This is exposed as a static method to help with testing.
  static int DefaultCapacity();

  // Shutdown the pool.  Once the pool starts shutting down, new tasks
  // cannot be submitted anymore.
  // If "wait" is true, shutdown waits for all pending tasks to be finished.
  // If "wait" is false, workers are stopped as soon as currently executing
  // tasks are finished.
  Status Shutdown(bool wait = true);

  // Spawn a fire-and-forget task on one of the workers.
  template <typename Function>
  Status Spawn(Function&& func) {
    return SpawnReal(std::forward<Function>(func));
  }

  // Submit a callable and arguments for execution.  Return a future that
  // will return the callable's result value once.
  // The callable's arguments are copied before execution.
  // Since the function is variadic and needs to return a result (the future),
  // an exception is raised if the task fails spawning (which currently
  // only occurs if the ThreadPool is shutting down).
  template <typename Function, typename... Args,
            typename Result = typename std::result_of<Function && (Args && ...)>::type>
  std::future<Result> Submit(Function&& func, Args&&... args) {
    // Trying to templatize std::packaged_task with Function doesn't seem
    // to work, so go through std::bind to simplify the packaged signature
    using PackagedTask = std::packaged_task<Result()>;
    auto task = PackagedTask(std::bind(std::forward<Function>(func), args...));
    auto fut = task.get_future();

    Status st = SpawnReal(detail::packaged_task_wrapper<Result>(std::move(task)));
    if (!st.ok()) {
      st.Abort("ThreadPool::Submit() was probably called after Shutdown()");
    }
    return fut;
  }

  struct State;

 protected:
  FRIEND_TEST(TestThreadPool, SetCapacity);
  FRIEND_TEST(TestGlobalThreadPool, Capacity);
  friend ARROW_EXPORT ThreadPool* GetCpuThreadPool();

  ThreadPool();

  ARROW_DISALLOW_COPY_AND_ASSIGN(ThreadPool);

  Status SpawnReal(std::function<void()> task);
  // Collect finished worker threads, making sure the OS threads have exited
  void CollectFinishedWorkersUnlocked();
  // Launch a given number of additional workers
  void LaunchWorkersUnlocked(int threads);
  // Get the current actual capacity
  int GetActualCapacity();
  // Reinitialize the thread pool if the pid changed
  void ProtectAgainstFork();

  static std::shared_ptr<ThreadPool> MakeCpuThreadPool();

  std::shared_ptr<State> sp_state_;
  State* state_;
  bool shutdown_on_destroy_;
#ifndef _WIN32
  pid_t pid_;
#endif
};

// Return the process-global thread pool for CPU-bound tasks.
ARROW_EXPORT ThreadPool* GetCpuThreadPool();

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_THREAD_POOL_H
