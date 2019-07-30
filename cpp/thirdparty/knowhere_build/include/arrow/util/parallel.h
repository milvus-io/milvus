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

#ifndef ARROW_UTIL_PARALLEL_H
#define ARROW_UTIL_PARALLEL_H

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/thread-pool.h"

namespace arrow {
namespace internal {

// A parallelizer that takes a `Status(int)` function and calls it with
// arguments between 0 and `num_tasks - 1`, on an arbitrary number of threads.

template <class FUNCTION>
Status ParallelFor(int num_tasks, FUNCTION&& func) {
  auto pool = internal::GetCpuThreadPool();
  std::vector<std::future<Status>> futures(num_tasks);

  for (int i = 0; i < num_tasks; ++i) {
    futures[i] = pool->Submit(func, i);
  }
  auto st = Status::OK();
  for (auto& fut : futures) {
    st &= fut.get();
  }
  return st;
}

// A variant of ParallelFor() with an explicit number of dedicated threads.
// In most cases it's more appropriate to use the 2-argument ParallelFor (above),
// or directly the global CPU thread pool (arrow/util/thread-pool.h).

template <class FUNCTION>
Status ParallelFor(int nthreads, int num_tasks, FUNCTION&& func) {
  std::vector<std::thread> thread_pool;
  thread_pool.reserve(nthreads);
  std::atomic<int> task_counter(0);

  std::mutex error_mtx;
  bool error_occurred = false;
  Status error;

  for (int thread_id = 0; thread_id < nthreads; ++thread_id) {
    thread_pool.emplace_back(
        [&num_tasks, &task_counter, &error, &error_occurred, &error_mtx, &func]() {
          int task_id;
          while (!error_occurred) {
            task_id = task_counter.fetch_add(1);
            if (task_id >= num_tasks) {
              break;
            }
            Status s = func(task_id);
            if (!s.ok()) {
              std::lock_guard<std::mutex> lock(error_mtx);
              error_occurred = true;
              error = s;
              break;
            }
          }
        });
  }
  for (auto&& thread : thread_pool) {
    thread.join();
  }
  if (error_occurred) {
    return error;
  }
  return Status::OK();
}

}  // namespace internal
}  // namespace arrow

#endif
