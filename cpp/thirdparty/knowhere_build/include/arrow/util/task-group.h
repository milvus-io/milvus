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

#ifndef ARROW_UTIL_TASK_GROUP_H
#define ARROW_UTIL_TASK_GROUP_H

#include <functional>
#include <memory>
#include <utility>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

class ThreadPool;

// TODO Simplify this.  Subgroups don't seem necessary.

/// \brief A group of related tasks
///
/// A TaskGroup executes tasks with the signature `Status()`.
/// Execution can be serial or parallel, depending on the TaskGroup
/// implementation.  When Finish() returns, it is guaranteed that all
/// tasks have finished, or at least one has errored.
///
class ARROW_EXPORT TaskGroup {
 public:
  /// Add a Status-returning function to execute.  Execution order is
  /// undefined.  The function may be executed immediately or later.
  template <typename Function>
  void Append(Function&& func) {
    return AppendReal(std::forward<Function>(func));
  }

  /// Wait for execution of all tasks (and subgroups) to be finished,
  /// or for at least one task (or subgroup) to error out.
  /// The returned Status propagates the error status of the first failing
  /// task (or subgroup).
  virtual Status Finish() = 0;

  /// The current agregate error Status.  Non-blocking, useful for stopping early.
  virtual Status current_status() = 0;

  /// Whether some tasks have already failed.  Non-blocking , useful for stopping early.
  virtual bool ok() = 0;

  /// How many tasks can typically be executed in parallel.
  /// This is only a hint, useful for testing or debugging.
  virtual int parallelism() = 0;

  /// Create a subgroup of this group.  This group can only finish
  /// when all subgroups have finished (this means you must be
  /// be careful to call Finish() on subgroups before calling it
  /// on the main group).
  // XXX if a subgroup errors out, should it propagate immediately to the parent
  // and to children?
  virtual std::shared_ptr<TaskGroup> MakeSubGroup() = 0;

  static std::shared_ptr<TaskGroup> MakeSerial();
  static std::shared_ptr<TaskGroup> MakeThreaded(internal::ThreadPool*);

  virtual ~TaskGroup() = default;

 protected:
  TaskGroup() = default;
  ARROW_DISALLOW_COPY_AND_ASSIGN(TaskGroup);

  virtual void AppendReal(std::function<Status()> task) = 0;
};

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_TASK_GROUP_H
