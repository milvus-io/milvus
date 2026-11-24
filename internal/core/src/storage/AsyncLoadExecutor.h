// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>

#include "folly/Executor.h"
#include "pb/common.pb.h"

namespace milvus::storage {

// Configures workers without creating an unused executor. Positive values only.
// Resizes the existing pool in place; call from a configuration thread because
// shrinking may wait for running workers. Existing tasks and keep-alives survive.
void
SetAsyncLoadThreadPoolSize(int threads);

// Returns the configured worker limit, including before first executor use.
[[nodiscard]] int
GetAsyncLoadThreadPoolSize();

// Sets CPU task-count and admission request-count weights in URGENT/HIGH/LOW
// order without creating the pool. Both schedulers reset their quota rounds.
// Non-positive values restore 32:8:1. Existing queues start a fresh quota round.
void
SetAsyncLoadSchedulingWeights(int urgent, int high, int low);

// Reads the immutable sequence bound to a coroutine's current executor; 0 if absent.
[[nodiscard]] uint64_t
GetAsyncLoadSequence(const folly::Executor* executor) noexcept;

// Local CPU and admission classes; the wire enum remains High/Low.
enum class AsyncLoadPriority { Urgent, High, Low };

// Preserves a local Urgent binding, including on custom executors. Otherwise
// respects the explicit wire High/Low argument, including deliberate demotion.
// Use the AsyncLoadPriority overload to explicitly demote a bound Urgent task.
[[nodiscard]] AsyncLoadPriority
ResolveAsyncLoadPriority(const folly::Executor* executor,
                         milvus::proto::common::LoadPriority fallback) noexcept;

// Binds priority and sequence_id to every submission/resumption. The default pool
// uses configured URGENT:HIGH:LOW task quotas (default 32:8:1), skipping empty classes.
// Within a class: sequence_id, then FIFO; 0 follows numbered loads.
// Custom executors keep their own queue discipline but retain the bound sequence.
// An already bound executor retains its sequence when sequence_id is omitted.
// Quotas count task reservations, not CPU time; running tasks are not preempted.
// Default Linux workers use nice 19 independently of their task priority.
[[nodiscard]] folly::Executor::KeepAlive<>
ResolveAsyncLoadExecutor(folly::Executor::KeepAlive<> executor,
                         AsyncLoadPriority priority,
                         uint64_t sequence_id = 0);

// Existing wire priorities map to High/Low; this does not promote Search.
[[nodiscard]] folly::Executor::KeepAlive<>
ResolveAsyncLoadExecutor(folly::Executor::KeepAlive<> executor,
                         milvus::proto::common::LoadPriority load_priority,
                         uint64_t sequence_id = 0);

}  // namespace milvus::storage
