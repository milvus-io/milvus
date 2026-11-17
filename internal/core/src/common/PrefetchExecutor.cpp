// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/PrefetchExecutor.h"

#include <folly/ScopeGuard.h>

namespace milvus {
namespace {
thread_local const folly::CancellationToken* worker_stop = nullptr;

struct PendingPrefetch {
    folly::Function<void()> work;
    folly::Promise<folly::Unit> done;

    void
    Discard() {
        work = nullptr;
        done.setValue(folly::unit);
    }
};
}  // namespace

bool
PrefetchExecutor::IsStopped() {
    return worker_stop && worker_stop->isCancellationRequested();
}

folly::Future<folly::Unit>
PrefetchExecutor::Submit(folly::Function<void()> work) const {
    if (stop_.isCancellationRequested()) {
        return folly::makeFuture();
    }
    auto job = std::make_shared<PendingPrefetch>();
    job->work = std::move(work);
    auto future = job->done.getFuture();
    auto on_stop = std::make_unique<folly::CancellationCallback>(
        stop_, [job] { job->Discard(); });

    pool_->add([job, stop = stop_, on_stop = std::move(on_stop)]() mutable {
        // Unregistration synchronizes with Discard. Only the worker can
        // claim the callable after reset returns.
        on_stop.reset();
        if (!job->work) {
            return;
        }
        if (stop.isCancellationRequested()) {
            job->Discard();
            return;
        }
        job->done.setWith([&] {
            auto run = std::move(job->work);
            auto previous = std::exchange(worker_stop, &stop);
            auto restore = folly::makeGuard([&] { worker_stop = previous; });
            run();
            return folly::unit;
        });
    });
    return future;
}
}  // namespace milvus
