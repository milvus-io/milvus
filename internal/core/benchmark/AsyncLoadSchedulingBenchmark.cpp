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

// Standalone benchmark: include the implementation to measure the exact private
// queue/adapter without exporting benchmark-only production interfaces.
#include "../src/storage/AsyncLoadExecutor.cpp"

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <latch>
#include <numeric>
#include <string>
#include <thread>

#include <folly/executors/ExecutorWithPriority.h>
#include <folly/executors/task_queue/PriorityUnboundedBlockingQueue.h>

// The standalone binary does not initialize the process-wide Milvus pool.
namespace milvus {
int CPU_NUM = 1;
}

namespace {
using Clock = std::chrono::steady_clock;
using Task = folly::CPUThreadPoolExecutor::CPUTask;
using Queue = folly::BlockingQueue<Task>;
using namespace milvus::storage;

// Fail even in the optimized NDEBUG build.
void
Check(bool value) {
    if (!value) {
        std::abort();
    }
}

// Compare the deployed default, a semaphore-matched control, and the new heap.
std::unique_ptr<Queue>
MakeQueue(const std::string& variant) {
    if (variant == "grouped_metadata") {
        return std::make_unique<LoadTaskQueue>();
    }
    if (variant == "folly_lifo") {
        return std::make_unique<folly::PriorityUnboundedBlockingQueue<Task>>(2);
    }
    return folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(2);
}

// Store sequence metadata before timing so raw queue runs exclude allocation.
std::shared_ptr<folly::RequestContext>
Context(uint64_t sequence, const std::shared_ptr<LoadTaskGroup>& group = {}) {
    folly::ShallowCopyRequestContextScopeGuard scope(
        kLoadSequenceToken,
        std::make_unique<LoadSequenceData>(sequence, group));
    return folly::RequestContext::saveContext();
}

// Emit sampled operation/dispatch latency separately from aggregate throughput.
void
Report(const std::string& mode,
       const std::string& variant,
       int repetition,
       size_t workers,
       size_t depth,
       int work_us,
       size_t count,
       double seconds,
       std::vector<double> samples) {
    Check(!samples.empty());
    std::sort(samples.begin(), samples.end());
    const auto quantile = [&](double p) {
        return samples[static_cast<size_t>((samples.size() - 1) * p)];
    };
    std::cout << mode << ',' << variant << ',' << repetition << ',' << workers
              << ',' << depth << ',' << work_us << ',' << count << ','
              << seconds << ',' << count / seconds / 1e6 << ','
              << quantile(0.50) << ',' << quantile(0.99) << ','
              << samples.size() << std::endl;
}

// Verify priority, sequence, and stable ties before running any timing loops.
void
VerifyOrder() {
    auto queue = MakeQueue("grouped_metadata");
    std::vector<int> actual;
    const int priorities[] = {-128, 127, 127, 127};
    const int sequences[] = {1, 20, 10, 10};
    for (int i = 0; i < 4; ++i) {
        Task task([&, i] { actual.push_back(i); },
                  Context(sequences[i]),
                  std::chrono::milliseconds(0),
                  nullptr,
                  priorities[i]);
        queue->addWithPriority(std::move(task), priorities[i]);
    }
    for (int i = 0; i < 4; ++i) {
        auto task = queue->take();
        task.func_();
    }
    Check(actual == std::vector<int>({2, 3, 1, 0}));
    Check(queue->size() == 0);
    // A weighted LOW turn must not let join poisons overtake real work.
    for (int n = 0; n < 16; ++n) {
        queue->addWithPriority(Task(), folly::Executor::LO_PRI);
    }
    for (int n = 0; n < 128; ++n) {
        Task task([] {}, Context(1), std::chrono::milliseconds(0), nullptr,
                  folly::Executor::HI_PRI);
        queue->addWithPriority(std::move(task), folly::Executor::HI_PRI);
    }
    for (int n = 0; n < 128; ++n) {
        Check(static_cast<bool>(queue->take().func_));
    }
    for (int n = 0; n < 16; ++n) {
        Check(!queue->take().func_);
    }
    Check(queue->size() == 0);
}

// Check weak context bindings, cross-pool fallback, timeout, and retirement.
void
VerifyGroupLifecycle() {
    auto queue = std::make_unique<LoadTaskQueue>();
    auto group = queue->GetGroup(127, 10);
    auto registry = group->registry;
    Check(group == queue->GetGroup(127, 10));
    Check(group != queue->GetGroup(-128, 10));
    auto context = Context(10, group);
    std::weak_ptr<LoadTaskGroup> retired = group;
    auto payload = std::make_shared<int>(42);
    std::weak_ptr<int> released = payload;
    Task pending([payload, group] {},
                 context,
                 std::chrono::milliseconds(0),
                 nullptr,
                 127);
    queue->addWithPriority(std::move(pending), 127);
    payload.reset();
    group.reset();
    Check(!retired.expired());
    // Destruction with a pending task must not leave a group/context cycle.
    queue.reset();
    Check(retired.expired() && released.expired());
    Check(registry->groups.empty());

    LoadTaskQueue source, target;
    auto source_group = source.GetGroup(127, 20);
    auto source_context = Context(20, source_group);
    Task foreign(
        [] {}, source_context, std::chrono::milliseconds(0), nullptr, 127);
    target.addWithPriority(std::move(foreign), 127);
    Check(source.size() == 0 && source_group->state.load() == 0);
    Check(target.size() == 1);
    auto taken = target.try_take_for(std::chrono::milliseconds(10));
    Check(taken.has_value());
    Check(!target.try_take_for(std::chrono::milliseconds(1)));

    // A snapshot outliving its source group still preserves the numeric key.
    std::vector<int> order;
    Task late([&] { order.push_back(2); },
              source_context,
              std::chrono::milliseconds(0),
              nullptr,
              127);
    Task early([&] { order.push_back(1); },
               context,
               std::chrono::milliseconds(0),
               nullptr,
               127);
    target.addWithPriority(std::move(late), 127);
    target.addWithPriority(std::move(early), 127);
    target.take().func_();
    target.take().func_();
    Check(order == std::vector<int>({1, 2}));
    Check(target.size() == 0);

    // Group lookup must not retain completed loads indefinitely.
    for (uint64_t sequence = 1; sequence <= 4096; ++sequence) {
        auto transient = source.GetGroup(127, sequence);
    }
    Check(source_group->registry->groups.size() == 1);
}

// Each thread removes then replaces one item, keeping depth within [D-W,D].
// One reported operation is a complete dequeue+enqueue pair, not one queue call.
void
QueueRun(const std::string& variant,
         int repetition,
         size_t workers,
         size_t depth,
         size_t count) {
    auto queue = MakeQueue(variant);
    std::vector<std::shared_ptr<folly::RequestContext>> contexts;
    std::vector<std::shared_ptr<LoadTaskGroup>> bindings;
    contexts.reserve(2048);
    bindings.reserve(2048);
    auto* grouped = dynamic_cast<LoadTaskQueue*>(queue.get());
    for (const int8_t priority : {int8_t{127}, int8_t{-128}}) {
        for (size_t i = 0; i < 1024; ++i) {
            auto binding =
                grouped ? grouped->GetGroup(priority, i + 1) : nullptr;
            contexts.push_back(Context(i + 1, binding));
            bindings.push_back(std::move(binding));
        }
    }
    for (size_t i = 0; i < depth; ++i) {
        const int8_t priority = i % 4 == 0 ? 127 : -128;
        Task task([] {},
                  contexts[(priority == 127 ? 0 : 1024) + i % 1024],
                  std::chrono::milliseconds(0),
                  nullptr,
                  priority);
        queue->addWithPriority(std::move(task), priority);
    }
    std::latch ready(workers), start(1), done(workers);
    std::vector<std::vector<double>> samples(workers);
    std::vector<std::thread> threads;
    threads.reserve(workers);
    for (size_t worker = 0; worker < workers; ++worker) {
        threads.emplace_back([&, worker] {
            auto& times = samples[worker];
            times.reserve(count / workers / 64 + 1);
            uint64_t random = worker + 1;
            ready.count_down();
            start.wait();
            for (size_t i = 0; i < count / workers; ++i) {
                const bool sample = i % 64 == 0;
                const auto before = sample ? Clock::now() : Clock::time_point{};
                auto task = queue->take();
                random = random * 6364136223846793005ULL + 1;
                const int8_t priority = (random >> 48) % 4 == 0 ? 127 : -128;
                task.context_ = contexts[(priority == 127 ? 0 : 1024) +
                                         (random >> 32) % 1024];
                queue->addWithPriority(std::move(task), priority);
                if (sample) {
                    times.push_back(std::chrono::duration<double, std::micro>(
                                        Clock::now() - before)
                                        .count());
                }
            }
            done.count_down();
        });
    }
    ready.wait();
    const auto begin = Clock::now();
    start.count_down();
    done.wait();
    const double seconds =
        std::chrono::duration<double>(Clock::now() - begin).count();
    for (auto& thread : threads) {
        thread.join();
    }
    Check(queue->size() == depth);
    std::vector<double> combined;
    combined.reserve(count / 64 + workers);
    for (const auto& times : samples) {
        combined.insert(combined.end(), times.begin(), times.end());
    }
    Report("queue_pair",
           variant,
           repetition,
           workers,
           depth,
           0,
           count,
           seconds,
           std::move(combined));
}

// A chain models repeated ready/resume submissions, not real I/O or coroutine
// machinery. Only its current callback accesses mutable chain state.
struct Chain {
    folly::Executor::KeepAlive<> executor;
    size_t remaining;
    size_t completed{0};
    int work_us;
    std::latch* done;
    std::vector<double> samples;

    void
    Submit() {
        const bool sample = completed % 16 == 0;
        const auto before = sample ? Clock::now() : Clock::time_point{};
        executor->add([this, sample, before] { Run(sample, before); });
    }

    void
    Run(bool sample, Clock::time_point before) {
        if (sample) {
            samples.push_back(
                std::chrono::duration<double, std::micro>(Clock::now() - before)
                    .count());
        }
        if (work_us != 0) {
            const auto until =
                Clock::now() + std::chrono::microseconds(work_us);
            while (Clock::now() < until) {
                asm volatile("" ::: "memory");
            }
        }
        ++completed;
        if (--remaining != 0) {
            Submit();
        } else {
            done->count_down();
        }
    }
};

// Gate the initial callbacks until every worker is ready; construction and join
// are excluded. Initial enqueue-to-start delays are excluded from samples.
void
ExecutorRun(const std::string& variant,
            int repetition,
            size_t workers,
            size_t depth,
            int work_us,
            size_t count) {
    std::unique_ptr<folly::CPUThreadPoolExecutor> pool;
    if (variant == "grouped_metadata") {
        pool = std::make_unique<PriorityThreadPoolExecutor>(workers);
    } else {
        pool = std::make_unique<folly::CPUThreadPoolExecutor>(
            workers, MakeQueue(variant), MakeAsyncLoadThreadFactory());
    }
    std::latch ready(workers), start(1), done(depth);
    for (size_t i = 0; i < workers; ++i) {
        pool->add([&] {
            ready.count_down();
            start.wait();
        });
    }
    ready.wait();
    std::vector<std::unique_ptr<Chain>> chains;
    chains.reserve(depth);
    for (size_t i = 0; i < depth; ++i) {
        const int8_t priority = i % 4 == 0 ? 127 : -128;
        auto executor =
            variant == "grouped_metadata" || variant == "folly_metadata"
                ? LoadExecutor::Create(
                      folly::getKeepAliveToken(pool.get()), priority, 1 + i / 4)
                : folly::ExecutorWithPriority::create(
                      folly::getKeepAliveToken(pool.get()), priority);
        auto chain = std::make_unique<Chain>();
        chain->executor = std::move(executor);
        chain->remaining = count / depth;
        chain->work_us = work_us;
        chain->done = &done;
        chain->samples.reserve(count / depth / 16 + 1);
        auto* ptr = chain.get();
        chain->executor->add([ptr] { ptr->Run(false, {}); });
        chains.push_back(std::move(chain));
    }
    const auto begin = Clock::now();
    start.count_down();
    done.wait();
    const double seconds =
        std::chrono::duration<double>(Clock::now() - begin).count();
    std::vector<double> samples;
    samples.reserve(count / 16);
    for (auto& chain : chains) {
        Check(chain->remaining == 0 && chain->completed == count / depth);
        samples.insert(
            samples.end(), chain->samples.begin(), chain->samples.end());
        chain->executor.reset();
    }
    pool->join();
    Report("executor_chain",
           variant,
           repetition,
           workers,
           depth,
           work_us,
           count,
           seconds,
           std::move(samples));
}
}  // namespace

int
main(int argc, char** argv) {
    if (argc != 9) {
        std::cerr
            << "mode variant repetition workers depth work_us count warmup\n";
        return 2;
    }
    VerifyOrder();
    VerifyGroupLifecycle();
    const std::string mode = argv[1], variant = argv[2];
    Check(variant == "folly" || variant == "folly_lifo" ||
          variant == "folly_metadata" || variant == "grouped_metadata");
    const int repetition = std::stoi(argv[3]);
    const size_t workers = std::stoul(argv[4]), depth = std::stoul(argv[5]);
    const int work_us = std::stoi(argv[6]);
    const size_t count = std::stoul(argv[7]);
    Check(workers > 0 && depth > workers && count % depth == 0 &&
          count % workers == 0 && (mode == "queue" || count / depth > 16));
    std::cout << std::fixed << std::setprecision(6);
    if (std::stoi(argv[8]) != 0) {
        QueueRun("folly", -1, 1, 64, 32768);
        QueueRun("grouped_metadata", -1, 1, 64, 32768);
    }
    if (mode == "queue") {
        QueueRun(variant, repetition, workers, depth, count);
    } else {
        Check(mode == "executor");
        ExecutorRun(variant, repetition, workers, depth, work_us, count);
    }
}
