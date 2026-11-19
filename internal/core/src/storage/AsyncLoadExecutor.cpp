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

#include "storage/AsyncLoadExecutor.h"

#include <algorithm>
#include <atomic>
#include <array>
#include <cassert>
#include <exception>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "folly/concurrency/UnboundedQueue.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/io/async/Request.h"
#include "folly/synchronization/ThrottledLifoSem.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "folly/executors/thread_factory/PriorityThreadFactory.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {
namespace {

const folly::RequestToken kLoadSequenceToken("milvus.load_seq");

struct LoadTaskGroup;

// Weak group references avoid a cycle through the group's queued RequestContexts.
// Sequence metadata remains usable when forwarded to an unrelated executor.
class LoadSequenceData final : public folly::RequestData {
 public:
    explicit LoadSequenceData(uint64_t sequence,
                              const std::shared_ptr<LoadTaskGroup>& group = {})
        : sequence(sequence), group(group) {
    }
    bool
    hasCallback() override {
        return false;
    }
    const uint64_t sequence;
    const std::weak_ptr<LoadTaskGroup> group;
};

// Reattaches metadata BEFORE every submission, including coroutine resumption
// from an I/O callback whose RequestContext belongs to another operation.
class LoadExecutor final : public folly::Executor {
 public:
    static folly::Executor::KeepAlive<>
    Create(folly::Executor::KeepAlive<> executor,
           int8_t priority,
           uint64_t sequence);

    void
    add(folly::Func func) override {
        folly::ShallowCopyRequestContextScopeGuard context(
            kLoadSequenceToken,
            std::make_unique<LoadSequenceData>(sequence_, group_));
        if (executor_->getNumPriorities() > 1) {
            executor_->addWithPriority(std::move(func), priority_);
        } else {
            executor_->add(std::move(func));
        }
    }

    uint64_t
    Sequence() const noexcept {
        return sequence_;
    }

    folly::Executor::KeepAlive<>
    BaseExecutor() const {
        return executor_.copy();
    }

 private:
    LoadExecutor(folly::Executor::KeepAlive<> executor,
                 int8_t priority,
                 uint64_t sequence,
                 std::shared_ptr<LoadTaskGroup> group)
        : executor_(std::move(executor)),
          priority_(priority),
          sequence_(sequence),
          group_(std::move(group)) {
    }

    bool
    keepAliveAcquire() noexcept override {
        references_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    void
    keepAliveRelease() noexcept override {
        if (references_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }

    std::atomic<size_t> references_{1};
    folly::Executor::KeepAlive<> executor_;
    const int8_t priority_;
    const uint64_t sequence_;
    const std::shared_ptr<LoadTaskGroup> group_;
};

// Ordered by priority, then sequence; zero retains the unspecified FIFO group.
struct LoadGroupKey {
    int8_t priority;
    uint64_t sequence;

    bool
    operator<(const LoadGroupKey& other) const noexcept {
        return priority != other.priority ? priority > other.priority
                                          : sequence < other.sequence;
    }
};

struct LoadGroupRegistry {
    std::mutex mutex;
    std::map<LoadGroupKey, std::weak_ptr<LoadTaskGroup>> groups;
};

// Owns one load's FIFO. Adapters and ready-heap entries keep the group alive;
// the registry is weak and drops its entry when the last owner releases it.
struct LoadTaskGroup {
    using Task = folly::CPUThreadPoolExecutor::CPUTask;
    static constexpr size_t kReady = 1;
    static constexpr size_t kTask = 2;

    LoadTaskGroup(std::shared_ptr<LoadGroupRegistry> registry, LoadGroupKey key)
        : registry(std::move(registry)), key(key) {
    }

    ~LoadTaskGroup() {
        std::lock_guard lock(registry->mutex);
        const auto it = registry->groups.find(key);
        // An expired entry may already have been replaced by a new binding.
        if (it != registry->groups.end() && it->second.expired()) {
            registry->groups.erase(it);
        }
    }

    const std::shared_ptr<LoadGroupRegistry> registry;
    const LoadGroupKey key;
    // Small segments avoid reserving a default 256-task segment per idle load.
    folly::UMPMCQueue<Task, true, 4> tasks;
    // Upper bits count unreserved tasks; bit zero means present in ready_.
    std::atomic<size_t> state{0};
};

// Only groups are sorted. Producers on an already-ready group bypass mutex_;
// consumers reserve a group under mutex_ and dequeue its FIFO after unlocking.
class LoadTaskQueue final
    : public folly::BlockingQueue<folly::CPUThreadPoolExecutor::CPUTask> {
    using Task = folly::CPUThreadPoolExecutor::CPUTask;
    using Group = LoadTaskGroup;
    struct Later {
        bool
        operator()(const std::shared_ptr<Group>& left,
                   const std::shared_ptr<Group>& right) const noexcept {
            return right->key < left->key;
        }
    };

 public:
    explicit LoadTaskQueue(std::array<size_t, 3> weights = {32, 8, 1})
        : weights_(weights), remaining_(weights[0]) {
    }

    void
    SetWeights(std::array<size_t, 3> weights) {
        std::lock_guard lock(mutex_);
        weights_ = weights;
        next_priority_ = 0;
        remaining_ = weights_[0];
    }

    ~LoadTaskQueue() override {
        // A queued callable can own its adapter/group. Release callables before
        // dropping ready_ ownership, otherwise discarded work can form a cycle.
        for (const auto& heap : ready_) {
            for (const auto& group : heap) {
                while (auto task = group->tasks.try_dequeue()) {
                }
                group->state.store(0, std::memory_order_relaxed);
            }
        }
    }

    // Bind once per adapter, sharing identical keys across independent adapters.
    // Reserve heap capacity before publishing a group so add cannot allocate a
    // ready entry after it has already committed the task to the FIFO.
    std::shared_ptr<Group>
    GetGroup(int8_t priority, uint64_t sequence) {
        const LoadGroupKey key{
            priority,
            sequence == 0 ? std::numeric_limits<uint64_t>::max() : sequence};
        {
            std::lock_guard lock(registry_->mutex);
            const auto it = registry_->groups.find(key);
            if (it != registry_->groups.end()) {
                if (auto group = it->second.lock()) {
                    return group;
                }
            }
        }
        // Allocate outside both locks; destroy a losing candidate after unlock.
        auto candidate = std::make_shared<Group>(registry_, key);
        std::lock_guard registry_lock(registry_->mutex);
        const auto it = registry_->groups.find(key);
        if (it != registry_->groups.end()) {
            if (auto group = it->second.lock()) {
                return group;
            }
        }
        {
            std::lock_guard lock(mutex_);
            const auto capacity = registry_->groups.size() + 1;
            auto& heap = ready_[PriorityIndex(priority)];
            if (heap.capacity() < capacity) {
                heap.reserve(std::max(capacity, heap.capacity() * 2));
            }
        }
        registry_->groups[key] = candidate;
        return candidate;
    }

    folly::BlockingQueueAddResult
    add(Task&& task) override {
        return addWithPriority(std::move(task), folly::Executor::MID_PRI);
    }

    folly::BlockingQueueAddResult
    addWithPriority(Task&& task, int8_t priority) override {
        // Folly's join poisons must follow ALL pending work, not a weighted
        // LOW turn; otherwise every worker could exit with real work queued.
        if (!task.func_) {
            {
                std::lock_guard lock(mutex_);
                ++stop_tasks_;
            }
            return available_.post();
        }
        uint64_t sequence = 0;
        std::shared_ptr<Group> group;
        if (task.func_ && task.context_) {
            const auto* data = static_cast<const LoadSequenceData*>(
                task.context_->getContextData(kLoadSequenceToken));
            if (data != nullptr) {
                sequence = data->sequence;
                group = data->group.lock();
            }
        }
        // Foreign contexts and Folly's shutdown tasks cannot reuse another
        // executor's group, or an adapter bound to a different priority.
        if (!group || group->registry != registry_ ||
            group->key.priority != priority) {
            group = GetGroup(priority, sequence);
        }
        group->tasks.enqueue(std::move(task));
        const auto previous =
            group->state.fetch_add(Group::kTask, std::memory_order_acq_rel);
        if (!(previous & Group::kReady)) {
            std::lock_guard lock(mutex_);
            const auto state = group->state.load(std::memory_order_relaxed);
            // Another producer may have published the group, and consumers may
            // already have reserved every task while this producer waited.
            if (state >= Group::kTask && !(state & Group::kReady)) {
                auto& heap = ready_[PriorityIndex(priority)];
                heap.push_back(group);
                std::push_heap(heap.begin(), heap.end(), Later{});
                group->state.fetch_or(Group::kReady, std::memory_order_release);
            }
        }
        // Every producer completes ready publication before posting a permit.
        return available_.post();
    }

    uint8_t
    getNumPriorities() override {
        return 3;
    }

    Task
    take() override {
        available_.wait();
        return Pop();
    }

    folly::Optional<Task>
    try_take_for(std::chrono::milliseconds timeout) override {
        if (!available_.try_wait_for(timeout)) {
            return folly::none;
        }
        return Pop();
    }

    size_t
    size() override {
        return available_.valueGuess();
    }

 private:
    // Folly forwards signed priorities unchanged to the custom blocking queue.
    static constexpr size_t
    PriorityIndex(int8_t priority) noexcept {
        return priority > folly::Executor::MID_PRI
                   ? 0
                   : (priority == folly::Executor::MID_PRI ? 1 : 2);
    }

    // Called under mutex_. Empty classes forfeit their quota; idle queues reset
    // the round so a new burst starts with Urgent. No clock reads or allocation.
    size_t
    SelectPriority() noexcept {
        for (size_t tried = 0; tried < weights_.size(); ++tried) {
            if (remaining_ != 0 && !ready_[next_priority_].empty()) {
                --remaining_;
                return next_priority_;
            }
            next_priority_ = (next_priority_ + 1) % weights_.size();
            remaining_ = weights_[next_priority_];
        }
        // The starting class may be the only ready class with an exhausted quota.
        assert(!ready_[next_priority_].empty());
        --remaining_;
        return next_priority_;
    }

    // Atomically clear readiness when reserving the last task. A racing producer
    // must then republish the group under mutex_ before waking another consumer.
    Task
    Pop() {
        std::shared_ptr<Group> group;
        {
            std::lock_guard lock(mutex_);
            if (ready_[0].empty() && ready_[1].empty() && ready_[2].empty()) {
                assert(stop_tasks_ != 0);
                --stop_tasks_;
                return Task();
            }
            auto& heap = ready_[SelectPriority()];
            group = heap.front();
            auto state = group->state.load(std::memory_order_relaxed);
            assert(state >= Group::kTask + Group::kReady &&
                   (state & Group::kReady));
            size_t next;
            do {
                next = state == Group::kTask + Group::kReady
                           ? 0
                           : state - Group::kTask;
            } while (
                !group->state.compare_exchange_weak(state,
                                                    next,
                                                    std::memory_order_acq_rel,
                                                    std::memory_order_relaxed));
            if (next == 0) {
                std::pop_heap(heap.begin(), heap.end(), Later{});
                heap.pop_back();
                if (ready_[0].empty() && ready_[1].empty() &&
                    ready_[2].empty()) {
                    next_priority_ = 0;
                    remaining_ = weights_[0];
                }
            }
        }
        // A concurrent producer may still be publishing an earlier FIFO slot.
        // Never wait for that slot or destroy a retired group under mutex_.
        return group->tasks.dequeue();
    }

    const std::shared_ptr<LoadGroupRegistry> registry_ =
        std::make_shared<LoadGroupRegistry>();
    std::mutex mutex_;
    std::array<size_t, 3> weights_;
    std::array<std::vector<std::shared_ptr<Group>>, 3> ready_;
    size_t stop_tasks_{0};
    size_t next_priority_{0};
    size_t remaining_{weights_[0]};
    folly::ThrottledLifoSem available_;
};

// Linux nice is per-thread; other platforms may apply it process-wide.
std::shared_ptr<folly::ThreadFactory>
MakeAsyncLoadThreadFactory() {
    auto factory =
        std::make_shared<folly::NamedThreadFactory>("MILVUS_ASYNC_LOAD_");
#ifdef __linux__
    return std::make_shared<folly::PriorityThreadFactory>(std::move(factory),
                                                          /* nice */ 19);
#else
    return factory;
#endif
}

// Owns the process-wide priority queues used by async-load CPU work.
class PriorityThreadPoolExecutor final : public folly::CPUThreadPoolExecutor {
 public:
    explicit PriorityThreadPoolExecutor(const int threads,
                                        std::array<size_t, 3> weights)
        : folly::CPUThreadPoolExecutor(threads,
                                       std::make_unique<LoadTaskQueue>(weights),
                                       MakeAsyncLoadThreadFactory()) {
    }

    void
    SetWeights(std::array<size_t, 3> weights) {
        static_cast<LoadTaskQueue*>(getTaskQueue())->SetWeights(weights);
    }

    std::shared_ptr<LoadTaskGroup>
    GetGroup(int8_t priority, uint64_t sequence) {
        return static_cast<LoadTaskQueue*>(getTaskQueue())
            ->GetGroup(priority, sequence);
    }
};

// Bind the actual ASYNC_LOAD queue; custom executors keep only sequence metadata.
folly::Executor::KeepAlive<>
LoadExecutor::Create(folly::Executor::KeepAlive<> executor,
                     int8_t priority,
                     uint64_t sequence) {
    std::shared_ptr<LoadTaskGroup> group;
    if (auto* pool =
            dynamic_cast<PriorityThreadPoolExecutor*>(executor.get())) {
        group = pool->GetGroup(priority, sequence);
    }
    return makeKeepAlive(new LoadExecutor(
        std::move(executor), priority, sequence, std::move(group)));
}

// Keeps configuration lazy and serializes resizes separately from executor
// acquisition, so a finishing worker can acquire a keep-alive during shrink.
struct DefaultExecutorState {
    std::mutex configure_mutex;
    std::mutex executor_mutex;
    int initial_threads = std::max(
        1, std::min(milvus::CPU_NUM, DEFAULT_THREAD_POOL_MAX_THREADS_SIZE));
    std::array<size_t, 3> weights{32, 8, 1};
    std::unique_ptr<PriorityThreadPoolExecutor> executor;
};

DefaultExecutorState&
GetDefaultExecutorState() {
    static DefaultExecutorState state;
    return state;
}

// Preserve Folly's priority API: urgent=positive, high=zero, low=negative.
[[nodiscard]] constexpr int8_t
ExecutorPriority(AsyncLoadPriority priority) noexcept {
    switch (priority) {
        case AsyncLoadPriority::Urgent:
            return folly::Executor::HI_PRI;
        case AsyncLoadPriority::High:
            return folly::Executor::MID_PRI;
        case AsyncLoadPriority::Low:
            return folly::Executor::LO_PRI;
    }
    return folly::Executor::MID_PRI;
}

// Returns the process-wide executor shared by async-load operations.
[[nodiscard]] folly::Executor::KeepAlive<>
GetDefaultAsyncLoadExecutor() {
    // The pool is never replaced. Static initialization synchronizes its first
    // publication without taking the configuration lock on every load window.
    static auto& executor = []() -> PriorityThreadPoolExecutor& {
        auto& state = GetDefaultExecutorState();
        std::lock_guard lock(state.executor_mutex);
        state.executor = std::make_unique<PriorityThreadPoolExecutor>(
            state.initial_threads, state.weights);
        return *state.executor;
    }();
    return folly::getKeepAliveToken(&executor);
}

}  // namespace

void
SetAsyncLoadSchedulingWeights(int urgent, int high, int low) {
    const std::array<size_t, 3> weights =
        urgent > 0 && high > 0 && low > 0
            ? std::array<size_t, 3>{static_cast<size_t>(urgent),
                                    static_cast<size_t>(high),
                                    static_cast<size_t>(low)}
            : std::array<size_t, 3>{32, 8, 1};
    auto& state = GetDefaultExecutorState();
    std::lock_guard lock(state.executor_mutex);
    if (state.executor) {
        state.executor->SetWeights(weights);
    }
    state.weights = weights;
}

void
SetAsyncLoadThreadPoolSize(const int threads) {
    if (threads <= 0) {
        ThrowInfo(ErrorCode::ConfigInvalid,
                  "Async load thread pool size must be positive, got {}",
                  threads);
    }
    auto& state = GetDefaultExecutorState();
    std::lock_guard configure_lock(state.configure_mutex);
    PriorityThreadPoolExecutor* executor;
    {
        std::lock_guard executor_lock(state.executor_mutex);
        if (!state.executor) {
            state.initial_threads = threads;
            return;
        }
        executor = state.executor.get();
    }
    const auto previous = executor->numThreads();
    if (previous == static_cast<size_t>(threads)) {
        return;
    }
    try {
        executor->setNumThreads(threads);
    } catch (...) {
        const auto resize_error = std::current_exception();
        try {
            executor->setNumThreads(previous);
        } catch (const std::exception& rollback_error) {
            LOG_ERROR("Failed to restore async load worker count to {}: {}",
                      previous,
                      rollback_error.what());
        }
        std::rethrow_exception(resize_error);
    }
}

int
GetAsyncLoadThreadPoolSize() {
    auto& state = GetDefaultExecutorState();
    std::lock_guard lock(state.executor_mutex);
    return state.executor ? static_cast<int>(state.executor->numThreads())
                          : state.initial_threads;
}

folly::Executor::KeepAlive<>
ResolveAsyncLoadExecutor(folly::Executor::KeepAlive<> executor,
                         const AsyncLoadPriority load_priority,
                         uint64_t load_seq) {
    if (const auto* bound = dynamic_cast<const LoadExecutor*>(executor.get())) {
        if (load_seq == 0) {
            load_seq = bound->Sequence();
        }
        executor = bound->BaseExecutor();
    }
    if (!executor) {
        executor = GetDefaultAsyncLoadExecutor();
    }
    auto priority = ExecutorPriority(load_priority);
    // External executors may only have two classes: preserve the old HIGH mapping.
    if (load_priority == AsyncLoadPriority::High &&
        dynamic_cast<PriorityThreadPoolExecutor*>(executor.get()) == nullptr) {
        priority = folly::Executor::HI_PRI;
    }
    return LoadExecutor::Create(std::move(executor), priority, load_seq);
}

folly::Executor::KeepAlive<>
ResolveAsyncLoadExecutor(folly::Executor::KeepAlive<> executor,
                         milvus::proto::common::LoadPriority priority,
                         uint64_t load_seq) {
    return ResolveAsyncLoadExecutor(
        std::move(executor),
        priority == milvus::proto::common::LoadPriority::LOW
            ? AsyncLoadPriority::Low
            : AsyncLoadPriority::High,
        load_seq);
}

uint64_t
NextLoadSequence() noexcept {
    static std::atomic<uint64_t> next{1};
    return next.fetch_add(1, std::memory_order_relaxed);
}

uint64_t
GetAsyncLoadSequence(const folly::Executor* executor) noexcept {
    const auto* load_executor = dynamic_cast<const LoadExecutor*>(executor);
    return load_executor ? load_executor->Sequence() : 0;
}

}  // namespace milvus::storage
