// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <stdlib.h>
#include <common/EasyAssert.h>
#include <folly/CancellationToken.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include "future_c_types.h"
#include "LeakyResult.h"
#include "Ready.h"
#include "pb/cgo_msg.pb.h"
#include <chrono>
#include "monitor/prometheus_client.h"

namespace milvus::futures {

template <class Duration>
class Metrics;

template <class Duration>
class Metrics {
 public:
    class ExecutionGuard {
     public:
        explicit ExecutionGuard(Metrics& metrics) : metrics_(metrics) {
            metrics.startExecute();
        }

        ExecutionGuard(const ExecutionGuard&) = delete;
        ExecutionGuard(const ExecutionGuard&&) = delete;
        ExecutionGuard&
        operator=(const ExecutionGuard&) = delete;
        ExecutionGuard&
        operator=(const ExecutionGuard&&) = delete;

        ~ExecutionGuard() {
            metrics_.executeDone();
        }

     private:
        Metrics& metrics_;
    };

    explicit Metrics()
        : time_point_(std::chrono::steady_clock::now()),
          queue_duration_(0),
          execute_duration_(0),
          cancelled_before_execute_(false) {
        milvus::monitor::internal_cgo_inflight_task_total_all.Increment();
    }

    Metrics(const Metrics&) = delete;
    Metrics(const Metrics&&) = delete;
    Metrics&
    operator=(const Metrics&) = delete;
    Metrics&
    operator=(const Metrics&&) = delete;

    ~Metrics() {
        milvus::monitor::internal_cgo_inflight_task_total_all.Decrement();
        milvus::monitor::internal_cgo_queue_duration_seconds_all.Observe(
            std::chrono::duration<double>(queue_duration_).count());
        if (cancelled_before_execute_) {
            milvus::monitor::internal_cgo_cancel_before_execute_total_all
                .Increment();
        } else {
            milvus::monitor::internal_cgo_executing_task_total_all.Decrement();
            milvus::monitor::internal_cgo_execute_duration_seconds_all.Observe(
                std::chrono::duration<double>(execute_duration_).count());
        }
    }

    void
    withCancel() {
        queue_duration_ = std::chrono::duration_cast<Duration>(
            std::chrono::steady_clock::now() - time_point_);
        cancelled_before_execute_ = true;
    }

    void
    startExecute() {
        auto now = std::chrono::steady_clock::now();
        queue_duration_ =
            std::chrono::duration_cast<Duration>(now - time_point_);
        time_point_ = now;
        milvus::monitor::internal_cgo_executing_task_total_all.Increment();
    }

    void
    executeDone() {
        auto now = std::chrono::steady_clock::now();
        execute_duration_ =
            std::chrono::duration_cast<Duration>(now - time_point_);
    }

 private:
    std::chrono::steady_clock::time_point time_point_;
    Duration queue_duration_;
    Duration execute_duration_;
    bool cancelled_before_execute_;
};

// FutureResult is a struct that represents the result of the future.
class FutureResult {
 public:
    void* result;
    CStatus status;
    Metrics<std::chrono::microseconds> metrics;
};

/// @brief a virtual class that represents a future can be polymorphic called by CGO code.
/// implemented by Future template.
class IFuture {
 public:
    /// @brief cancel the future with the given exception.
    /// After cancelled is called, the underlying async function will receive cancellation.
    /// It just a signal notification, the cancellation is handled by user-defined.
    /// If the underlying async function ignore the cancellation signal, the Future is still blocked.
    virtual void
    cancel() = 0;

    /// @brief check if the future is ready or canceled.
    /// @return true if the future is ready or canceled, otherwise false.
    virtual bool
    isReady() = 0;

    /// @brief register a callback that will be called when the future is ready or future has been ready.
    virtual void
    registerReadyCallback(CUnlockGoMutexFn unlockFn, CLockedGoMutex* mutex) = 0;

    /// @brief get the result of the future. it must be called if future is ready.
    /// the first element of the pair is the result,
    /// the second element of the pair is the exception.
    /// !!! It can only be called once,
    /// and the result need to be manually released by caller after these call.
    virtual std::pair<void*, CStatus>
    leakyGet() = 0;

    /// @brief leaked future object created by method `Future<R>::createLeakedFuture` can be droped by these method.
    static void
    releaseLeakedFuture(IFuture* future) {
        delete future;
    }

    virtual ~IFuture() = default;
};

/// @brief a class that represents a cancellation token
class CancellationToken : public folly::CancellationToken {
 public:
    CancellationToken(folly::CancellationToken&& token) noexcept
        : folly::CancellationToken(std::move(token)) {
    }

    /// @brief check if the token is cancelled, throw a FutureCancellation exception if it is.
    void
    throwIfCancelled() const {
        if (isCancellationRequested()) {
            throw folly::FutureCancellation();
        }
    }
};

/// @brief Future is a class that bound a future with a result for
/// using by cgo.
/// @tparam R is the return type of the producer function.
template <class R>
class Future : public IFuture {
 public:
    /// @brief do a async operation which will produce a result.
    /// fn returns pointer to R (leaked, default memory allocator) if it is success, otherwise it will throw a exception.
    /// returned result or exception will be handled by consumer side.
    template <typename Fn,
              typename = std::enable_if<
                  std::is_invocable_r_v<R*, Fn, CancellationToken>>>
    static std::unique_ptr<Future<R>>
    async(folly::Executor::KeepAlive<> executor,
          int priority,
          Fn&& fn) noexcept {
        auto future = std::make_unique<Future<R>>();
        // setup the interrupt handler for the promise.
        future->setInterruptHandler();
        // start async function.
        future->asyncProduce(executor, priority, std::forward<Fn>(fn));
        // register consume callback function.
        future->registerConsumeCallback(executor, priority);
        return future;
    }

    /// use `async`.
    Future()
        : metrics_(),
          ready_(std::make_shared<Ready<LeakyResult<R>>>()),
          promise_(std::make_shared<folly::SharedPromise<R*>>()),
          cancellation_source_() {
    }

    Future(const Future<R>&) = delete;

    Future(Future<R>&&) noexcept = default;

    Future&
    operator=(const Future<R>&) = delete;

    Future&
    operator=(Future<R>&&) noexcept = default;

    /// @brief see `IFuture::cancel`
    void
    cancel() noexcept override {
        promise_->getSemiFuture().cancel();
    }

    /// @brief see `IFuture::registerReadyCallback`
    void
    registerReadyCallback(CUnlockGoMutexFn unlockFn,
                          CLockedGoMutex* mutex) noexcept override {
        ready_->callOrRegisterCallback(
            [unlockFn = unlockFn, mutex = mutex]() { unlockFn(mutex); });
    }

    /// @brief see `IFuture::isReady`
    bool
    isReady() noexcept override {
        return ready_->isReady();
    }

    /// @brief see `IFuture::leakyGet`
    std::pair<void*, CStatus>
    leakyGet() noexcept override {
        auto result = std::move(*ready_).getValue();
        return result.leakyGet();
    }

 private:
    /// @brief set the interrupt handler for the promise used in async produce arm.
    void
    setInterruptHandler() {
        promise_->setInterruptHandler([cancellation_source =
                                           cancellation_source_,
                                       ready = ready_](
                                          const folly::exception_wrapper& ew) {
            // 1. set the result to perform a fast fail.
            // 2. set the cancellation to the source to notify cancellation to the consumers.
            ew.handle(
                [&](const folly::FutureCancellation& e) {
                    cancellation_source.requestCancellation();
                },
                [&](const folly::FutureTimeout& e) {
                    cancellation_source.requestCancellation();
                });
        });
    }

    /// @brief do the R produce operation in async way.
    template <typename Fn,
              typename... Args,
              typename = std::enable_if<
                  std::is_invocable_r_v<R*, Fn, CancellationToken>>>
    void
    asyncProduce(folly::Executor::KeepAlive<> executor, int priority, Fn&& fn) {
        // start produce process async.
        auto cancellation_token =
            CancellationToken(cancellation_source_.getToken());
        auto runner = [fn = std::forward<Fn>(fn),
                       cancellation_token = std::move(cancellation_token),
                       this]() {
            if (cancellation_token.isCancellationRequested()) {
                metrics_.withCancel();
                throw folly::FutureCancellation();
            }

            auto executionGuard =
                Metrics<std::chrono::microseconds>::ExecutionGuard(metrics_);
            return fn(cancellation_token);
        };

        // the runner is executed may be executed in different thread.
        // so manage the promise with shared_ptr.
        auto thenRunner = [promise = promise_, runner = std::move(runner)](
                              auto&&) { promise->setWith(std::move(runner)); };
        folly::makeSemiFuture().via(executor, priority).then(thenRunner);
    }

    /// @brief async consume the result of the future.
    void
    registerConsumeCallback(folly::Executor::KeepAlive<> executor,
                            int priority) noexcept {
        // set up the result consume arm and exception consume arm.
        promise_->getSemiFuture()
            .via(executor, priority)
            .thenValue(
                [ready = ready_](R* r) { ready->setValue(LeakyResult<R>(r)); })
            .thenError(folly::tag_t<folly::FutureCancellation>{},
                       [ready = ready_](const folly::FutureCancellation& e) {
                           ready->setValue(
                               LeakyResult<R>(milvus::FollyCancel, e.what()));
                       })
            .thenError(folly::tag_t<folly::FutureException>{},
                       [ready = ready_](const folly::FutureException& e) {
                           ready->setValue(LeakyResult<R>(
                               milvus::FollyOtherException, e.what()));
                       })
            .thenError(folly::tag_t<milvus::SegcoreError>{},
                       [ready = ready_](const milvus::SegcoreError& e) {
                           ready->setValue(LeakyResult<R>(
                               static_cast<int>(e.get_error_code()), e.what()));
                       })
            .thenError(folly::tag_t<std::exception>{},
                       [ready = ready_](const std::exception& e) {
                           ready->setValue(LeakyResult<R>(
                               milvus::UnexpectedError, e.what()));
                       });
    }

 private:
    Metrics<std::chrono::microseconds> metrics_;
    std::shared_ptr<Ready<LeakyResult<R>>> ready_;
    std::shared_ptr<folly::SharedPromise<R*>> promise_;
    folly::CancellationSource cancellation_source_;
};
};  // namespace milvus::futures