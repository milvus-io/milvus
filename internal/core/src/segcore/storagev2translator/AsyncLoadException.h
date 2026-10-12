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

#include <exception>
#include <new>
#include <utility>

#include "common/EasyAssert.h"
#include "folly/OperationCancelled.h"
#include "folly/coro/Task.h"
#include "folly/futures/Future.h"

namespace milvus::segcore::storagev2translator::detail {

// Called only from a catch handler. Preserve typed errors and classify native
// exceptions while their types are still available. An Arrow status already
// flattened upstream cannot recover its original category here.
[[noreturn]] inline void
RethrowAsyncLoadException() {
    try {
        throw;
    } catch (const SegcoreError&) {
        throw;
    } catch (const std::bad_alloc& error) {
        throw SegcoreError(ErrorCode::MemAllocateFailed, error.what());
    } catch (const folly::OperationCancelled& error) {
        throw SegcoreError(ErrorCode::FollyCancel, error.what());
    } catch (const folly::FutureCancellation& error) {
        throw SegcoreError(ErrorCode::FollyCancel, error.what());
    } catch (const folly::FutureException& error) {
        throw SegcoreError(ErrorCode::FollyOtherException, error.what());
    } catch (const std::exception& error) {
        throw SegcoreError(ErrorCode::UnexpectedError, error.what());
    } catch (...) {
        throw SegcoreError(ErrorCode::UnexpectedError,
                           "Unknown async load exception");
    }
}

// Classify only after the operation has drained work and selected its failure.
template <typename T>
folly::coro::Task<T>
ClassifyAsyncLoadExceptions(folly::coro::Task<T> task) {
    try {
        co_return co_await std::move(task);
    } catch (...) {
        RethrowAsyncLoadException();
    }
}

}  // namespace milvus::segcore::storagev2translator::detail
