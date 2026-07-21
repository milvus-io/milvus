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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include <folly/CancellationToken.h>

#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "segcore/SegmentReadLease.h"

namespace milvus::segcore {
namespace {

using namespace std::chrono_literals;

template <typename Predicate>
bool
WaitUntil(Predicate&& predicate, std::chrono::milliseconds timeout = 2s) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

TEST(SegmentReadGateTest, PendingWriterRejectsNewReaders) {
    SegmentReadGate gate;
    auto first_reader = gate.AcquireRead(folly::CancellationToken(), 100);

    std::atomic<bool> writer_acquired{false};
    std::atomic<bool> release_writer{false};
    std::thread writer([&] {
        auto publish_lease = gate.AcquirePublish(nullptr, 100);
        writer_acquired.store(true, std::memory_order_release);
        while (!release_writer.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        publish_lease.MarkPublished();
    });

    EXPECT_TRUE(WaitUntil([&] { return gate.WriterPending(); }));

    try {
        auto lease = gate.AcquireRead(folly::CancellationToken(), 100);
        FAIL() << "expected pending writer to reject a new reader";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyOtherException);
    }
    EXPECT_EQ(gate.ActiveReaders(), 1);

    first_reader.reset();
    EXPECT_TRUE(WaitUntil(
        [&] { return writer_acquired.load(std::memory_order_acquire); }));

    release_writer.store(true, std::memory_order_release);
    writer.join();

    auto next_reader = gate.AcquireRead(folly::CancellationToken(), 100);
    EXPECT_TRUE(next_reader->valid());
    EXPECT_EQ(gate.PublishedGeneration(), 1);
    EXPECT_EQ(gate.BlockedReadersTotal(), 1);
}

TEST(SegmentReadGateTest, ReadLeaseCanReleaseOnAnotherThread) {
    SegmentReadGate gate;
    auto lease = gate.AcquireRead(folly::CancellationToken(), 101);
    EXPECT_EQ(gate.ActiveReaders(), 1);

    std::thread releaser(
        [lease = std::move(lease)]() mutable { lease.reset(); });
    releaser.join();

    EXPECT_EQ(gate.ActiveReaders(), 0);
}

TEST(SegmentReadGateTest, CancelledPublisherReopensGate) {
    SegmentReadGate gate;
    auto first_reader = gate.AcquireRead(folly::CancellationToken(), 102);

    folly::CancellationSource cancellation_source;
    milvus::OpContext op_ctx(cancellation_source.getToken());
    std::atomic<bool> cancelled{false};
    std::thread writer([&] {
        try {
            auto publish_lease = gate.AcquirePublish(&op_ctx, 102);
        } catch (const SegcoreError& error) {
            cancelled.store(error.get_error_code() == ErrorCode::FollyCancel,
                            std::memory_order_release);
        }
    });

    EXPECT_TRUE(WaitUntil([&] { return gate.WriterPending(); }));
    cancellation_source.requestCancellation();
    writer.join();

    EXPECT_TRUE(cancelled.load(std::memory_order_acquire));
    EXPECT_FALSE(gate.WriterPending());
    EXPECT_EQ(gate.PublishedGeneration(), 0);

    first_reader.reset();
    auto next_reader = gate.AcquireRead(folly::CancellationToken(), 102);
    EXPECT_TRUE(next_reader->valid());
}

TEST(SegmentReadGateTest, PreCancelledPublisherDoesNotPublish) {
    SegmentReadGate gate;
    folly::CancellationSource cancellation_source;
    cancellation_source.requestCancellation();
    milvus::OpContext op_ctx(cancellation_source.getToken());

    try {
        auto publish_lease = gate.AcquirePublish(&op_ctx, 103);
        FAIL() << "expected pre-cancelled publisher to fail";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }

    EXPECT_FALSE(gate.WriterPending());
    EXPECT_EQ(gate.PublishedGeneration(), 0);
    auto reader = gate.AcquireRead(folly::CancellationToken(), 103);
    EXPECT_TRUE(reader->valid());
}

TEST(SegmentReadGateTest, CancellationAtLastReaderReleaseDoesNotPublish) {
    SegmentReadGate gate;
    auto reader = gate.AcquireRead(folly::CancellationToken(), 104);

    folly::CancellationSource cancellation_source;
    milvus::OpContext op_ctx(cancellation_source.getToken());
    std::atomic<bool> cancelled{false};
    std::thread writer([&] {
        try {
            auto publish_lease = gate.AcquirePublish(&op_ctx, 104);
        } catch (const SegcoreError& error) {
            cancelled.store(error.get_error_code() == ErrorCode::FollyCancel,
                            std::memory_order_release);
        }
    });

    EXPECT_TRUE(WaitUntil([&] { return gate.WriterPending(); }));
    cancellation_source.requestCancellation();
    reader.reset();
    writer.join();

    EXPECT_TRUE(cancelled.load(std::memory_order_acquire));
    EXPECT_FALSE(gate.WriterPending());
    EXPECT_EQ(gate.PublishedGeneration(), 0);
}

TEST(SegmentReadGateTest, NullContextPublisherTimesOutAndReopensGate) {
    SegmentReadGate gate(20ms);
    auto reader = gate.AcquireRead(folly::CancellationToken(), 105);

    try {
        auto publish_lease = gate.AcquirePublish(nullptr, 105);
        FAIL() << "expected publication drain timeout";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyOtherException);
    }

    EXPECT_FALSE(gate.WriterPending());
    EXPECT_EQ(gate.PublishedGeneration(), 0);
    reader.reset();
    auto next_reader = gate.AcquireRead(folly::CancellationToken(), 105);
    EXPECT_TRUE(next_reader->valid());
}

TEST(SegmentReadGateTest, CrossSegmentRequestsReleaseAndRetryWithoutDeadlock) {
    SegmentReadGate first_gate;
    SegmentReadGate second_gate;
    auto first_request_lease =
        first_gate.AcquireRead(folly::CancellationToken(), 201);
    auto second_request_lease =
        second_gate.AcquireRead(folly::CancellationToken(), 202);

    std::atomic<bool> first_writer_acquired{false};
    std::atomic<bool> second_writer_acquired{false};
    std::thread first_writer([&] {
        auto publish_lease = first_gate.AcquirePublish(nullptr, 201);
        first_writer_acquired.store(true, std::memory_order_release);
    });
    std::thread second_writer([&] {
        auto publish_lease = second_gate.AcquirePublish(nullptr, 202);
        second_writer_acquired.store(true, std::memory_order_release);
    });

    EXPECT_TRUE(WaitUntil([&] { return first_gate.WriterPending(); }));
    EXPECT_TRUE(WaitUntil([&] { return second_gate.WriterPending(); }));

    EXPECT_THROW(second_gate.AcquireRead(folly::CancellationToken(), 202),
                 SegcoreError);
    EXPECT_THROW(first_gate.AcquireRead(folly::CancellationToken(), 201),
                 SegcoreError);

    // Whole-request retry first releases every lease acquired by the failed
    // attempt. Both publishers can then drain independently.
    first_request_lease.reset();
    second_request_lease.reset();
    first_writer.join();
    second_writer.join();

    EXPECT_TRUE(first_writer_acquired.load(std::memory_order_acquire));
    EXPECT_TRUE(second_writer_acquired.load(std::memory_order_acquire));
}

}  // namespace
}  // namespace milvus::segcore
