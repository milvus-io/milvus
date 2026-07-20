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

TEST(SegmentReadGateTest, PendingWriterBlocksNewReaders) {
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

    std::atomic<bool> second_reader_acquired{false};
    std::thread second_reader([&] {
        auto lease = gate.AcquireRead(folly::CancellationToken(), 100);
        second_reader_acquired.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(20ms);
    EXPECT_FALSE(second_reader_acquired.load(std::memory_order_acquire));
    EXPECT_EQ(gate.ActiveReaders(), 1);

    first_reader.reset();
    EXPECT_TRUE(WaitUntil(
        [&] { return writer_acquired.load(std::memory_order_acquire); }));
    EXPECT_FALSE(second_reader_acquired.load(std::memory_order_acquire));

    release_writer.store(true, std::memory_order_release);
    writer.join();
    second_reader.join();

    EXPECT_TRUE(second_reader_acquired.load(std::memory_order_acquire));
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

}  // namespace
}  // namespace milvus::segcore
