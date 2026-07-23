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

#include "segcore/TextLobReader.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <gtest/gtest.h>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/lob_column/lob_reference.h"

namespace milvus::segcore {
namespace {

using milvus_storage::lob_column::EncodedRef;
using milvus_storage::lob_column::FLAG_LOB_REFERENCE;
using milvus_storage::lob_column::LOB_REFERENCE_SIZE;
using milvus_storage::lob_column::LobColumnConfig;
using milvus_storage::lob_column::LobColumnReader;

class BlockingCallTracker {
 public:
    void
    ObserveCall() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++calls_;
        entered_ = true;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }

    bool
    WaitForEntered(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return entered_; });
    }

    void
    Release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        cv_.notify_all();
    }

    int
    CallCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

 private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_{false};
    bool released_{false};
    int calls_{0};
};

std::vector<uint8_t>
Bytes(const std::string& value) {
    return {value.begin(), value.end()};
}

class InstrumentedLobColumnReader : public LobColumnReader {
 public:
    explicit InstrumentedLobColumnReader(
        std::shared_ptr<BlockingCallTracker> tracker)
        : tracker_(std::move(tracker)) {
    }

    arrow::Result<std::vector<uint8_t>>
    ReadData(const uint8_t*, size_t) override {
        tracker_->ObserveCall();
        return Bytes("mock-text");
    }

    arrow::Result<std::vector<std::vector<uint8_t>>>
    ReadBatchData(const std::vector<EncodedRef>& encoded_refs) override {
        tracker_->ObserveCall();
        return std::vector<std::vector<uint8_t>>(encoded_refs.size(),
                                                 Bytes("mock-text"));
    }

    arrow::Result<std::shared_ptr<arrow::BinaryArray>>
    ReadArrowArray(const std::shared_ptr<arrow::BinaryArray>&) override {
        return arrow::Status::NotImplemented("unused");
    }

    arrow::Result<std::vector<std::vector<uint8_t>>>
    TakeData(const std::string&, const std::vector<int32_t>&) override {
        return arrow::Status::NotImplemented("unused");
    }

    arrow::Status
    Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }

    bool
    IsClosed() const override {
        return closed_;
    }

    void
    ClearCache() override {
    }

 private:
    std::shared_ptr<BlockingCallTracker> tracker_;
    bool closed_{false};
};

class FailingLobColumnReader : public LobColumnReader {
 public:
    explicit FailingLobColumnReader(arrow::Status status)
        : status_(std::move(status)) {
    }

    arrow::Result<std::vector<uint8_t>>
    ReadData(const uint8_t*, size_t) override {
        return status_;
    }

    arrow::Result<std::vector<std::vector<uint8_t>>>
    ReadBatchData(const std::vector<EncodedRef>&) override {
        return status_;
    }

    arrow::Result<std::shared_ptr<arrow::BinaryArray>>
    ReadArrowArray(const std::shared_ptr<arrow::BinaryArray>&) override {
        return status_;
    }

    arrow::Result<std::vector<std::vector<uint8_t>>>
    TakeData(const std::string&, const std::vector<int32_t>&) override {
        return status_;
    }

    arrow::Status
    Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }

    bool
    IsClosed() const override {
        return closed_;
    }

    void
    ClearCache() override {
    }

 private:
    arrow::Status status_;
    bool closed_{false};
};

class ShortBatchLobColumnReader : public LobColumnReader {
 public:
    arrow::Result<std::vector<uint8_t>>
    ReadData(const uint8_t*, size_t) override {
        return Bytes("mock-text");
    }

    arrow::Result<std::vector<std::vector<uint8_t>>>
    ReadBatchData(const std::vector<EncodedRef>& encoded_refs) override {
        auto result_size = encoded_refs.empty() ? 0 : encoded_refs.size() - 1;
        return std::vector<std::vector<uint8_t>>(result_size,
                                                 Bytes("mock-text"));
    }

    arrow::Result<std::shared_ptr<arrow::BinaryArray>>
    ReadArrowArray(const std::shared_ptr<arrow::BinaryArray>&) override {
        return arrow::Status::NotImplemented("unused");
    }

    arrow::Result<std::vector<std::vector<uint8_t>>>
    TakeData(const std::string&, const std::vector<int32_t>&) override {
        return arrow::Status::NotImplemented("unused");
    }

    arrow::Status
    Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }

    bool
    IsClosed() const override {
        return closed_;
    }

    void
    ClearCache() override {
    }

 private:
    bool closed_{false};
};

TextLobReaderFactory
MakeReaderFactory(std::shared_ptr<BlockingCallTracker> tracker,
                  std::shared_ptr<std::atomic<int>> factory_calls) {
    return [tracker = std::move(tracker),
            factory_calls = std::move(factory_calls)](
               std::shared_ptr<arrow::fs::FileSystem>, const LobColumnConfig&)
               -> arrow::Result<std::unique_ptr<LobColumnReader>> {
        factory_calls->fetch_add(1, std::memory_order_relaxed);
        std::unique_ptr<LobColumnReader> reader =
            std::make_unique<InstrumentedLobColumnReader>(tracker);
        return std::move(reader);
    };
}

std::vector<uint8_t>
MakeLobReference() {
    std::vector<uint8_t> ref(LOB_REFERENCE_SIZE, 0);
    ref[0] = FLAG_LOB_REFERENCE;
    return ref;
}

template <typename Task>
void
ExpectTaskUsesReaderLock(
    const std::shared_ptr<SharedTextLobReader>& shared_reader,
    const std::shared_ptr<BlockingCallTracker>& tracker,
    Task task) {
    constexpr auto kBlockedCheck = std::chrono::seconds(1);
    constexpr auto kTimeout = std::chrono::seconds(5);

    std::unique_lock<std::mutex> reader_lock(shared_reader->mutex);
    auto started = std::make_shared<std::promise<void>>();
    auto done = std::make_shared<std::promise<void>>();
    auto started_future = started->get_future();
    auto done_future = done->get_future();

    std::thread worker([started, done, task = std::move(task)]() mutable {
        try {
            started->set_value();
            task();
            done->set_value();
        } catch (...) {
            done->set_exception(std::current_exception());
        }
    });

    if (started_future.wait_for(kTimeout) != std::future_status::ready) {
        tracker->Release();
        worker.detach();
        FAIL() << "read worker did not start";
    }

    if (tracker->WaitForEntered(kBlockedCheck)) {
        tracker->Release();
        reader_lock.unlock();
        worker.join();
        FAIL() << "read reached LobColumnReader while shared reader mutex "
                  "was held";
    }

    reader_lock.unlock();

    if (!tracker->WaitForEntered(kTimeout)) {
        tracker->Release();
        worker.detach();
        FAIL() << "read did not reach LobColumnReader after shared reader "
                  "mutex was released";
    }

    tracker->Release();

    if (done_future.wait_for(kTimeout) != std::future_status::ready) {
        worker.detach();
        FAIL() << "read did not complete after LobColumnReader was released";
    }

    worker.join();
    try {
        done_future.get();
    } catch (const std::exception& ex) {
        FAIL() << ex.what();
    } catch (...) {
        FAIL() << "unknown exception";
    }
}

TEST(TextLobReaderRegistry, ReadTextSerializesCallsOnSharedReader) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    auto registry = std::make_shared<TextLobReaderRegistry>(
        MakeReaderFactory(tracker, factory_calls));
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    const std::string lob_base_path = "/tmp/shared-text-lob-reader";
    auto ref = std::make_shared<std::vector<uint8_t>>(MakeLobReference());
    auto text = std::make_shared<std::string>();

    auto shared_reader = registry->Acquire(lob_base_path, nullptr, *properties);
    ASSERT_NE(shared_reader, nullptr);

    ExpectTaskUsesReaderLock(shared_reader, tracker, [=] {
        *text = shared_reader->ReadText(
            nullptr, *properties, ref->data(), ref->size());
    });

    EXPECT_EQ(*text, "mock-text");
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->CallCount(), 1);
}

TEST(TextLobReaderRegistry, ReadBatchSerializesCallsOnSharedReader) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    auto registry = std::make_shared<TextLobReaderRegistry>(
        MakeReaderFactory(tracker, factory_calls));
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    const std::string lob_base_path = "/tmp/shared-text-lob-reader";
    auto ref = std::make_shared<std::vector<uint8_t>>(MakeLobReference());
    auto refs =
        std::make_shared<std::vector<EncodedRef>>(std::vector<EncodedRef>{
            {ref->data(), ref->size()}, {ref->data(), ref->size()}});
    auto texts = std::make_shared<std::vector<std::string>>();

    auto shared_reader = registry->Acquire(lob_base_path, nullptr, *properties);
    ASSERT_NE(shared_reader, nullptr);

    ExpectTaskUsesReaderLock(shared_reader, tracker, [=] {
        *texts = shared_reader->ReadBatch(nullptr, *properties, *refs);
    });

    EXPECT_EQ(*texts, (std::vector<std::string>{"mock-text", "mock-text"}));
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->CallCount(), 1);
}

TEST(TextLobReaderRegistry, ReadBatchIntoSerializesCallsOnSharedReader) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    auto registry = std::make_shared<TextLobReaderRegistry>(
        MakeReaderFactory(tracker, factory_calls));
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    const std::string lob_base_path = "/tmp/shared-text-lob-reader";
    auto ref = std::make_shared<std::vector<uint8_t>>(MakeLobReference());
    auto refs =
        std::make_shared<std::vector<EncodedRef>>(std::vector<EncodedRef>{
            {ref->data(), ref->size()}, {ref->data(), ref->size()}});
    auto dst =
        std::make_shared<google::protobuf::RepeatedPtrField<std::string>>();
    dst->Add();
    dst->Add();

    auto shared_reader = registry->Acquire(lob_base_path, nullptr, *properties);
    ASSERT_NE(shared_reader, nullptr);

    ExpectTaskUsesReaderLock(shared_reader, tracker, [=] {
        shared_reader->ReadBatchInto(nullptr, *properties, *refs, dst.get());
    });

    ASSERT_EQ(dst->size(), 2);
    EXPECT_EQ(dst->Get(0), "mock-text");
    EXPECT_EQ(dst->Get(1), "mock-text");
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->CallCount(), 1);
}

TEST(TextLobReaderRegistry, SharesReadersWithoutOwningTheirLifetime) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    TextLobReaderRegistry registry(MakeReaderFactory(tracker, factory_calls));
    const std::string lob_base_path = "/tmp/shared-text-lob-reader";

    auto first = registry.AcquireHandle(lob_base_path);
    auto second = registry.AcquireHandle(lob_base_path);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first, second);
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 0);
    EXPECT_EQ(registry.LiveReaderCountForTesting(), 1);

    std::weak_ptr<SharedTextLobReader> weak = first;
    first.reset();
    second.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(registry.LiveReaderCountForTesting(), 0);

    auto replacement = registry.AcquireHandle(lob_base_path);
    EXPECT_NE(replacement, nullptr);
    EXPECT_EQ(registry.LiveReaderCountForTesting(), 1);
}

TEST(TextLobReaderRegistry, ConcurrentHandleCreationIsDeduplicated) {
    TextLobReaderRegistry registry;
    const std::string lob_base_path = "/tmp/concurrent-text-lob-reader";
    constexpr size_t kThreadCount = 16;
    std::vector<std::shared_ptr<SharedTextLobReader>> handles(kThreadCount);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back(
            [&, i] { handles[i] = registry.AcquireHandle(lob_base_path); });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_NE(handles.front(), nullptr);
    for (const auto& handle : handles) {
        EXPECT_EQ(handle, handles.front());
    }
    EXPECT_EQ(registry.LiveReaderCountForTesting(), 1);
}

TEST(TextLobReaderRegistry, ReaderInitializationIsLazyAndShared) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    TextLobReaderRegistry registry(MakeReaderFactory(tracker, factory_calls));
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    const std::string lob_base_path = "/tmp/lazy-text-lob-reader";

    auto handle = registry.AcquireHandle(lob_base_path);
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 0);

    auto initialized = registry.Acquire(lob_base_path, nullptr, *properties);
    EXPECT_EQ(initialized, handle);
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);

    auto initialized_again =
        registry.Acquire(lob_base_path, nullptr, *properties);
    EXPECT_EQ(initialized_again, handle);
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);
}

TEST(TextLobReaderRegistry, LiveReadersHaveNoCapacityLimit) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    TextLobReaderRegistry registry(MakeReaderFactory(tracker, factory_calls));
    auto properties = std::make_shared<milvus_storage::api::Properties>();

    constexpr size_t kReaderCount = 256;
    std::vector<std::shared_ptr<SharedTextLobReader>> readers;
    readers.reserve(kReaderCount);
    for (size_t i = 0; i < kReaderCount; ++i) {
        auto reader = registry.Acquire(
            "/tmp/live-text-reader-" + std::to_string(i), nullptr, *properties);
        ASSERT_NE(reader, nullptr);
        ASSERT_NE(reader->reader, nullptr);
        readers.push_back(std::move(reader));
    }

    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), kReaderCount);
    EXPECT_EQ(registry.LiveReaderCountForTesting(), kReaderCount);

    std::weak_ptr<SharedTextLobReader> first = readers.front();
    std::weak_ptr<LobColumnReader> first_reader = readers.front()->reader;
    readers.clear();
    EXPECT_TRUE(first.expired());
    EXPECT_TRUE(first_reader.expired());
    EXPECT_EQ(registry.LiveReaderCountForTesting(), 0);
}

TEST(TextLobReaderRegistry, MalformedRemoteReferenceIsDataFormatBroken) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    TextLobReaderRegistry registry(MakeReaderFactory(tracker, factory_calls));
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    auto reader = registry.AcquireHandle("/tmp/malformed-text-ref");
    std::array<uint8_t, 2> malformed_ref = {FLAG_LOB_REFERENCE, 0};

    try {
        reader->ReadText(
            nullptr, *properties, malformed_ref.data(), malformed_ref.size());
        FAIL() << "expected malformed TEXT reference error";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), DataFormatBroken);
    }
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 0);
}

TEST(TextLobReaderRegistry, StorageErrorCodesSurviveReaderBoundary) {
    auto properties = std::make_shared<milvus_storage::api::Properties>();

    TextLobReaderFactory permanent_factory =
        [](std::shared_ptr<arrow::fs::FileSystem>, const LobColumnConfig&)
        -> arrow::Result<std::unique_ptr<LobColumnReader>> {
        return arrow::Status::IOError("permanent reader creation failure");
    };
    TextLobReaderRegistry permanent_registry(std::move(permanent_factory));
    try {
        permanent_registry.Acquire(
            "/tmp/permanent-text-reader", nullptr, *properties);
        FAIL() << "expected storage error";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), StorageError);
    }

    auto transient_status = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::StorageTransientThrottling,
        "throttled");
    TextLobReaderFactory transient_factory =
        [transient_status](std::shared_ptr<arrow::fs::FileSystem>,
                           const LobColumnConfig&)
        -> arrow::Result<std::unique_ptr<LobColumnReader>> {
        std::unique_ptr<LobColumnReader> reader =
            std::make_unique<FailingLobColumnReader>(transient_status);
        return std::move(reader);
    };
    TextLobReaderRegistry transient_registry(std::move(transient_factory));
    auto ref = MakeLobReference();
    try {
        auto reader = transient_registry.Acquire(
            "/tmp/transient-text-reader", nullptr, *properties);
        reader->ReadBatch(nullptr,
                          *properties,
                          std::vector<EncodedRef>{{ref.data(), ref.size()}});
        FAIL() << "expected transient storage error";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), StorageTransientError);
    }
}

TEST(TextLobReaderRegistry, BatchResultSizeMismatchIsInternalError) {
    TextLobReaderFactory factory = [](std::shared_ptr<arrow::fs::FileSystem>,
                                      const LobColumnConfig&)
        -> arrow::Result<std::unique_ptr<LobColumnReader>> {
        std::unique_ptr<LobColumnReader> reader =
            std::make_unique<ShortBatchLobColumnReader>();
        return std::move(reader);
    };
    TextLobReaderRegistry registry(std::move(factory));
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    auto reader =
        registry.Acquire("/tmp/short-batch-text-reader", nullptr, *properties);
    auto ref = MakeLobReference();
    std::vector<EncodedRef> refs = {{ref.data(), ref.size()},
                                    {ref.data(), ref.size()}};

    try {
        reader->ReadBatch(nullptr, *properties, refs);
        FAIL() << "expected batch-size contract error";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), UnexpectedError);
    }

    google::protobuf::RepeatedPtrField<std::string> dst;
    dst.Add();
    dst.Add();
    try {
        reader->ReadBatchInto(nullptr, *properties, refs, &dst);
        FAIL() << "expected batch-size contract error";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), UnexpectedError);
    }
}

}  // namespace
}  // namespace milvus::segcore
