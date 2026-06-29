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

#include "segcore/TextColumnCache.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <gtest/gtest.h>
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
ExpectTaskUsesCachedReaderLock(
    const std::shared_ptr<CachedTextLobReader>& cached_reader,
    const std::shared_ptr<BlockingCallTracker>& tracker,
    Task task) {
    constexpr auto kBlockedCheck = std::chrono::seconds(1);
    constexpr auto kTimeout = std::chrono::seconds(5);

    std::unique_lock<std::mutex> reader_lock(cached_reader->mutex);
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
        FAIL() << "read reached LobColumnReader while cached reader mutex "
                  "was held";
    }

    reader_lock.unlock();

    if (!tracker->WaitForEntered(kTimeout)) {
        tracker->Release();
        worker.detach();
        FAIL() << "read did not reach LobColumnReader after cached reader "
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

TEST(TextColumnCache, ReadTextSerializesCallsOnCachedReader) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    auto cache = std::make_shared<TextColumnCache>(
        TextColumnCacheConfig{}, MakeReaderFactory(tracker, factory_calls));
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    const std::string lob_base_path = "/tmp/text-column-cache";
    auto ref = std::make_shared<std::vector<uint8_t>>(MakeLobReference());
    auto text = std::make_shared<std::string>();

    auto cached_reader =
        cache->GetOrCreateReader(lob_base_path, nullptr, *properties);
    ASSERT_NE(cached_reader, nullptr);

    ExpectTaskUsesCachedReaderLock(cached_reader, tracker, [=] {
        *text = cache->ReadText(
            lob_base_path, nullptr, *properties, ref->data(), ref->size());
    });

    EXPECT_EQ(*text, "mock-text");
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->CallCount(), 1);
}

TEST(TextColumnCache, ReadBatchSerializesCallsOnCachedReader) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    auto cache = std::make_shared<TextColumnCache>(
        TextColumnCacheConfig{}, MakeReaderFactory(tracker, factory_calls));
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    const std::string lob_base_path = "/tmp/text-column-cache";
    auto ref = std::make_shared<std::vector<uint8_t>>(MakeLobReference());
    auto refs =
        std::make_shared<std::vector<EncodedRef>>(std::vector<EncodedRef>{
            {ref->data(), ref->size()}, {ref->data(), ref->size()}});
    auto texts = std::make_shared<std::vector<std::string>>();

    auto cached_reader =
        cache->GetOrCreateReader(lob_base_path, nullptr, *properties);
    ASSERT_NE(cached_reader, nullptr);

    ExpectTaskUsesCachedReaderLock(cached_reader, tracker, [=] {
        *texts = cache->ReadBatch(lob_base_path, nullptr, *properties, *refs);
    });

    EXPECT_EQ(*texts, (std::vector<std::string>{"mock-text", "mock-text"}));
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->CallCount(), 1);
}

TEST(TextColumnCache, ReadBatchIntoSerializesCallsOnCachedReader) {
    auto tracker = std::make_shared<BlockingCallTracker>();
    auto factory_calls = std::make_shared<std::atomic<int>>(0);
    auto cache = std::make_shared<TextColumnCache>(
        TextColumnCacheConfig{}, MakeReaderFactory(tracker, factory_calls));
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    const std::string lob_base_path = "/tmp/text-column-cache";
    auto ref = std::make_shared<std::vector<uint8_t>>(MakeLobReference());
    auto refs =
        std::make_shared<std::vector<EncodedRef>>(std::vector<EncodedRef>{
            {ref->data(), ref->size()}, {ref->data(), ref->size()}});
    auto dst =
        std::make_shared<google::protobuf::RepeatedPtrField<std::string>>();
    dst->Add();
    dst->Add();

    auto cached_reader =
        cache->GetOrCreateReader(lob_base_path, nullptr, *properties);
    ASSERT_NE(cached_reader, nullptr);

    ExpectTaskUsesCachedReaderLock(cached_reader, tracker, [=] {
        cache->ReadBatchInto(
            lob_base_path, nullptr, *properties, *refs, dst.get());
    });

    ASSERT_EQ(dst->size(), 2);
    EXPECT_EQ(dst->Get(0), "mock-text");
    EXPECT_EQ(dst->Get(1), "mock-text");
    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->CallCount(), 1);
}

}  // namespace
}  // namespace milvus::segcore
