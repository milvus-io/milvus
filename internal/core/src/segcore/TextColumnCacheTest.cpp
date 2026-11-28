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
#include "cachinglayer/Utils.h"
#include <folly/OperationCancelled.h>
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

#include "common/EasyAssert.h"
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
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }

    bool
    WaitForCalls(int count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return calls_ >= count; });
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
        return arrow::Status::NotImplemented("sync batch must not be called");
    }

    folly::SemiFuture<arrow::Result<std::vector<std::string>>>
    ReadBatchAsync(const std::vector<EncodedRef>& refs) override {
        return folly::makeSemiFuture().deferValue(
            [tracker = tracker_,
             count = refs.size(),
             caller = std::this_thread::get_id()](
                folly::Unit) -> arrow::Result<std::vector<std::string>> {
                EXPECT_EQ(std::this_thread::get_id(), caller);
                tracker->ObserveCall();
                return std::vector<std::string>(count, "mock-text");
            });
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
ExpectConcurrentReads(const std::shared_ptr<BlockingCallTracker>& tracker,
                      Task task,
                      const std::shared_ptr<TextColumnCache>& cache) {
    auto first = std::async(std::launch::async, task);
    auto second = std::async(std::launch::async, task);
    const bool overlapping = tracker->WaitForCalls(2, std::chrono::seconds(5));
    // Eviction must not invalidate either admitted read.
    cache->Clear();
    tracker->Release();
    EXPECT_TRUE(overlapping);
    EXPECT_NO_THROW(first.get());
    EXPECT_NO_THROW(second.get());
}

TEST(TextColumnCache, ReadBatchAllowsConcurrentCallsOnCachedReader) {
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

    auto cached_reader =
        cache->GetOrCreateReader(lob_base_path, nullptr, properties);
    ASSERT_NE(cached_reader, nullptr);

    ExpectConcurrentReads(
        tracker,
        [=] {
            auto result =
                cache->ReadBatch(lob_base_path, nullptr, properties, *refs);
            EXPECT_EQ(result,
                      (std::vector<std::string>{"mock-text", "mock-text"}));
        },
        cache);

    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->CallCount(), 2);
}

TEST(TextColumnCache, ReadBatchIntoAllowsConcurrentCallsOnCachedReader) {
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

    auto cached_reader =
        cache->GetOrCreateReader(lob_base_path, nullptr, properties);
    ASSERT_NE(cached_reader, nullptr);

    ExpectConcurrentReads(
        tracker,
        [=] {
            google::protobuf::RepeatedPtrField<std::string> local_dst;
            local_dst.Add();
            local_dst.Add();
            cache->ReadBatchInto(
                lob_base_path, nullptr, properties, *refs, &local_dst);
            EXPECT_EQ(local_dst.Get(0), "mock-text");
            EXPECT_EQ(local_dst.Get(1), "mock-text");
        },
        cache);

    EXPECT_EQ(factory_calls->load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->CallCount(), 2);
}

class FailingAsyncReader : public InstrumentedLobColumnReader {
 public:
    explicit FailingAsyncReader(int mode)
        : InstrumentedLobColumnReader(std::make_shared<BlockingCallTracker>()),
          mode_(mode) {
    }

    folly::SemiFuture<arrow::Result<std::vector<std::string>>>
    ReadBatchAsync(const std::vector<EncodedRef>&) override {
        return folly::makeSemiFuture().deferValue(
            [mode = mode_](
                folly::Unit) -> arrow::Result<std::vector<std::string>> {
                if (mode == 0) {
                    return milvus_storage::MakeExtendError(
                        milvus_storage::ExtendStatusCode::
                            StorageTransientTimeout,
                        "async storage timeout");
                }
                if (mode == 1) {
                    throw SegcoreError(ErrorCode::StorageTransientError,
                                       "async backend exception");
                }
                if (mode == 2)
                    throw std::bad_alloc();
                if (mode == 3)
                    return arrow::Status::Invalid("corrupt LOB");
                if (mode == 4)
                    return arrow::Status::OutOfMemory("decoder OOM");
                throw folly::OperationCancelled();
            });
    }

 private:
    int mode_;
};

TEST(TextColumnCache, PreservesAsyncErrorsForBothReadAPIs) {
    for (int mode = 0; mode < 6; ++mode) {
        TextColumnCache cache(
            TextColumnCacheConfig{},
            [mode](std::shared_ptr<arrow::fs::FileSystem>,
                   const LobColumnConfig&)
                -> arrow::Result<std::unique_ptr<LobColumnReader>> {
                return std::unique_ptr<LobColumnReader>(
                    std::make_unique<FailingAsyncReader>(mode));
            });
        auto raw = MakeLobReference();
        std::vector<EncodedRef> refs{{raw.data(), raw.size()}};
        for (int api : {0, 1, 2}) {
            try {
                if (api == 1) {
                    google::protobuf::RepeatedPtrField<std::string> dst;
                    dst.Add();
                    cache.ReadBatchInto("lob", nullptr, nullptr, refs, &dst);
                } else if (api == 0) {
                    cache.ReadBatch("lob", nullptr, nullptr, refs);
                } else {
                    auto future =
                        cache.ReadBatchAsync("lob", nullptr, nullptr, refs);
                    cache.Clear();
                    cachinglayer::SemiInlineGet(std::move(future));
                }
                FAIL() << "expected async failure";
            } catch (const SegcoreError& error) {
                EXPECT_EQ(error.get_error_code(),
                          (mode == 2 || mode == 4)
                              ? ErrorCode::MemAllocateFailed
                          : mode == 3 ? ErrorCode::DataFormatBroken
                          : mode == 5 ? ErrorCode::FollyCancel
                                      : ErrorCode::StorageTransientError);
            }
        }
    }
}

TEST(TextColumnCache, SharesPropertiesWithReader) {
    auto properties = std::make_shared<const milvus_storage::api::Properties>();
    std::shared_ptr<const milvus_storage::api::Properties> reader_properties;
    auto tracker = std::make_shared<BlockingCallTracker>();
    TextColumnCache cache(
        TextColumnCacheConfig{},
        [&](std::shared_ptr<arrow::fs::FileSystem>,
            const LobColumnConfig& config)
            -> arrow::Result<std::unique_ptr<LobColumnReader>> {
            reader_properties = config.properties;
            return std::unique_ptr<LobColumnReader>(
                std::make_unique<InstrumentedLobColumnReader>(tracker));
        });
    cache.GetOrCreateReader("/tmp/text-column-cache", nullptr, properties);
    EXPECT_EQ(reader_properties, properties);
}

TEST(TextColumnCache, PreservesStorageErrorWhenCreatingReader) {
    auto status = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::StorageTransientTimeout,
        "storage timeout");
    TextColumnCache cache(
        TextColumnCacheConfig{},
        [status](std::shared_ptr<arrow::fs::FileSystem>, const LobColumnConfig&)
            -> arrow::Result<std::unique_ptr<LobColumnReader>> {
            return status;
        });
    auto properties = std::make_shared<const milvus_storage::api::Properties>();

    try {
        static_cast<void>(cache.GetOrCreateReader(
            "/tmp/text-column-cache", nullptr, properties));
        FAIL() << "expected storage error";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(),
                  milvus::ErrorCode::StorageTransientError);
        EXPECT_NE(std::string(error.what()).find("storage timeout"),
                  std::string::npos);
    }
}

}  // namespace
}  // namespace milvus::segcore
