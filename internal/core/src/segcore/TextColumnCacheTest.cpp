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
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
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

struct ConcurrentCallTracker {
    void
    ObserveCall() {
        auto active_now = active.fetch_add(1, std::memory_order_acq_rel) + 1;
        calls.fetch_add(1, std::memory_order_relaxed);

        auto observed = max_active.load(std::memory_order_relaxed);
        while (observed < active_now &&
               !max_active.compare_exchange_weak(
                   observed, active_now, std::memory_order_relaxed)) {
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        active.fetch_sub(1, std::memory_order_acq_rel);
    }

    std::atomic<int> active{0};
    std::atomic<int> max_active{0};
    std::atomic<int> calls{0};
};

std::vector<uint8_t>
Bytes(const std::string& value) {
    return {value.begin(), value.end()};
}

class InstrumentedLobColumnReader : public LobColumnReader {
 public:
    explicit InstrumentedLobColumnReader(
        std::shared_ptr<ConcurrentCallTracker> tracker)
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
    std::shared_ptr<ConcurrentCallTracker> tracker_;
    bool closed_{false};
};

TextLobReaderFactory
MakeReaderFactory(std::shared_ptr<ConcurrentCallTracker> tracker,
                  std::atomic<int>* factory_calls) {
    return [tracker = std::move(tracker), factory_calls](
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

std::vector<std::string>
RunConcurrently(size_t thread_count, const std::function<void(size_t)>& task) {
    std::mutex mutex;
    std::condition_variable cv;
    size_t ready = 0;
    bool start = false;
    std::vector<std::string> errors;
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    for (size_t i = 0; i < thread_count; ++i) {
        threads.emplace_back([&, i] {
            {
                std::unique_lock<std::mutex> lock(mutex);
                ++ready;
                if (ready == thread_count) {
                    cv.notify_one();
                }
                cv.wait(lock, [&] { return start; });
            }

            try {
                task(i);
            } catch (const std::exception& ex) {
                std::lock_guard<std::mutex> lock(mutex);
                errors.emplace_back(ex.what());
            } catch (...) {
                std::lock_guard<std::mutex> lock(mutex);
                errors.emplace_back("unknown exception");
            }
        });
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&] { return ready == thread_count; });
        start = true;
    }
    cv.notify_all();

    for (auto& thread : threads) {
        thread.join();
    }

    return errors;
}

TEST(TextColumnCache, ReadTextSerializesCallsOnCachedReader) {
    auto tracker = std::make_shared<ConcurrentCallTracker>();
    std::atomic<int> factory_calls{0};
    TextColumnCache cache(TextColumnCacheConfig{},
                          MakeReaderFactory(tracker, &factory_calls));
    milvus_storage::api::Properties properties;
    const std::string lob_base_path = "/tmp/text-column-cache";
    auto ref = MakeLobReference();

    ASSERT_NE(cache.GetOrCreateReader(lob_base_path, nullptr, properties),
              nullptr);

    constexpr size_t kThreadCount = 8;
    auto errors = RunConcurrently(kThreadCount, [&](size_t) {
        auto text = cache.ReadText(
            lob_base_path, nullptr, properties, ref.data(), ref.size());
        if (text != "mock-text") {
            throw std::runtime_error("unexpected text: " + text);
        }
    });

    if (!errors.empty()) {
        FAIL() << errors.front();
    }
    EXPECT_EQ(factory_calls.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->calls.load(std::memory_order_relaxed), kThreadCount);
    EXPECT_EQ(tracker->max_active.load(std::memory_order_relaxed), 1);
}

TEST(TextColumnCache, ReadBatchSerializesCallsOnCachedReader) {
    auto tracker = std::make_shared<ConcurrentCallTracker>();
    std::atomic<int> factory_calls{0};
    TextColumnCache cache(TextColumnCacheConfig{},
                          MakeReaderFactory(tracker, &factory_calls));
    milvus_storage::api::Properties properties;
    const std::string lob_base_path = "/tmp/text-column-cache";
    auto ref = MakeLobReference();
    std::vector<EncodedRef> refs = {{ref.data(), ref.size()},
                                    {ref.data(), ref.size()}};

    ASSERT_NE(cache.GetOrCreateReader(lob_base_path, nullptr, properties),
              nullptr);

    constexpr size_t kThreadCount = 8;
    auto errors = RunConcurrently(kThreadCount, [&](size_t) {
        auto texts = cache.ReadBatch(lob_base_path, nullptr, properties, refs);
        if (texts != std::vector<std::string>{"mock-text", "mock-text"}) {
            throw std::runtime_error("unexpected batch text");
        }
    });

    if (!errors.empty()) {
        FAIL() << errors.front();
    }
    EXPECT_EQ(factory_calls.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->calls.load(std::memory_order_relaxed), kThreadCount);
    EXPECT_EQ(tracker->max_active.load(std::memory_order_relaxed), 1);
}

TEST(TextColumnCache, ReadBatchIntoSerializesCallsOnCachedReader) {
    auto tracker = std::make_shared<ConcurrentCallTracker>();
    std::atomic<int> factory_calls{0};
    TextColumnCache cache(TextColumnCacheConfig{},
                          MakeReaderFactory(tracker, &factory_calls));
    milvus_storage::api::Properties properties;
    const std::string lob_base_path = "/tmp/text-column-cache";
    auto ref = MakeLobReference();
    std::vector<EncodedRef> refs = {{ref.data(), ref.size()},
                                    {ref.data(), ref.size()}};

    ASSERT_NE(cache.GetOrCreateReader(lob_base_path, nullptr, properties),
              nullptr);

    constexpr size_t kThreadCount = 8;
    auto errors = RunConcurrently(kThreadCount, [&](size_t) {
        google::protobuf::RepeatedPtrField<std::string> dst;
        dst.Add();
        dst.Add();
        cache.ReadBatchInto(lob_base_path, nullptr, properties, refs, &dst);
        if (dst.size() != 2 || dst.Get(0) != "mock-text" ||
            dst.Get(1) != "mock-text") {
            throw std::runtime_error("unexpected batch-into text");
        }
    });

    if (!errors.empty()) {
        FAIL() << errors.front();
    }
    EXPECT_EQ(factory_calls.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(tracker->calls.load(std::memory_order_relaxed), kThreadCount);
    EXPECT_EQ(tracker->max_active.load(std::memory_order_relaxed), 1);
}

}  // namespace
}  // namespace milvus::segcore
