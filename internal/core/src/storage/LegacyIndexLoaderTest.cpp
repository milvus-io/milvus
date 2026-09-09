// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>
#include <future>
#include <numeric>
#include "folly/ScopeGuard.h"
#include "folly/system/ThreadName.h"
#include "folly/coro/Baton.h"
#include "storage/Event.h"
#include "storage/LegacyIndexLoader.h"
#include "storage/PluginLoader.h"
#include "storage/Util.h"
#include "test_utils/AsyncLoadTestUtils.h"
#include "test_utils/PlannerCipherPlugin.h"

namespace milvus::storage {
namespace {
constexpr auto kPriority = proto::common::LoadPriority::HIGH;

std::vector<uint8_t>
Encode(const std::vector<uint8_t>& payload,
       DataType type = DataType::NONE,
       std::shared_ptr<CPluginContext> encryption = {}) {
    std::unique_ptr<IndexData> codec;
    const uint8_t empty_payload = 0;
    if (type == DataType::NONE) {
        codec = std::make_unique<IndexData>(
            payload.empty() ? &empty_payload : payload.data(), payload.size());
    } else {
        auto field = CreateFieldData(type, DataType::NONE, false);
        if (type == DataType::STRING) {
            const std::string value(payload.begin(), payload.end());
            field->FillFieldData(&value, 1);
        } else {
            field->FillFieldData(payload.data(), payload.size());
        }
        auto reader = std::make_shared<PayloadReader>(field);
        codec = std::make_unique<IndexData>(reader);
    }
    codec->SetFieldDataMeta({1, 2, 3, 101});
    codec->set_index_meta({3, 101, 1000, 1});
    return codec->serialize_to_remote_file(std::move(encryption));
}

class LegacyIndexLoaderTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        old_slots_ = budget_.CapacitySlots();
        old_threads_ = GetAsyncLoadThreadPoolSize();
        budget_.SetCapacitySlots(1);
        SetAsyncLoadThreadPoolSize(1);
    }
    void
    TearDown() override {
        const bool acquired =
            budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High);
        EXPECT_TRUE(acquired);
        if (acquired) {
            budget_.Release({1, 1});
        }
        budget_.SetCapacitySlots(old_slots_);
        SetAsyncLoadThreadPoolSize(old_threads_);
    }
    void
    ExpectReserved() {
        const bool acquired =
            budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High);
        if (acquired) {
            budget_.Release({1, 1});
        }
        EXPECT_FALSE(acquired);
    }
    template <typename T>
    T
    Run(folly::coro::Task<T> task) {
        return folly::coro::blockingWait(std::move(task).scheduleOn(
            ResolveAsyncLoadExecutor({}, kPriority)));
    }
    std::shared_ptr<RemoteInputStream>
    Open(std::vector<uint8_t> content) {
        file_ = std::make_shared<test::ControlledDirectReadFile>(
            std::move(content));
        std::shared_ptr<arrow::io::RandomAccessFile> arrow_file = file_;
        return std::make_shared<RemoteInputStream>(std::move(arrow_file));
    }
    std::vector<uint8_t>
    Read(RemoteInputStream& input, const LegacyIndexFileInfo& info) {
        std::vector<uint8_t> output;
        output.reserve(info.payload_bytes);
        LegacyIndexConsumer consumer =
            [&](size_t offset,
                std::span<const uint8_t> bytes) -> folly::coro::Task<void> {
            EXPECT_EQ(offset, output.size());
            ExpectReserved();
            if (!bytes.empty()) {
                // Zero extra slots isolates the byte limit from the slot limit.
                const LoadAdmissionRequest request{budget_.CapacityBytes(), 0};
                const bool acquired =
                    budget_.TryAcquire(request, LoadAdmissionPriority::High);
                if (acquired) {
                    budget_.Release(request);
                }
                EXPECT_FALSE(acquired);
            }
            EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
                "MILVUS_ASYNC"));
            output.insert(output.end(), bytes.begin(), bytes.end());
            co_return;
        };
        Run(StreamLegacyIndexFileAsync(input, info, consumer, kPriority));
        return output;
    }
    LoadAdmissionController& budget_ = LoadAdmissionController::GetInstance();
    test::ScopedLoadTransientBudget bytes_guard_{1024 * 1024};
    size_t old_slots_{};
    int old_threads_{};
    std::shared_ptr<test::ControlledDirectReadFile> file_;
};

TEST_F(LegacyIndexLoaderTest, RawRangesAndEmptyPayload) {
    for (const size_t size : {size_t{0}, DefaultStreamSliceSize() * 2 + 19}) {
        std::vector<uint8_t> payload(size);
        std::iota(payload.begin(), payload.end(), uint8_t{0});
        auto input = Open(Encode(payload));
        auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
        ASSERT_TRUE(info.raw_payload);
        ASSERT_EQ(info.payload_bytes, size);
        file_->ResetCounters();
        EXPECT_EQ(Read(*input, info), payload);
        for (const auto& call : file_->DirectReadCalls()) {
            EXPECT_LE(call.nbytes, DefaultStreamSliceSize());
        }
        EXPECT_EQ(file_->DirectReadCalls().size(), size == 0 ? 0 : 3);
        EXPECT_LE(file_->PeakInflight(), 1);
        EXPECT_EQ(file_->ReadAtCalls(), 0);
        EXPECT_EQ(file_->AsyncReadCalls(), 0);
    }
}

TEST_F(LegacyIndexLoaderTest, ParquetInt8AndOldString) {
    for (const auto& payload :
         std::vector<std::vector<uint8_t>>{{}, {0, 127, 128, 255, 0, 42}}) {
        for (const auto type : {DataType::INT8, DataType::STRING}) {
            auto input = Open(Encode(payload, type));
            auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
            ASSERT_FALSE(info.raw_payload);
            EXPECT_GE(info.max_transient_bytes,
                      info.file_bytes + info.payload_bytes);
            EXPECT_EQ(Read(*input, info), payload);
        }
    }
}

TEST_F(LegacyIndexLoaderTest, EncryptedEnvelopeUsesExistingDecoder) {
    auto& plugins = PluginLoader::GetInstance();
    plugins.registerPluginForTest(
        std::make_shared<test::CollectionBoundPlannerCipherPlugin>(1));
    auto cleanup = folly::makeGuard(
        [&] { plugins.unregisterPluginForTest("CipherPlugin"); });
    auto encryption = std::make_shared<CPluginContext>();
    encryption->collection_id = 1;
    encryption->ez_id = 2;
    const std::vector<uint8_t> payload{0, 9, 255, 42};
    for (const auto type : {DataType::NONE, DataType::INT8, DataType::STRING}) {
        auto input = Open(Encode(payload, type, encryption));
        auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
        ASSERT_FALSE(info.raw_payload);
        EXPECT_EQ(Read(*input, info), payload);
    }
}

TEST_F(LegacyIndexLoaderTest, BufferedArrowFallback) {
    const std::vector<uint8_t> payload{0, 1, 2, 255};
    auto file =
        std::make_shared<test::AsyncTrackingRandomAccessFile>(Encode(payload));
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file = file;
    RemoteInputStream input(std::move(arrow_file));
    auto info = Run(InspectLegacyIndexFileAsync(input, kPriority));
    EXPECT_EQ(Read(input, info), payload);
    EXPECT_GT(file->AsyncReadCalls(), 0);
    EXPECT_EQ(file->ReadAtCalls(), 0);
}

TEST_F(LegacyIndexLoaderTest, ConcurrentLoadMakesProgressWithOneWorkerAndSlot) {
    auto input = Open(Encode({1, 2, 3}));
    auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
    folly::coro::Baton release;
    std::promise<void> started;
    auto started_future = started.get_future();
    LegacyIndexConsumer consumer =
        [&](size_t, std::span<const uint8_t> bytes) -> folly::coro::Task<void> {
        started.set_value();
        co_await release;
        EXPECT_EQ(bytes[2], 3);
    };
    auto load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFileAsync(*input, info, consumer, kPriority));
    });
    std::future<LegacyIndexFileInfo> sibling;
    auto cleanup = folly::makeGuard([&] {
        release.post();
        if (load.valid()) {
            load.wait();
        }
        if (sibling.valid()) {
            sibling.wait();
        }
    });
    ASSERT_EQ(started_future.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    ExpectReserved();
    sibling = std::async(std::launch::async, [&] {
        return Run(InspectLegacyIndexFileAsync(*input, kPriority));
    });
    EXPECT_EQ(sibling.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    release.post();
    EXPECT_NO_THROW(load.get());
    EXPECT_TRUE(sibling.get().raw_payload);
    cleanup.dismiss();
}

TEST_F(LegacyIndexLoaderTest, CancelWaitingAdmissionBeforeAnyRead) {
    auto input = Open(Encode({1, 2, 3}));
    auto lease = Run([&]() -> folly::coro::Task<LoadAdmissionLease> {
        co_return co_await budget_.AcquireAsync({1, 1},
                                                LoadAdmissionPriority::High);
    }());
    folly::CancellationSource cancel;
    auto task = std::async(std::launch::async, [&] {
        return Run(
            InspectLegacyIndexFileAsync(*input, kPriority, cancel.getToken()));
    });
    EXPECT_EQ(task.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    cancel.requestCancellation();
    EXPECT_THROW(task.get(), SegcoreError);
    EXPECT_TRUE(file_->DirectReadCalls().empty());
}

TEST_F(LegacyIndexLoaderTest, CancellationDrainsIssuedRead) {
    auto input = Open(Encode({1, 2, 3}));
    auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
    file_->ResetCounters();
    file_->SetAutoComplete(false);
    folly::CancellationSource cancel;
    bool consumed = false;
    LegacyIndexConsumer consumer =
        [&](size_t, std::span<const uint8_t>) -> folly::coro::Task<void> {
        consumed = true;
        co_return;
    };
    auto load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFileAsync(
            *input, info, consumer, kPriority, cancel.getToken()));
    });
    auto cleanup = folly::makeGuard([&] {
        if (file_->WaitForCallCount(1)) {
            file_->Complete(0);
        }
        load.wait();
    });
    ASSERT_TRUE(file_->WaitForCallCount(1));
    cancel.requestCancellation();
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    ExpectReserved();
    file_->Complete(0);
    EXPECT_THROW(load.get(), SegcoreError);
    EXPECT_FALSE(consumed);
    cleanup.dismiss();
}

TEST_F(LegacyIndexLoaderTest, ReadAndConsumerFailuresReleaseAdmission) {
    auto input = Open(Encode({1, 2, 3}));
    auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
    file_->SetNextCompletion(arrow::Status::IOError("injected read failure"));
    EXPECT_THROW(Read(*input, info), SegcoreError);
    LegacyIndexConsumer fail =
        [](size_t, std::span<const uint8_t>) -> folly::coro::Task<void> {
        ThrowInfo(MemAllocateFailed, "injected consumer failure");
        co_return;
    };
    try {
        Run(StreamLegacyIndexFileAsync(*input, info, fail, kPriority));
        FAIL() << "expected consumer failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), MemAllocateFailed);
    }
}

TEST_F(LegacyIndexLoaderTest, DecoderFailureReleasesAdmission) {
    const std::vector<uint8_t> payload(1024, 42);
    auto encoded = Encode(payload, DataType::INT8);
    auto input = Open(encoded);
    const auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
    // Damage the Parquet footer after successful envelope inspection.
    file_->CorruptRemoteByte(encoded.size() - 1);
    EXPECT_THROW(Read(*input, info), SegcoreError);
}

TEST_F(LegacyIndexLoaderTest, EncodedEventLengthValidatedBeforeDecoding) {
    const std::vector<uint8_t> payload(1024, 42);
    auto& plugins = PluginLoader::GetInstance();
    plugins.registerPluginForTest(
        std::make_shared<test::CollectionBoundPlannerCipherPlugin>(1));
    auto cleanup = folly::makeGuard(
        [&] { plugins.unregisterPluginForTest("CipherPlugin"); });
    auto encryption = std::make_shared<CPluginContext>();
    encryption->collection_id = 1;
    encryption->ez_id = 2;
    // The identity cipher lets us corrupt the decrypted event header directly.
    for (bool encrypted : {false, true}) {
        auto encoded =
            Encode(payload, DataType::INT8, encrypted ? encryption : nullptr);
        auto input = Open(encoded);
        const auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
        const int32_t invalid_length = -1;
        std::memcpy(encoded.data() + info.payload_offset + sizeof(Timestamp) +
                        sizeof(EventType),
                    &invalid_length,
                    sizeof(invalid_length));
        input = Open(std::move(encoded));
        EXPECT_THROW(Read(*input, info), SegcoreError);
    }
}

TEST_F(LegacyIndexLoaderTest, MissingDecryptorReleasesAdmission) {
    auto& plugins = PluginLoader::GetInstance();
    plugins.registerPluginForTest(
        std::make_shared<test::CollectionBoundPlannerCipherPlugin>(1));
    auto cleanup = folly::makeGuard(
        [&] { plugins.unregisterPluginForTest("CipherPlugin"); });
    auto encryption = std::make_shared<CPluginContext>();
    encryption->collection_id = 1;
    encryption->ez_id = 2;
    auto input = Open(Encode({1, 2, 3}, DataType::NONE, encryption));
    const auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
    plugins.unregisterPluginForTest("CipherPlugin");
    EXPECT_THROW(Read(*input, info), SegcoreError);
}

TEST_F(LegacyIndexLoaderTest, RejectMalformedDescriptorAndRawEvent) {
    auto encoded = Encode({1, 2, 3});
    for (const size_t size : {size_t{0}, size_t{20}}) {
        auto input =
            Open(std::vector<uint8_t>(encoded.begin(), encoded.begin() + size));
        EXPECT_THROW(Run(InspectLegacyIndexFileAsync(*input, kPriority)),
                     SegcoreError);
    }
    auto input = Open(encoded);
    const auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
    // Corrupt the raw event length, keeping descriptor metadata intact.
    EventHeader header;
    const size_t event_offset = info.payload_offset -
                                GetEventHeaderSize(header) -
                                2 * sizeof(Timestamp);
    const int32_t invalid_length = -1;
    std::memcpy(
        encoded.data() + event_offset + sizeof(Timestamp) + sizeof(EventType),
        &invalid_length,
        sizeof(invalid_length));
    input = Open(std::move(encoded));
    EXPECT_THROW(Run(InspectLegacyIndexFileAsync(*input, kPriority)),
                 SegcoreError);
}
}  // namespace
}  // namespace milvus::storage
