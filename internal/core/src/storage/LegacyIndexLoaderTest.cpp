// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "storage/LegacyIndexLoader.h"
#include "arrow/filesystem/localfs.h"
#include "common/Slice.h"
#include "folly/ScopeGuard.h"
#include "folly/coro/Baton.h"
#include "folly/system/ThreadName.h"
#include "storage/Event.h"
#include "storage/FileWriter.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalFileIOPool.h"
#include "storage/PluginLoader.h"
#include "storage/Util.h"
#include "test_utils/AsyncLoadTestUtils.h"
#include "test_utils/PlannerCipherPlugin.h"
#include "test_utils/TmpPath.h"
#include <array>
#include <atomic>
#include <fstream>
#include <future>
#include <gtest/gtest.h>
#include <map>
#include <numeric>

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
        budget_.SetCapacitySlots(1);
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
    Run(folly::coro::Task<T> task,
        proto::common::LoadPriority priority = kPriority) {
        return folly::coro::blockingWait(folly::coro::co_withExecutor(
            ResolveAsyncLoadExecutor({}, priority), std::move(task)));
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
        std::vector<uint8_t> output(info.payload_bytes);
        const EntryTarget consumer =
            MemoryEntryTarget{nullptr, output.data(), output.size()};
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
    std::vector<uint8_t> output(3, 255);
    const EntryTarget consumer =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
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
    EXPECT_EQ(output, std::vector<uint8_t>(3, 255));
    cleanup.dismiss();
}

TEST_F(LegacyIndexLoaderTest, ReadAndTargetFailuresReleaseAdmission) {
    auto input = Open(Encode({1, 2, 3}));
    auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
    file_->SetNextCompletion(arrow::Status::IOError("injected read failure"));
    EXPECT_THROW(Read(*input, info), SegcoreError);
    std::vector<uint8_t> output(2);
    const EntryTarget fail =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
    try {
        Run(StreamLegacyIndexFileAsync(*input, info, fail, kPriority));
        FAIL() << "expected target overflow";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), UnexpectedError);
    }
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
        EXPECT_EQ(Read(*input, info), payload);
        const int32_t invalid_length = -1;
        std::memcpy(encoded.data() + info.payload_offset + sizeof(Timestamp) +
                        sizeof(EventType),
                    &invalid_length,
                    sizeof(invalid_length));
        input = Open(std::move(encoded));
        EXPECT_THROW(Read(*input, info), SegcoreError);
    }
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

TEST_F(LegacyIndexLoaderTest,
       UnalignedConcatenationUsesExplicitBufferedTarget) {
    test::TmpPath directory;
    const auto previous_mode = FileWriter::GetMode();
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    LocalFileIOPool::GetInstance().Configure(1);
    auto restore = folly::makeGuard([&] {
        LocalFileIOPool::GetInstance().Configure(0);
        FileWriter::SetMode(previous_mode);
    });
    auto manager =
        std::make_shared<LocalChunkManager>(directory.get().string());
    const std::vector<std::vector<uint8_t>> payloads{{1, 2, 3},
                                                     {4, 5, 6, 7, 8}};
    std::vector<LegacyIndexFile> files;
    for (size_t i = 0; i < payloads.size(); ++i) {
        const auto path = (directory.get() / std::to_string(i)).string();
        auto encoded = Encode(payloads[i]);
        manager->Write(path, encoded.data(), encoded.size());
        auto source = Run(OpenLegacyIndexInputAsync(manager, {}, path));
        files.push_back(
            {path, Run(InspectLegacyIndexFileAsync(*source, kPriority))});
    }
    auto file =
        std::make_shared<IndexFileTarget>((directory.get() / "joined").string(),
                                          8,
                                          true,
                                          FileWriter::WriteMode::BUFFERED);
    const EntryTarget target = FileEntryTarget{file, 0, 8};
    // LOW uses the global DIRECT policy without the explicit target override.
    Run(ReadLegacyIndexFilesAsync(
        files, manager, {}, target, proto::common::LoadPriority::LOW));
    std::vector<uint8_t> actual(8);
    ASSERT_EQ(manager->Size(file->path), actual.size());
    manager->Read(file->path, actual.data(), actual.size());
    EXPECT_EQ(actual, (std::vector<uint8_t>{1, 2, 3, 4, 5, 6, 7, 8}));
    EXPECT_FALSE(file->Committed());
    Run(RunLocalFileIOAsync([&] { file->Cleanup(); }, kPriority));
    EXPECT_FALSE(std::filesystem::exists(file->path));
}

class CheckingLocalChunkManager : public LocalChunkManager {
 public:
    using LocalChunkManager::LocalChunkManager;
    uint64_t
    Size(const std::string& path) override {
        EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
            "MILVUS_LF_IO_"));
        return LocalChunkManager::Size(path);
    }
    uint64_t
    Read(const std::string& path,
         uint64_t offset,
         void* data,
         uint64_t bytes) override {
        EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
            "MILVUS_LF_IO_"));
        return LocalChunkManager::Read(path, offset, data, bytes);
    }
};

TEST_F(LegacyIndexLoaderTest, LocalSizeAndReadsRunOnLocalFilePool) {
    LocalFileIOPool::GetInstance().Configure(1);
    auto restore =
        folly::makeGuard([] { LocalFileIOPool::GetInstance().Configure(0); });
    test::TmpPath directory;
    auto manager =
        std::make_shared<CheckingLocalChunkManager>(directory.get().string());
    const auto path = (directory.get() / "local").string();
    const std::vector<uint8_t> expected{3, 2, 1};
    auto encoded = Encode(expected);
    manager->Write(path, encoded.data(), encoded.size());
    auto input = Run(OpenLegacyIndexInputAsync(manager, {}, path));
    const auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
    std::vector<uint8_t> output(expected.size());
    const EntryTarget target =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
    Run(StreamLegacyIndexFileAsync(*input, info, target, kPriority));
    EXPECT_EQ(output, expected);
}
class FailingSizeFile : public test::AsyncTrackingRandomAccessFile {
 public:
    explicit FailingSizeFile(ErrorCode code)
        : AsyncTrackingRandomAccessFile({}), code_(code) {
    }
    arrow::Result<int64_t>
    GetSize() override {
        ThrowInfo(code_, "injected GetSize failure");
    }

 private:
    ErrorCode code_;
};

class SingleInputFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    explicit SingleInputFileSystem(
        std::shared_ptr<arrow::io::RandomAccessFile> input)
        : SubTreeFileSystem("", std::make_shared<arrow::fs::LocalFileSystem>()),
          input_(std::move(input)) {
    }
    arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFileAsync(const std::string&) override {
        return arrow::Future<
            std::shared_ptr<arrow::io::RandomAccessFile>>::MakeFinished(input_);
    }

 private:
    std::shared_ptr<arrow::io::RandomAccessFile> input_;
};

TEST_F(LegacyIndexLoaderTest, GenericFilesystemSizeKeepsThrownErrorCode) {
    for (const auto code : {FileReadFailed, ConfigInvalid}) {
        auto fs = std::make_shared<SingleInputFileSystem>(
            std::make_shared<FailingSizeFile>(code));
        try {
            Run(OpenLegacyIndexInputAsync({}, fs, "legacy"));
            FAIL() << "expected GetSize failure";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), code);
        }
    }
}
}  // namespace
}  // namespace milvus::storage
