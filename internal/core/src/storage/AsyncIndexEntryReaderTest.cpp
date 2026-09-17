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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/future.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "folly/CancellationToken.h"
#include "folly/ScopeGuard.h"
#include "folly/system/ThreadName.h"
#include "folly/coro/BlockingWait.h"
#include "common/Common.h"
#include "common/EasyAssert.h"
#include "filemanager/InputStream.h"
#include "milvus-storage/common/extend_status.h"
#include "test_utils/Constants.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/IndexEntryEncryptedLocalWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/AsyncIndexEntryReader.h"
#include "index/IndexLoadPlan.h"
#include "storage/IndexEntryFormat.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LocalFileIOPool.h"
#include "storage/IndexEntryTarget.h"
#include "storage/EntryStreamUtils.h"
#include "storage/Crc32cUtil.h"
#include "storage/PluginLoader.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "test_utils/AsyncLoadTestUtils.h"

using namespace milvus::storage;

namespace {
// Occupies the single configured worker so a local-file phase stays queued.
class LocalFileIOBlocker {
 public:
    LocalFileIOBlocker()
        : executor_(LocalFileIOPool::GetInstance().GetExecutor()) {
        auto started = std::make_shared<std::promise<void>>();
        auto started_future = started->get_future();
        executor_->add([started, release = release_.get_future().share()] {
            started->set_value();
            release.wait();
        });
        EXPECT_EQ(started_future.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
    }

    ~LocalFileIOBlocker() {
        Release();
    }

    void
    Release() {
        if (!released_) {
            released_ = true;
            release_.set_value();
        }
    }

    bool
    WaitForQueuedTask() const {
        auto* worker =
            dynamic_cast<folly::CPUThreadPoolExecutor*>(executor_.get());
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (worker->getPendingTaskCount() == 0 &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return worker->getPendingTaskCount() > 0;
    }

 private:
    folly::Executor::KeepAlive<> executor_;
    std::promise<void> release_;
    bool released_{false};
};

std::unique_ptr<AsyncIndexEntryReader>
OpenAsyncReader(std::shared_ptr<milvus::InputStream> input) {
    return folly::coro::blockingWait(
        AsyncIndexEntryReader::Open(
            input, 0, milvus::proto::common::LoadPriority::HIGH, {})
            .scheduleOn(milvus::storage::ResolveAsyncLoadExecutor(
                {}, milvus::proto::common::LoadPriority::HIGH)));
}
}  // namespace

namespace {

class IndexEntryStreamConfigGuard {
 public:
    IndexEntryStreamConfigGuard()
        : budget_(LoadAdmissionController::GetInstance()),
          capacity_bytes_(budget_.CapacityBytes()) {
    }

    ~IndexEntryStreamConfigGuard() {
        budget_.SetCapacityBytes(capacity_bytes_);
    }

 private:
    LoadAdmissionController& budget_;
    size_t capacity_bytes_;
};

// Simple XOR-based mock cipher for testing (NOT for production use!)
class MockEncryptor : public plugin::IEncryptor {
 public:
    explicit MockEncryptor(uint8_t key) : key_(key) {
    }

    std::string
    Encrypt(const std::string& plaintext) const override {
        return Encrypt(plaintext.data(), plaintext.size());
    }

    std::string
    Encrypt(std::string_view plaintext) const override {
        return Encrypt(plaintext.data(), plaintext.size());
    }

    std::string
    Encrypt(const void* data, size_t len) const override {
        // Format: [1-byte key][XOR'd data]
        std::string result;
        result.reserve(1 + len);
        result.push_back(static_cast<char>(key_));
        const auto* src = static_cast<const uint8_t*>(data);
        for (size_t i = 0; i < len; i++) {
            result.push_back(static_cast<char>(src[i] ^ key_));
        }
        return result;
    }

    std::string
    GetKey() const override {
        return std::string(1, static_cast<char>(key_));
    }

 private:
    uint8_t key_;
};

class MockDecryptor : public plugin::IDecryptor {
 public:
    std::string
    Decrypt(const std::string& ciphertext) const override {
        return Decrypt(ciphertext.data(), ciphertext.size());
    }

    std::string
    Decrypt(std::string_view ciphertext) const override {
        return Decrypt(ciphertext.data(), ciphertext.size());
    }

    std::string
    Decrypt(const void* data, size_t len) const override {
        if (len < 1) {
            return "";
        }
        const auto* src = static_cast<const uint8_t*>(data);
        uint8_t key = src[0];
        std::string result;
        result.reserve(len - 1);
        for (size_t i = 1; i < len; i++) {
            result.push_back(static_cast<char>(src[i] ^ key));
        }
        return result;
    }

    std::string
    GetKey() const override {
        return "";
    }
};

class MockCipherPlugin : public plugin::ICipherPlugin {
 public:
    std::string
    getPluginName() const override {
        return "CipherPlugin";
    }

    void
    Update(int64_t, int64_t, const std::string&) override {
    }

    std::pair<std::shared_ptr<plugin::IEncryptor>, std::string>
    GetEncryptor(int64_t, int64_t) const override {
        return {std::make_shared<MockEncryptor>(0x5A), "mock_edek"};
    }

    std::shared_ptr<plugin::IDecryptor>
    GetDecryptor(int64_t, int64_t, const std::string&) const override {
        return std::make_shared<MockDecryptor>();
    }
};

class RecordingInputStream : public milvus::InputStream {
 public:
    struct ReadRange {
        size_t offset;
        size_t size;
        std::string thread_name;
    };

    explicit RecordingInputStream(std::shared_ptr<milvus::InputStream> base)
        : base_(std::move(base)) {
    }

    const std::vector<ReadRange>&
    ReadRanges() const {
        return read_ranges_;
    }

    void
    ClearReadRanges() {
        std::lock_guard<std::mutex> lock(mutex_);
        read_ranges_.clear();
    }

    size_t
    Size() const override {
        return base_->Size();
    }

    bool
    Seek(int64_t offset) override {
        return base_->Seek(offset);
    }

    size_t
    Tell() const override {
        return base_->Tell();
    }

    bool
    Eof() const override {
        return base_->Eof();
    }

    size_t
    Read(void* ptr, size_t size) override {
        return base_->Read(ptr, size);
    }

    size_t
    ReadAt(void*, size_t, size_t) override {
        ADD_FAILURE() << "Async reader must use ReadAtAsync";
        return 0;
    }

    folly::SemiFuture<size_t>
    ReadAtAsync(void* ptr, size_t offset, size_t size) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            read_ranges_.push_back(
                {offset, size, folly::getCurrentThreadName().value_or("")});
        }
        return base_->ReadAtAsync(ptr, offset, size);
    }

    size_t
    Read(int fd, size_t size) override {
        return base_->Read(fd, size);
    }

 private:
    std::shared_ptr<milvus::InputStream> base_;
    std::vector<ReadRange> read_ranges_;
    std::mutex mutex_;
};

std::string
GetRootPath() {
    return TestLocalPath + "async_index_reader_test";
}
const std::string kV3FilePath = "test_v3_index";

std::vector<uint8_t>
GeneratePattern(size_t size) {
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<uint8_t>(i % 256);
    }
    return data;
}

std::vector<uint8_t>
ReadLocalFileBytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary | std::ios::ate);
    EXPECT_TRUE(input.is_open()) << "failed to open " << path;
    auto size = static_cast<size_t>(input.tellg());
    input.seekg(0);
    std::vector<uint8_t> data(size);
    input.read(reinterpret_cast<char*>(data.data()), size);
    EXPECT_TRUE(input.good()) << "failed to read " << path;
    return data;
}

}  // namespace

class AsyncIndexEntryReaderTest : public testing::Test {
 public:
    void
    SetUp() override {
        auto conf = milvus_storage::ArrowFileSystemConfig();
        conf.storage_type = "local";
        conf.root_path = GetRootPath();
        auto result = milvus_storage::CreateArrowFileSystem(conf);
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        fs_ = result.ValueOrDie();
    }

    void
    TearDown() override {
        if (fs_) {
            fs_->DeleteDirContents("");
        }
    }

 protected:
    std::shared_ptr<milvus::OutputStream>
    CreateOutputStream(const std::string& path) {
        auto result = fs_->OpenOutputStream(path);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return std::make_shared<RemoteOutputStream>(
            std::move(result.ValueOrDie()));
    }

    std::shared_ptr<milvus::InputStream>
    CreateInputStream(const std::string& path) {
        auto result = fs_->OpenInputFile(path);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return std::make_shared<RemoteInputStream>(
            std::move(result.ValueOrDie()));
    }

    milvus_storage::ArrowFileSystemPtr fs_;
};

// Mirror production ownership while testing the reader in isolation.
static folly::coro::Task<milvus::index::IndexLoadPlan>
ReadEntriesForTest(AsyncIndexEntryReader& reader,
                   std::vector<EntryLoadPlan> entries,
                   milvus::proto::common::LoadPriority priority,
                   folly::CancellationToken token = {}) {
    milvus::index::IndexLoadPlan plan;
    plan.entries = std::move(entries);
    co_await reader.ReadEntriesAsync(plan.entries, priority, token);
    co_return std::move(plan);
}

TEST_F(AsyncIndexEntryReaderTest, InvalidFileSizeDoesNotRemoveExistingFile) {
    const auto path = GetRootPath() + "/existing_file";
    std::filesystem::create_directories(GetRootPath());
    {
        std::ofstream output(path);
        output << "keep";
    }
    EXPECT_THROW(
        IndexFileTarget(path, std::numeric_limits<size_t>::max(), false)
            .Prepare(io::Priority::MIDDLE),
        milvus::SegcoreError);
    ASSERT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(ReadLocalFileBytes(path),
              (std::vector<uint8_t>{'k', 'e', 'e', 'p'}));
}

TEST_F(AsyncIndexEntryReaderTest, FileTargetCleanupPreservesOwnership) {
    std::filesystem::create_directories(GetRootPath());
    const auto path = GetRootPath() + "/file_target_cleanup";
    for (bool retain : {false, true}) {
        for (bool commit : {false, true}) {
            auto target = std::make_shared<IndexFileTarget>(path, 0, retain);
            target->Prepare(io::Priority::MIDDLE);
            target->Finish();
            if (commit) {
                target->Commit();
            }
            target->Cleanup();
            EXPECT_EQ(std::filesystem::exists(path), retain && commit);
            if (!(retain && commit)) {
                // A lingering owner must not unlink a new file at the same path.
                std::ofstream replacement(path);
                replacement << "replacement";
            }
            target->Cleanup();
            target.reset();
            EXPECT_TRUE(std::filesystem::exists(path));
            std::filesystem::remove(path);
        }
    }
}

TEST_F(AsyncIndexEntryReaderTest,
       CancelledMetadataAdmissionPreservesErrorCode) {
    const std::string file_path = kV3FilePath + "_cancel_metadata";
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.PutMeta("value", 42);
        writer.Finish();
    }
    milvus::test::ScopedLoadTransientBudget budget_guard(1);
    auto& budget = LoadAdmissionController::GetInstance();
    auto held = folly::coro::blockingWait(
        budget.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
    auto direct_file = std::make_shared<milvus::test::ControlledDirectReadFile>(
        ReadLocalFileBytes(GetRootPath() + "/" + file_path));
    auto input = std::make_shared<RemoteInputStream>(direct_file);
    folly::CancellationSource cancel;
    auto load = std::async(std::launch::async, [&] {
        try {
            folly::coro::blockingWait(
                AsyncIndexEntryReader::Open(
                    input,
                    0,
                    milvus::proto::common::LoadPriority::HIGH,
                    cancel.getToken())
                    .scheduleOn(milvus::storage::ResolveAsyncLoadExecutor(
                        {}, milvus::proto::common::LoadPriority::HIGH)));
            return milvus::ErrorCode::Success;
        } catch (const milvus::SegcoreError& error) {
            return error.get_error_code();
        }
    });
    // Magic, footer and directory bypass the held budget; _meta waits for it.
    EXPECT_TRUE(direct_file->WaitForCallCount(3));
    EXPECT_FALSE(
        direct_file->WaitForCallCount(4, std::chrono::milliseconds(50)));
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    cancel.requestCancellation();
    // Always release before asserting so a regression cannot strand the task.
    held.Release();
    EXPECT_EQ(load.get(), milvus::ErrorCode::FollyCancel);
}

TEST_F(AsyncIndexEntryReaderTest,
       LargeDirectoryReadBypassesAdmissionAndDrainsOnCancel) {
    const auto path = kV3FilePath + "_large_directory";
    {
        IndexEntryDirectStreamWriter writer(CreateOutputStream(path));
        const uint8_t value = 42;
        writer.WriteEntry(std::string(128 * 1024, 'x'), &value, 1);
        writer.Finish();
    }
    milvus::test::ScopedLoadTransientBudget budget_guard(1024);
    auto direct = std::make_shared<milvus::test::ControlledDirectReadFile>(
        ReadLocalFileBytes(GetRootPath() + "/" + path));
    direct->SetAutoComplete(false);
    auto input = std::make_shared<RemoteInputStream>(direct);
    folly::CancellationSource cancel;
    auto load = std::async(std::launch::async, [&] {
        return folly::coro::blockingWait(
            AsyncIndexEntryReader::Open(
                input,
                0,
                milvus::proto::common::LoadPriority::HIGH,
                cancel.getToken())
                .scheduleOn(ResolveAsyncLoadExecutor(
                    {}, milvus::proto::common::LoadPriority::HIGH)));
    });
    auto drain = folly::makeGuard([&] {
        cancel.requestCancellation();
        direct->SetAutoComplete(true);
        for (size_t i = 0; i < direct->DirectReadCalls().size(); ++i)
            direct->Complete(i);
        load.wait();
    });
    for (size_t i = 0; i < 2; ++i) {
        ASSERT_TRUE(direct->WaitForCallCount(i + 1));
        direct->Complete(i);
    }
    ASSERT_TRUE(direct->WaitForCallCount(3));
    EXPECT_GT(direct->DirectReadCalls()[2].nbytes, 64 * 1024);
    auto& admission = LoadAdmissionController::GetInstance();
    const bool acquired =
        admission.TryAcquire({1024, 1}, LoadAdmissionPriority::High);
    if (acquired)
        admission.Release({1024, 1});
    EXPECT_TRUE(acquired);
    cancel.requestCancellation();
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    direct->Complete(2);
    try {
        (void)load.get();
        FAIL() << "expected cancellation";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::ErrorCode::FollyCancel);
    }
    drain.dismiss();
    EXPECT_EQ(direct->ReadAtCalls(), 0);
    EXPECT_EQ(direct->DirectReadCalls().size(), 3);
}

TEST_F(AsyncIndexEntryReaderTest, CatalogExposesStablePlainEntrySources) {
    const std::string file_path = kV3FilePath + "_plain_catalog";
    auto alpha = GeneratePattern(64);
    auto beta = GeneratePattern(128);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("alpha", alpha.data(), alpha.size());
        writer.WriteEntry("beta", beta.data(), beta.size());
        writer.PutMeta("directory_value", 17);
        writer.Finish();
    }

    auto recording =
        std::make_shared<RecordingInputStream>(CreateInputStream(file_path));
    auto reader = OpenAsyncReader(recording);
    auto reads_after_open = recording->ReadRanges().size();

    static_assert(std::is_same_v<decltype(reader->Directory()),
                                 const IndexEntryDirectory&>);
    const auto& entries = reader->Directory().Entries();
    ASSERT_EQ(entries.size(), 3);

    EXPECT_EQ(reader->Directory().At("alpha").name, "alpha");
    EXPECT_EQ(reader->Directory().At("alpha").plaintext_size, alpha.size());
    EXPECT_EQ(reader->Directory().At("alpha").expected_crc,
              Crc32cValue(alpha.data(), alpha.size()));
    ASSERT_TRUE(std::holds_alternative<PlainEntrySource>(
        reader->Directory().At("alpha").source));
    const auto& alpha_source =
        std::get<PlainEntrySource>(reader->Directory().At("alpha").source);
    EXPECT_EQ(alpha_source.remote_offset, MILVUS_V3_MAGIC_SIZE);

    EXPECT_EQ(reader->Directory().At("beta").name, "beta");
    EXPECT_EQ(reader->Directory().At("beta").plaintext_size, beta.size());
    const auto& beta_source =
        std::get<PlainEntrySource>(reader->Directory().At("beta").source);
    EXPECT_EQ(beta_source.remote_offset, MILVUS_V3_MAGIC_SIZE + alpha.size());

    EXPECT_EQ(reader->Directory().At(MILVUS_V3_META_ENTRY_NAME).name,
              MILVUS_V3_META_ENTRY_NAME);
    EXPECT_TRUE(reader->IndexMeta().contains("directory_value"));
    EXPECT_EQ(reader->IndexMeta().at("directory_value").get<int>(), 17);
    EXPECT_THROW(reader->Directory().At("missing"), milvus::SegcoreError);

    EXPECT_EQ(recording->ReadRanges().size(), reads_after_open);

    auto sync_input = CreateInputStream(file_path);
    auto sync_reader = IndexEntryReader::Open(sync_input, sync_input->Size());
    EXPECT_EQ(sync_reader->IndexMeta().at("directory_value").get<int>(), 17);
    ASSERT_EQ(sync_reader->Directory().Entries().size(), entries.size());
    for (const auto& entry : entries) {
        const auto& sync_entry = sync_reader->Directory().At(entry.name);
        EXPECT_EQ(sync_entry.plaintext_size, entry.plaintext_size);
        EXPECT_EQ(sync_entry.expected_crc, entry.expected_crc);
        EXPECT_EQ(std::get<PlainEntrySource>(sync_entry.source).remote_offset,
                  std::get<PlainEntrySource>(entry.source).remote_offset);
    }
    EXPECT_EQ(sync_reader->ReadEntry("alpha").data, alpha);
    EXPECT_EQ(sync_reader->ReadEntry("beta").data, beta);
}

TEST_F(AsyncIndexEntryReaderTest,
       PlainEntryUsesExactlyOneNativeCallerOwnedRead) {
    const std::string file_path = kV3FilePath + "_direct_plain_slice";
    auto data = GeneratePattern(4 * kStreamSliceAlignment);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    ASSERT_NE(direct_file, nullptr);

    direct_file->ResetCounters();

    std::vector<uint8_t> target(data.size());
    folly::coro::blockingWait(ReadEntriesForTest(
        *reader,
        {{"data", MemoryEntryTarget{nullptr, target.data(), target.size()}}},
        milvus::proto::common::LoadPriority::HIGH));

    EXPECT_TRUE(std::equal(target.begin(), target.end(), data.begin()));
    auto calls = direct_file->DirectReadCalls();
    ASSERT_EQ(calls.size(), 1);
    const auto& source =
        std::get<PlainEntrySource>(reader->Directory().At("data").source);
    EXPECT_EQ(calls[0].position, source.remote_offset);
    EXPECT_EQ(calls[0].nbytes, target.size());
    EXPECT_EQ(calls[0].destination, target.data());
    EXPECT_EQ(direct_file->AsyncReadCalls(), 0);
    EXPECT_EQ(direct_file->PeakInflight(), 1);
}

TEST_F(AsyncIndexEntryReaderTest, PlainEntryFallsBackToArrowBufferRead) {
    const std::string file_path = kV3FilePath + "_buffered_fallback";
    auto data = GeneratePattern(kStreamSliceAlignment);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::AsyncTrackingRandomAccessFile* fallback_file = nullptr;
    auto reader = milvus::test::OpenAsyncIndexEntryReader(std::move(packed),
                                                          &fallback_file);
    ASSERT_NE(fallback_file, nullptr);

    fallback_file->ResetCounters();

    std::vector<uint8_t> target(data.size());
    folly::coro::blockingWait(ReadEntriesForTest(
        *reader,
        {{"data", MemoryEntryTarget{nullptr, target.data(), target.size()}}},
        milvus::proto::common::LoadPriority::HIGH));

    EXPECT_TRUE(std::equal(target.begin(), target.end(), data.begin()));
    EXPECT_EQ(fallback_file->AsyncReadCalls(), 1);
    EXPECT_EQ(fallback_file->ReadAtCalls(), 0);
}

TEST_F(AsyncIndexEntryReaderTest, PlainEntryRejectsShortRead) {
    const std::string file_path = kV3FilePath + "_direct_short_read";
    auto data = GeneratePattern(kStreamSliceAlignment);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    direct_file->SetNextCompletion(arrow::Status::OK(), 127);

    std::vector<uint8_t> target(data.size());
    EXPECT_THROW(
        folly::coro::blockingWait(ReadEntriesForTest(
            *reader,
            {{"data",
              MemoryEntryTarget{nullptr, target.data(), target.size()}}},
            milvus::proto::common::LoadPriority::HIGH)),
        milvus::SegcoreError);
}

TEST_F(AsyncIndexEntryReaderTest, PlainEntryRetriesTransientStorageError) {
    const std::string file_path = kV3FilePath + "_direct_storage_error";
    auto data = GeneratePattern(kStreamSliceAlignment);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    direct_file->SetNextCompletion(milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::StorageTransientThrottling,
        "throttled"));

    std::vector<uint8_t> target(data.size());
    folly::coro::blockingWait(ReadEntriesForTest(
        *reader,
        {{"data", MemoryEntryTarget{nullptr, target.data(), target.size()}}},
        milvus::proto::common::LoadPriority::HIGH));

    EXPECT_TRUE(std::equal(target.begin(), target.end(), data.begin()));
    EXPECT_EQ(direct_file->DirectReadCalls().size(), 2);
}

TEST_F(AsyncIndexEntryReaderTest, PlainEntryCancellationDrainsStreamRetry) {
    const std::string file_path = kV3FilePath + "_direct_retry_cancel";
    auto data = GeneratePattern(kStreamSliceAlignment);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    direct_file->SetAutoComplete(false);

    folly::CancellationSource cancellation_source;
    std::vector<uint8_t> target(data.size());
    auto read_future = std::async(std::launch::async, [&]() {
        folly::coro::blockingWait(ReadEntriesForTest(
            *reader,
            {{"data",
              MemoryEntryTarget{nullptr, target.data(), target.size()}}},
            milvus::proto::common::LoadPriority::HIGH,
            cancellation_source.getToken()));
    });

    ASSERT_TRUE(direct_file->WaitForCallCount(1));
    cancellation_source.requestCancellation();
    direct_file->Complete(
        0,
        milvus_storage::MakeExtendError(
            milvus_storage::ExtendStatusCode::StorageTransientThrottling,
            "throttled"));

    ASSERT_TRUE(direct_file->WaitForCallCount(2));
    EXPECT_EQ(read_future.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    direct_file->Complete(1);
    ASSERT_EQ(read_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    try {
        read_future.get();
        FAIL() << "expected cancellation";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::ErrorCode::FollyCancel);
    }
    EXPECT_EQ(direct_file->DirectReadCalls().size(), 2);
}

TEST_F(AsyncIndexEntryReaderTest,
       ReadEntriesCombinesOutOfOrderSlicesAcrossEntries) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_round_robin";
    const size_t slice_size = DefaultStreamSliceSize();
    auto data_a = GeneratePattern(2 * slice_size);
    auto data_b = GeneratePattern(2 * slice_size);
    std::reverse(data_b.begin(), data_b.end());
    for (size_t i = slice_size; i < data_a.size(); ++i) {
        data_a[i] ^= 0x5a;
        data_b[i] ^= 0x3c;
    }
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("a", data_a.data(), data_a.size());
        writer.WriteEntry("b", data_b.data(), data_b.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    direct_file->SetAutoComplete(false);

    auto target_a = std::make_shared<std::vector<uint8_t>>(data_a.size());
    auto target_b = std::make_shared<std::vector<uint8_t>>(data_b.size());
    std::vector<EntryLoadPlan> entries;

    entries.push_back(EntryLoadPlan{
        "a", MemoryEntryTarget{target_a, target_a->data(), target_a->size()}});
    entries.push_back(EntryLoadPlan{
        "b", MemoryEntryTarget{target_b, target_b->data(), target_b->size()}});

    auto materialize_future = std::async(
        std::launch::async,
        [reader = reader.get(), entries = std::move(entries)]() mutable {
            return folly::coro::blockingWait(
                ReadEntriesForTest(*reader,
                                   std::move(entries),
                                   milvus::proto::common::LoadPriority::HIGH));
        });
    auto drain_on_failure = folly::makeGuard([&] {
        direct_file->SetAutoComplete(true);
        const auto submitted = direct_file->DirectReadCalls().size();
        for (size_t i = 0; i < submitted; ++i) {
            direct_file->Complete(i);
        }
    });

    ASSERT_TRUE(direct_file->WaitForCallCount(4));
    auto calls = direct_file->DirectReadCalls();
    ASSERT_EQ(calls.size(), 4);
    const auto& source_a =
        std::get<PlainEntrySource>(reader->Directory().At("a").source);
    const auto& source_b =
        std::get<PlainEntrySource>(reader->Directory().At("b").source);
    // Workers may start in any order. Complete each entry's second slice
    // before its first slice, independently of executor start order.
    std::vector<std::pair<int64_t, size_t>> positions;
    for (size_t i = 0; i < calls.size(); ++i) {
        positions.emplace_back(calls[i].position, i);
    }
    std::sort(positions.begin(), positions.end());
    EXPECT_EQ(positions[0].first, source_a.remote_offset);
    EXPECT_EQ(positions[1].first, source_a.remote_offset + slice_size);
    EXPECT_EQ(positions[2].first, source_b.remote_offset);
    EXPECT_EQ(positions[3].first, source_b.remote_offset + slice_size);
    for (auto i : {1, 3, 2, 0}) {
        direct_file->Complete(positions[i].second);
    }

    auto plan = materialize_future.get();
    EXPECT_EQ(*target_a, data_a);
    EXPECT_EQ(*target_b, data_b);

    EXPECT_EQ(direct_file->PeakInflight(), 4);
}

TEST_F(AsyncIndexEntryReaderTest,
       ReadEntriesUsesBufferedAsyncReadWhenNativeReadIntoIsUnavailable) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_buffered";
    const size_t slice_size = DefaultStreamSliceSize();
    auto data = GeneratePattern(2 * slice_size);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::AsyncTrackingRandomAccessFile* fallback_file = nullptr;
    auto reader = milvus::test::OpenAsyncIndexEntryReader(std::move(packed),
                                                          &fallback_file);
    ASSERT_NE(fallback_file, nullptr);

    fallback_file->ResetCounters();

    auto target = std::make_shared<std::vector<uint8_t>>(data.size());
    std::vector<EntryLoadPlan> entries;

    entries.push_back(EntryLoadPlan{
        "data", MemoryEntryTarget{target, target->data(), target->size()}});

    auto plan = folly::coro::blockingWait(
        ReadEntriesForTest(*reader,
                           std::move(entries),
                           milvus::proto::common::LoadPriority::HIGH));

    EXPECT_EQ(*target, data);
    EXPECT_EQ(fallback_file->AsyncReadCalls(), 2);
    EXPECT_EQ(fallback_file->ReadAtCalls(), 0);
}

TEST_F(AsyncIndexEntryReaderTest, ReadEntriesUsesOnlyGlobalAdmissionLimits) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    auto& admission = LoadAdmissionController::GetInstance();
    const auto old_slots = admission.CapacitySlots();
    const auto old_workers = GetAsyncLoadThreadPoolSize();
    auto restore = folly::makeGuard([&] {
        admission.SetCapacitySlots(old_slots);
        SetAsyncLoadThreadPoolSize(old_workers);
    });
    admission.SetCapacitySlots(2);
    admission.SetCapacityBytes(2 * DefaultStreamSliceSize());
    SetAsyncLoadThreadPoolSize(1);
    const std::string file_path = kV3FilePath + "_materialize_inflight";
    const size_t slice_size = DefaultStreamSliceSize();
    constexpr size_t slices = 10;
    auto data = GeneratePattern(slices * slice_size);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    direct_file->SetAutoComplete(false);

    auto target = std::make_shared<std::vector<uint8_t>>(data.size());
    std::vector<EntryLoadPlan> entries;

    entries.push_back(EntryLoadPlan{
        "data", MemoryEntryTarget{target, target->data(), target->size()}});

    auto materialize_future = std::async(
        std::launch::async,
        [reader = reader.get(), entries = std::move(entries)]() mutable {
            return folly::coro::blockingWait(
                ReadEntriesForTest(*reader,
                                   std::move(entries),
                                   milvus::proto::common::LoadPriority::LOW));
        });
    auto drain_on_failure = folly::makeGuard([&] {
        direct_file->SetAutoComplete(true);
        const auto submitted = direct_file->DirectReadCalls().size();
        for (size_t i = 0; i < submitted; ++i) {
            direct_file->Complete(i);
        }
    });

    ASSERT_TRUE(direct_file->WaitForCallCount(2));
    EXPECT_FALSE(
        direct_file->WaitForCallCount(3, std::chrono::milliseconds(50)));
    // Expand both global limits past the former per-load eight-task/128 MiB
    // caps, without completing a read or adding another async worker.
    admission.SetCapacityBytes(slices * slice_size);
    admission.SetCapacitySlots(slices);
    ASSERT_TRUE(direct_file->WaitForCallCount(slices));
    EXPECT_EQ(direct_file->PeakInflight(), slices);
    for (size_t i = 0; i < slices; ++i) {
        direct_file->Complete(i);
    }

    auto plan = materialize_future.get();

    EXPECT_EQ(*target, data);
}

TEST_F(AsyncIndexEntryReaderTest,
       ReadEntriesSharesSlotsAndObservesCapacityChanges) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_joint_slots";
    const size_t slice_size = DefaultStreamSliceSize();
    auto data = GeneratePattern(4 * slice_size);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    direct_file->SetAutoComplete(false);
    auto& admission = LoadAdmissionController::GetInstance();
    const auto previous_slots = admission.CapacitySlots();
    auto restore_slots =
        folly::makeGuard([&] { admission.SetCapacitySlots(previous_slots); });
    admission.SetCapacitySlots(2);
    // Another load consumes one slot without consuming any byte budget.
    auto held = folly::coro::blockingWait(
        admission.AcquireAsync({0, 1}, LoadAdmissionPriority::High));

    auto target = std::make_shared<std::vector<uint8_t>>(data.size());
    std::vector<EntryLoadPlan> entries;

    entries.push_back(EntryLoadPlan{
        "data", MemoryEntryTarget{target, target->data(), target->size()}});

    auto materialize_future = std::async(
        std::launch::async,
        [reader = reader.get(), entries = std::move(entries)]() mutable {
            return folly::coro::blockingWait(
                ReadEntriesForTest(*reader,
                                   std::move(entries),
                                   milvus::proto::common::LoadPriority::LOW));
        });
    auto drain_on_failure = folly::makeGuard([&] {
        admission.SetCapacitySlots(0);
        direct_file->SetAutoComplete(true);
        const auto submitted = direct_file->DirectReadCalls().size();
        for (size_t i = 0; i < submitted; ++i) {
            direct_file->Complete(i);
        }
    });

    ASSERT_TRUE(direct_file->WaitForCallCount(1));
    EXPECT_FALSE(
        direct_file->WaitForCallCount(2, std::chrono::milliseconds(50)));
    held.Release();
    ASSERT_TRUE(direct_file->WaitForCallCount(2));

    admission.SetCapacitySlots(1);
    direct_file->Complete(1);
    EXPECT_FALSE(
        direct_file->WaitForCallCount(3, std::chrono::milliseconds(50)));
    direct_file->Complete(0);
    ASSERT_TRUE(direct_file->WaitForCallCount(3));

    admission.SetCapacitySlots(0);
    ASSERT_TRUE(direct_file->WaitForCallCount(4));
    direct_file->Complete(3);
    direct_file->Complete(2);

    auto plan = materialize_future.get();

    EXPECT_EQ(*target, data);
    EXPECT_LE(direct_file->PeakInflight(), 2);
    admission.SetCapacitySlots(2);
    ASSERT_TRUE(admission.TryAcquire({0, 2}, LoadAdmissionPriority::High));
    admission.Release({0, 2});
}

TEST_F(AsyncIndexEntryReaderTest,
       ReadEntriesRejectsOverlappingMemoryEntryTargetsBeforeRead) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_memory_overlap";
    const size_t entry_size = kStreamSliceAlignment;
    auto data_a = GeneratePattern(entry_size);
    auto data_b = GeneratePattern(entry_size);
    std::reverse(data_b.begin(), data_b.end());
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("a", data_a.data(), data_a.size());
        writer.WriteEntry("b", data_b.data(), data_b.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);

    auto target =
        std::make_shared<std::vector<uint8_t>>(2 * entry_size, uint8_t{0});
    std::vector<EntryLoadPlan> entries;
    entries.push_back(EntryLoadPlan{
        "a", MemoryEntryTarget{target, target->data(), entry_size}});
    entries.push_back(EntryLoadPlan{
        "b",
        MemoryEntryTarget{
            target, target->data() + entry_size / 2, entry_size}});

    EXPECT_THROW(folly::coro::blockingWait(ReadEntriesForTest(
                     *reader,
                     std::move(entries),
                     milvus::proto::common::LoadPriority::HIGH)),
                 milvus::SegcoreError);
    EXPECT_TRUE(direct_file->DirectReadCalls().empty());
}

TEST_F(AsyncIndexEntryReaderTest,
       ReadEntriesRejectsOverlappingFileEntryTargetsBeforePrepare) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_mmap_overlap";
    const std::string staging_path =
        GetRootPath() + "/materialize_mmap_overlap.mmap";
    const size_t entry_size = kStreamSliceAlignment;
    auto data_a = GeneratePattern(entry_size);
    auto data_b = GeneratePattern(entry_size);
    std::reverse(data_b.begin(), data_b.end());
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("a", data_a.data(), data_a.size());
        writer.WriteEntry("b", data_b.data(), data_b.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);

    auto staging =
        std::make_shared<IndexFileTarget>(staging_path, 2 * entry_size, false);
    std::vector<EntryLoadPlan> entries;
    entries.push_back(
        EntryLoadPlan{"a", FileEntryTarget{staging, 0, entry_size}});
    entries.push_back(EntryLoadPlan{
        "b", FileEntryTarget{staging, entry_size / 2, entry_size}});

    EXPECT_THROW(folly::coro::blockingWait(ReadEntriesForTest(
                     *reader,
                     std::move(entries),
                     milvus::proto::common::LoadPriority::HIGH)),
                 milvus::SegcoreError);
    EXPECT_TRUE(direct_file->DirectReadCalls().empty());
    EXPECT_FALSE(std::filesystem::exists(staging_path));
}

TEST_F(AsyncIndexEntryReaderTest,
       FileSliceHoldsAdmissionUntilLimitedWriteCompletes) {
    const auto data = GeneratePattern(kStreamSliceAlignment);
    milvus::test::ScopedLoadTransientBudget budget_guard(
        2 * data.size() + 2 * FileWriter::ALIGNMENT_MASK);
    auto& budget = LoadAdmissionController::GetInstance();
    const auto old_slots = budget.CapacitySlots();
    budget.SetCapacitySlots(1);
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    const auto old_mode = FileWriter::GetMode();
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    auto restore = folly::makeGuard([&] {
        FileWriter::SetMode(old_mode);
        pool.Configure(0);
        budget.SetCapacitySlots(old_slots);
    });
    const auto path = kV3FilePath + "_limited_file_slice";
    {
        IndexEntryDirectStreamWriter writer(CreateOutputStream(path));
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }
    milvus::test::ControlledDirectReadFile* direct = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(
        ReadLocalFileBytes(GetRootPath() + "/" + path), &direct);
    direct->SetAutoComplete(false);
    const auto local = GetRootPath() + "/limited_file_slice";
    auto staging = std::make_shared<IndexFileTarget>(local, data.size(), false);
    auto permit = pool.AcquireWritePermit();
    auto load = std::async(std::launch::async, [&] {
        return folly::coro::blockingWait(ReadEntriesForTest(
            *reader,
            {{"data", FileEntryTarget{staging, 0, data.size()}}},
            milvus::proto::common::LoadPriority::LOW));
    });
    auto drain = folly::makeGuard([&] {
        permit = {};
        direct->SetAutoComplete(true);
        for (size_t i = 0; i < direct->DirectReadCalls().size(); ++i)
            direct->Complete(i);
    });
    ASSERT_TRUE(direct->WaitForCallCount(1));
    direct->Complete(0);
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    EXPECT_EQ(std::filesystem::file_size(local), 0);
    const bool admitted =
        budget.TryAcquire({1, 1}, LoadAdmissionPriority::High);
    EXPECT_FALSE(admitted);
    if (admitted)
        budget.Release({1, 1});
    permit = {};
    ASSERT_EQ(load.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto plan = load.get();
    EXPECT_EQ(ReadLocalFileBytes(local), data);
    EXPECT_THROW(staging->WriteAt(0, nullptr, 0), milvus::SegcoreError);
    const bool released =
        budget.TryAcquire({1, 1}, LoadAdmissionPriority::High);
    EXPECT_TRUE(released);
    if (released)
        budget.Release({1, 1});
}

TEST_F(AsyncIndexEntryReaderTest, SharedFilePadsEntryBoundaries) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    const auto old_mode = FileWriter::GetMode();
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    auto restore = folly::makeGuard([&] { FileWriter::SetMode(old_mode); });
    const auto a = GeneratePattern(33);
    const auto b = GeneratePattern(45);
    const auto path = kV3FilePath + "_shared_unaligned_file";
    {
        IndexEntryDirectStreamWriter writer(CreateOutputStream(path));
        writer.WriteEntry("a", a.data(), a.size());
        writer.WriteEntry("b", b.data(), b.size());
        writer.Finish();
    }
    auto reader = OpenAsyncReader(CreateInputStream(path));
    const auto local = GetRootPath() + "/shared_unaligned_file";
    auto staging = std::make_shared<IndexFileTarget>(
        local, FileWriter::ALIGNMENT_BYTES + b.size(), false);
    // The reserved padding must not overlap another entry's data.
    EXPECT_THROW(
        folly::coro::blockingWait(ReadEntriesForTest(
            *reader,
            {{"a", FileEntryTarget{staging, 0, FileWriter::ALIGNMENT_BYTES}},
             {"b", FileEntryTarget{staging, a.size(), b.size()}}},
            milvus::proto::common::LoadPriority::LOW)),
        milvus::SegcoreError);
    auto plan = folly::coro::blockingWait(ReadEntriesForTest(
        *reader,
        {{"a", FileEntryTarget{staging, 0, FileWriter::ALIGNMENT_BYTES}},
         {"b",
          FileEntryTarget{staging, FileWriter::ALIGNMENT_BYTES, b.size()}}},
        milvus::proto::common::LoadPriority::LOW));
    auto expected = a;
    expected.resize(FileWriter::ALIGNMENT_BYTES, 0);
    expected.insert(expected.end(), b.begin(), b.end());
    EXPECT_EQ(ReadLocalFileBytes(local), expected);
}

TEST_F(AsyncIndexEntryReaderTest,
       ReadEntriesClosesWriterBeforeIndexMaterialization) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_mmap_finish";
    const std::string staging_path =
        GetRootPath() + "/materialize_mmap_finish.mmap";
    auto data = GeneratePattern(kStreamSliceAlignment);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    auto staging =
        std::make_shared<IndexFileTarget>(staging_path, data.size(), false);
    std::vector<EntryLoadPlan> entries;
    entries.push_back(
        EntryLoadPlan{"data", FileEntryTarget{staging, 0, data.size()}});

    {
        auto plan = folly::coro::blockingWait(
            ReadEntriesForTest(*reader,
                               std::move(entries),
                               milvus::proto::common::LoadPriority::HIGH));

        ASSERT_TRUE(staging->Prepared());
        EXPECT_THROW(staging->WriteAt(0, nullptr, 0), milvus::SegcoreError);
        EXPECT_TRUE(std::filesystem::exists(staging_path));
    }
    EXPECT_FALSE(std::filesystem::exists(staging_path));
}

TEST_F(AsyncIndexEntryReaderTest,
       ReadEntriesRejectsCombinedCrcAndRemovesStagingFile) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_crc";
    const std::string staging_path = GetRootPath() + "/materialize_crc.mmap";
    const size_t slice_size = DefaultStreamSliceSize();
    auto data = GeneratePattern(2 * slice_size);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    const auto& source =
        std::get<PlainEntrySource>(reader->Directory().At("data").source);
    direct_file->CorruptRemoteByte(source.remote_offset + slice_size + 7);

    std::vector<EntryLoadPlan> entries;

    auto staging =
        std::make_shared<IndexFileTarget>(staging_path, data.size(), false);
    entries.push_back(
        EntryLoadPlan{"data", FileEntryTarget{staging, 0, data.size()}});

    EXPECT_THROW(folly::coro::blockingWait(ReadEntriesForTest(
                     *reader,
                     std::move(entries),
                     milvus::proto::common::LoadPriority::HIGH)),
                 milvus::SegcoreError);
    EXPECT_FALSE(std::filesystem::exists(staging_path));
}

TEST_F(AsyncIndexEntryReaderTest, ReadEntriesCancelsQueuedMmapPreparation) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto restore = folly::makeGuard([&] { pool.Configure(0); });
    const auto file_path = kV3FilePath + "_queued_mmap_prepare";
    const auto staging_path = GetRootPath() + "/queued_mmap_prepare/payload";
    const auto data = GeneratePattern(kStreamSliceAlignment);
    {
        IndexEntryDirectStreamWriter writer(CreateOutputStream(file_path));
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(
        ReadLocalFileBytes(GetRootPath() + "/" + file_path), &direct_file);
    auto staging =
        std::make_shared<IndexFileTarget>(staging_path, data.size(), false);
    std::vector<EntryLoadPlan> entries;
    entries.push_back(
        EntryLoadPlan{"data", FileEntryTarget{staging, 0, data.size()}});
    LocalFileIOBlocker blocker;
    folly::CancellationSource cancellation;
    auto load = std::async(std::launch::async, [&] {
        return folly::coro::blockingWait(
            ReadEntriesForTest(*reader,
                               std::move(entries),
                               milvus::proto::common::LoadPriority::HIGH,
                               cancellation.getToken()));
    });
    auto unblock = folly::makeGuard([&] { blocker.Release(); });
    ASSERT_TRUE(blocker.WaitForQueuedTask());
    EXPECT_TRUE(direct_file->DirectReadCalls().empty());
    cancellation.requestCancellation();
    blocker.Release();
    try {
        (void)load.get();
        FAIL() << "expected cancelled file preparation";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::ErrorCode::FollyCancel);
    }
    EXPECT_TRUE(direct_file->DirectReadCalls().empty());
    // No directory was created: cancellation was checked on the queued worker.
    EXPECT_FALSE(std::filesystem::exists(
        std::filesystem::path(staging_path).parent_path()));
}

TEST_F(AsyncIndexEntryReaderTest,
       ReadEntriesFinishesAndCleansMmapOnLocalFileIOPool) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto restore = folly::makeGuard([&] { pool.Configure(0); });
    const auto file_path = kV3FilePath + "_mmap_local_io";
    const auto staging_path = GetRootPath() + "/mmap_local_io/payload";
    const auto data = GeneratePattern(kStreamSliceAlignment);
    {
        IndexEntryDirectStreamWriter writer(CreateOutputStream(file_path));
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }
    enum class Outcome { Success, Corrupt, Cancelled };
    for (auto outcome :
         {Outcome::Success, Outcome::Corrupt, Outcome::Cancelled}) {
        SCOPED_TRACE(static_cast<int>(outcome));
        milvus::test::ControlledDirectReadFile* direct_file = nullptr;
        auto reader = milvus::test::OpenDirectIndexEntryReader(
            ReadLocalFileBytes(GetRootPath() + "/" + file_path), &direct_file);
        if (outcome == Outcome::Corrupt) {
            const auto& source = std::get<PlainEntrySource>(
                reader->Directory().At("data").source);
            direct_file->CorruptRemoteByte(source.remote_offset + 7);
        }
        direct_file->SetAutoComplete(false);
        auto staging =
            std::make_shared<IndexFileTarget>(staging_path, data.size(), false);
        std::vector<EntryLoadPlan> entries;
        entries.push_back(
            EntryLoadPlan{"data", FileEntryTarget{staging, 0, data.size()}});
        folly::CancellationSource cancellation;
        auto load = std::async(std::launch::async, [&] {
            return folly::coro::blockingWait(
                ReadEntriesForTest(*reader,
                                   std::move(entries),
                                   milvus::proto::common::LoadPriority::HIGH,
                                   cancellation.getToken()));
        });
        auto drain = folly::makeGuard([&] {
            direct_file->SetAutoComplete(true);
            for (size_t i = 0; i < direct_file->DirectReadCalls().size(); ++i) {
                direct_file->Complete(i);
            }
        });
        ASSERT_TRUE(direct_file->WaitForCallCount(1));
        // Preparation has finished. Occupy the pool before completing the read.
        LocalFileIOBlocker blocker;
        if (outcome == Outcome::Cancelled) {
            cancellation.requestCancellation();
            EXPECT_EQ(load.wait_for(std::chrono::milliseconds(0)),
                      std::future_status::timeout);
        }
        direct_file->Complete(0);
        ASSERT_TRUE(blocker.WaitForQueuedTask());
        EXPECT_EQ(load.wait_for(std::chrono::milliseconds(0)),
                  std::future_status::timeout);
        ASSERT_TRUE(staging->Prepared());
        EXPECT_NO_THROW(staging->WriteAt(0, nullptr, 0));
        EXPECT_TRUE(std::filesystem::exists(staging_path));
        blocker.Release();
        if (outcome != Outcome::Success) {
            try {
                (void)load.get();
                FAIL() << "expected failed materialization";
            } catch (const milvus::SegcoreError& error) {
                if (outcome == Outcome::Cancelled) {
                    EXPECT_EQ(error.get_error_code(),
                              milvus::ErrorCode::FollyCancel);
                }
            }
        } else {
            auto plan = load.get();

            EXPECT_THROW(staging->WriteAt(0, nullptr, 0), milvus::SegcoreError);
        }
        EXPECT_FALSE(std::filesystem::exists(staging_path));
    }
}

TEST_F(AsyncIndexEntryReaderTest,
       DisablingLocalFileIOPoolDoesNotWaitForScalarRemoteRead) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto restore = folly::makeGuard([&] { pool.Configure(0); });
    const auto file_path = kV3FilePath + "_disable_mmap_pool";
    const auto staging_path = GetRootPath() + "/disable_mmap_pool/payload";
    const auto data = GeneratePattern(kStreamSliceAlignment);
    {
        IndexEntryDirectStreamWriter writer(CreateOutputStream(file_path));
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(
        ReadLocalFileBytes(GetRootPath() + "/" + file_path), &direct_file);
    direct_file->SetAutoComplete(false);
    auto staging =
        std::make_shared<IndexFileTarget>(staging_path, data.size(), false);
    std::vector<EntryLoadPlan> entries;
    entries.push_back(
        EntryLoadPlan{"data", FileEntryTarget{staging, 0, data.size()}});
    auto load = std::async(std::launch::async, [&] {
        return folly::coro::blockingWait(
            ReadEntriesForTest(*reader,
                               std::move(entries),
                               milvus::proto::common::LoadPriority::HIGH));
    });
    auto drain = folly::makeGuard([&] {
        direct_file->SetAutoComplete(true);
        for (size_t i = 0; i < direct_file->DirectReadCalls().size(); ++i) {
            direct_file->Complete(i);
        }
    });
    ASSERT_TRUE(direct_file->WaitForCallCount(1));
    auto configure = std::async(std::launch::async, [&] { pool.Configure(0); });
    auto complete_before_join =
        folly::makeGuard([&] { direct_file->Complete(0); });
    ASSERT_EQ(configure.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    configure.get();
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    direct_file->Complete(0);
    auto plan = load.get();

    EXPECT_THROW(staging->WriteAt(0, nullptr, 0), milvus::SegcoreError);
}

TEST_F(AsyncIndexEntryReaderTest,
       ReadEntriesCancelsPendingAdmissionAndDrainsIssuedReads) {
    const size_t slice_size = DefaultStreamSliceSize();
    milvus::test::ScopedLoadTransientBudget budget(2 * slice_size);
    const std::string file_path = kV3FilePath + "_materialize_failure_drain";
    auto data = GeneratePattern(4 * slice_size);
    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }

    auto packed = ReadLocalFileBytes(GetRootPath() + "/" + file_path);
    milvus::test::ControlledDirectReadFile* direct_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(std::move(packed),
                                                           &direct_file);
    direct_file->SetAutoComplete(false);

    auto target = std::make_shared<std::vector<uint8_t>>(data.size());
    std::vector<EntryLoadPlan> entries;

    entries.push_back(EntryLoadPlan{
        "data", MemoryEntryTarget{target, target->data(), target->size()}});

    auto materialize_future = std::async(
        std::launch::async,
        [reader = reader.get(), entries = std::move(entries)]() mutable {
            return folly::coro::blockingWait(
                ReadEntriesForTest(*reader,
                                   std::move(entries),
                                   milvus::proto::common::LoadPriority::HIGH));
        });
    auto drain_on_failure = folly::makeGuard([&] {
        direct_file->SetAutoComplete(true);
        const auto submitted = direct_file->DirectReadCalls().size();
        for (size_t i = 0; i < submitted; ++i) {
            direct_file->Complete(i);
        }
    });

    ASSERT_TRUE(direct_file->WaitForCallCount(2));
    direct_file->Complete(
        0,
        milvus_storage::MakeExtendError(
            milvus_storage::ExtendStatusCode::AwsErrorNonRetryable,
            "permanent storage error"));

    EXPECT_EQ(materialize_future.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    EXPECT_EQ(direct_file->DirectReadCalls().size(), 2);

    direct_file->Complete(1);
    try {
        (void)materialize_future.get();
        FAIL() << "expected permanent storage error";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::ErrorCode::StorageError);
    }
    EXPECT_EQ(direct_file->DirectReadCalls().size(), 2);
}

TEST_F(AsyncIndexEntryReaderTest, EncryptedMaterializationUsesSharedExecutor) {
    auto cipher = std::make_shared<MockCipherPlugin>();
    PluginLoader::GetInstance().registerPluginForTest(cipher);
    auto unload = folly::makeGuard([] {
        PluginLoader::GetInstance().unregisterPluginForTest("CipherPlugin");
    });
    IndexEntryStreamConfigGuard budget_guard;
    auto& budget = LoadAdmissionController::GetInstance();
    budget.SetCapacityBytes(kStreamSliceAlignment);
    auto data = GeneratePattern(2 * kStreamSliceAlignment + 71);
    data[kStreamSliceAlignment + 11] ^= 0xFF;
    const std::vector<uint8_t> other_data(kStreamSliceAlignment + 37, 0xA5);
    const auto path = kV3FilePath + "_encrypted_materialize";
    {
        IndexEntryEncryptedLocalWriter writer(
            path, fs_, cipher, 1, 100, GetRootPath(), kStreamSliceAlignment);
        writer.WriteEntry("data", data.data(), data.size());
        writer.WriteEntry("other", other_data.data(), other_data.size());
        writer.Finish();
    }
    // Read multiple encrypted entries in reverse directory order to exercise
    // per-entry slice indices, distinct full slices, and partial final slices.
    for (const auto priority : {milvus::proto::common::LoadPriority::HIGH,
                                milvus::proto::common::LoadPriority::LOW}) {
        auto input =
            std::make_shared<RecordingInputStream>(CreateInputStream(path));
        auto reader = folly::coro::blockingWait(
            AsyncIndexEntryReader::Open(input, 100, priority)
                .scheduleOn(
                    milvus::storage::ResolveAsyncLoadExecutor({}, priority)));
        auto target = std::make_shared<std::vector<uint8_t>>(data.size());
        auto other_target =
            std::make_shared<std::vector<uint8_t>>(other_data.size());
        std::vector<EntryLoadPlan> entries;
        entries.push_back(EntryLoadPlan{
            "other",
            MemoryEntryTarget{
                other_target, other_target->data(), other_target->size()}});
        entries.push_back(EntryLoadPlan{
            "data", MemoryEntryTarget{target, target->data(), target->size()}});
        ASSERT_EQ(std::get<EncryptedEntrySource>(
                      reader->Directory().At("data").source)
                      .slices.size(),
                  3);
        auto sync_input = CreateInputStream(path);
        auto sync_reader =
            IndexEntryReader::Open(sync_input, sync_input->Size(), 100);
        for (const auto* name : {"data", "other"}) {
            const auto& sync_entry = sync_reader->Directory().At(name);
            const auto& async_entry = reader->Directory().At(name);
            EXPECT_EQ(sync_entry.plaintext_size, async_entry.plaintext_size);
            EXPECT_EQ(sync_entry.expected_crc, async_entry.expected_crc);
            const auto& sync_slices =
                std::get<EncryptedEntrySource>(sync_entry.source).slices;
            const auto& async_slices =
                std::get<EncryptedEntrySource>(async_entry.source).slices;
            ASSERT_EQ(sync_slices.size(), async_slices.size());
            for (size_t i = 0; i < sync_slices.size(); ++i) {
                EXPECT_EQ(sync_slices[i].remote_offset,
                          async_slices[i].remote_offset);
                EXPECT_EQ(sync_slices[i].remote_bytes,
                          async_slices[i].remote_bytes);
                EXPECT_EQ(sync_slices[i].plaintext_offset,
                          async_slices[i].plaintext_offset);
                EXPECT_EQ(sync_slices[i].plaintext_bytes,
                          async_slices[i].plaintext_bytes);
            }
        }
        EXPECT_EQ(sync_reader->ReadEntry("data").data, data);
        EXPECT_EQ(sync_reader->ReadEntry("other").data, other_data);
        auto plan = folly::coro::blockingWait(
            ReadEntriesForTest(*reader, std::move(entries), priority)
                .scheduleOn(
                    milvus::storage::ResolveAsyncLoadExecutor({}, priority)));
        EXPECT_EQ(*target, data);
        EXPECT_EQ(*other_target, other_data);

        for (const auto& read : input->ReadRanges()) {
            EXPECT_TRUE(read.thread_name.starts_with("MILVUS_ASYNC"))
                << read.thread_name;
        }
        EXPECT_TRUE(budget.TryAcquire({kStreamSliceAlignment, 1},
                                      LoadAdmissionPriority::High));
        budget.Release({kStreamSliceAlignment, 1});
    }
}

TEST_F(AsyncIndexEntryReaderTest,
       DirectoryParserValidatesSourceRangesAndNames) {
    nlohmann::json json = {{"entries",
                            {{{"name", "data"},
                              {"offset", 0},
                              {"size", 32},
                              {"crc32", "00000000"}}}}};
    auto parse = [&](int64_t file_size) {
        const auto bytes = json.dump();
        return ParseIndexEntryDirectory(
            std::span(reinterpret_cast<const uint8_t*>(bytes.data()),
                      bytes.size()),
            file_size);
    };
    auto [directory, encryption] = parse(4096);
    EXPECT_FALSE(encryption.has_value());
    EXPECT_EQ(directory.At("data").plaintext_size, 32);
    EXPECT_EQ(
        std::get<PlainEntrySource>(directory.At("data").source).remote_offset,
        MILVUS_V3_MAGIC_SIZE);
    json["entries"].push_back(json["entries"][0]);
    EXPECT_THROW(parse(4096), milvus::SegcoreError);
    json["entries"].erase(1);
    json["entries"][0]["offset"] = 4096;
    EXPECT_THROW(parse(4096), milvus::SegcoreError);
    json["__edek__"] = "key";
    json["__ez_id__"] = "7";
    json["slice_size"] = 4096;
    json["entries"] = {{{"name", "data"},
                        {"original_size", 33},
                        {"crc32", "00000000"},
                        {"slices", {{{"offset", 0}, {"size", 48}}}}}};
    auto encrypted = parse(4096);
    ASSERT_TRUE(encrypted.second.has_value());
    EXPECT_EQ(encrypted.second->ez_id, 7);
    EXPECT_EQ(encrypted.second->edek, "key");
    const auto& source =
        std::get<EncryptedEntrySource>(encrypted.first.At("data").source);
    ASSERT_EQ(source.slices.size(), 1);
    EXPECT_EQ(source.slices[0].plaintext_bytes, 33);
    json["entries"][0]["original_size"] = 8192;
    EXPECT_THROW(parse(4096), milvus::SegcoreError);
}
