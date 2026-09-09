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
#include "storage/AsyncLoadExecutor.h"
#include "storage/IndexMaterializer.h"
#include "storage/LocalFileIOPool.h"
#include "storage/IndexLoadPlan.h"
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
OpenAsyncReader(std::shared_ptr<milvus::InputStream> input, int64_t file_size) {
    return folly::coro::blockingWait(
        AsyncIndexEntryReader::Open(
            input, file_size, 0, milvus::proto::common::LoadPriority::HIGH, {})
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
    ReadAt(void* ptr, size_t offset, size_t size) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            read_ranges_.push_back(
                {offset, size, folly::getCurrentThreadName().value_or("")});
        }
        return base_->ReadAt(ptr, offset, size);
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

    int64_t
    GetFileSize(const std::string& path) {
        auto info = fs_->GetFileInfo(path);
        EXPECT_TRUE(info.ok()) << info.status().ToString();
        return info.ValueOrDie().size();
    }

    milvus_storage::ArrowFileSystemPtr fs_;
};

TEST_F(AsyncIndexEntryReaderTest, InvalidMappingSizeDoesNotRemoveExistingFile) {
    const auto path = GetRootPath() + "/existing_file";
    std::filesystem::create_directories(GetRootPath());
    {
        std::ofstream output(path);
        output << "keep";
    }
    EXPECT_THROW(
        WritableMmapFile::Create(path, std::numeric_limits<size_t>::max()),
        milvus::SegcoreError);
    ASSERT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(ReadLocalFileBytes(path),
              (std::vector<uint8_t>{'k', 'e', 'e', 'p'}));
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
                    input->Size(),
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
    const bool started = direct_file->WaitForCallCount(1);
    EXPECT_TRUE(started);
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    cancel.requestCancellation();
    // Always release before asserting so a regression cannot strand the task.
    held.Release();
    EXPECT_EQ(load.get(), milvus::ErrorCode::FollyCancel);
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
        writer.PutMeta("catalog_value", 17);
        writer.Finish();
    }

    auto recording =
        std::make_shared<RecordingInputStream>(CreateInputStream(file_path));
    auto reader = OpenAsyncReader(recording, GetFileSize(file_path));
    auto reads_after_open = recording->ReadRanges().size();

    static_assert(
        std::is_same_v<decltype(reader->Catalog()), const IndexEntryCatalog&>);
    const auto& entries = reader->Catalog().Entries();
    ASSERT_EQ(entries.size(), 3);

    EXPECT_EQ(reader->Catalog().At("alpha").name, "alpha");
    EXPECT_EQ(reader->Catalog().At("alpha").plaintext_size, alpha.size());
    EXPECT_EQ(reader->Catalog().At("alpha").expected_crc,
              Crc32cValue(alpha.data(), alpha.size()));
    ASSERT_TRUE(std::holds_alternative<PlainEntrySource>(
        reader->Catalog().At("alpha").source));
    const auto& alpha_source =
        std::get<PlainEntrySource>(reader->Catalog().At("alpha").source);
    EXPECT_EQ(alpha_source.remote_offset, MILVUS_V3_MAGIC_SIZE);
    EXPECT_EQ(alpha_source.remote_bytes, alpha.size());

    EXPECT_EQ(reader->Catalog().At("beta").name, "beta");
    EXPECT_EQ(reader->Catalog().At("beta").plaintext_size, beta.size());
    const auto& beta_source =
        std::get<PlainEntrySource>(reader->Catalog().At("beta").source);
    EXPECT_EQ(beta_source.remote_offset, MILVUS_V3_MAGIC_SIZE + alpha.size());
    EXPECT_EQ(beta_source.remote_bytes, beta.size());

    EXPECT_EQ(reader->Catalog().At(MILVUS_V3_META_ENTRY_NAME).name,
              MILVUS_V3_META_ENTRY_NAME);
    EXPECT_TRUE(reader->Catalog().HasMeta("catalog_value"));
    EXPECT_EQ(reader->Catalog().GetMeta<int>("catalog_value"), 17);
    EXPECT_THROW(reader->Catalog().At("missing"), milvus::SegcoreError);

    EXPECT_EQ(recording->ReadRanges().size(), reads_after_open);
}

TEST_F(AsyncIndexEntryReaderTest,
       DirectPlainSliceUsesExactlyOneNativeCallerOwnedRead) {
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

    const size_t entry_offset = 97;
    std::vector<uint8_t> target(kStreamSliceAlignment + 31);
    folly::coro::blockingWait(reader->ReadSliceIntoAsync(
        "data", entry_offset, target.data(), target.size()));

    EXPECT_TRUE(
        std::equal(target.begin(), target.end(), data.begin() + entry_offset));
    auto calls = direct_file->DirectReadCalls();
    ASSERT_EQ(calls.size(), 1);
    const auto& source =
        std::get<PlainEntrySource>(reader->Catalog().At("data").source);
    EXPECT_EQ(calls[0].position, source.remote_offset + entry_offset);
    EXPECT_EQ(calls[0].nbytes, target.size());
    EXPECT_EQ(calls[0].destination, target.data());
    EXPECT_EQ(direct_file->AsyncReadCalls(), 0);
    EXPECT_EQ(direct_file->PeakInflight(), 1);
}

TEST_F(AsyncIndexEntryReaderTest, PlainSliceFallsBackToArrowBufferRead) {
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

    std::vector<uint8_t> target(128);
    folly::coro::blockingWait(
        reader->ReadSliceIntoAsync("data", 0, target.data(), target.size()));

    EXPECT_TRUE(std::equal(target.begin(), target.end(), data.begin()));
    EXPECT_EQ(fallback_file->AsyncReadCalls(), 1);
    EXPECT_EQ(fallback_file->ReadAtCalls(), 0);
}

TEST_F(AsyncIndexEntryReaderTest, DirectPlainSliceRejectsShortRead) {
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

    std::vector<uint8_t> target(128);
    EXPECT_THROW(folly::coro::blockingWait(reader->ReadSliceIntoAsync(
                     "data", 0, target.data(), target.size())),
                 milvus::SegcoreError);
}

TEST_F(AsyncIndexEntryReaderTest,
       DirectPlainSliceRetriesTransientStorageError) {
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

    std::vector<uint8_t> target(128);
    folly::coro::blockingWait(
        reader->ReadSliceIntoAsync("data", 0, target.data(), target.size()));

    EXPECT_TRUE(std::equal(target.begin(), target.end(), data.begin()));
    EXPECT_EQ(direct_file->DirectReadCalls().size(), 2);
}

TEST_F(AsyncIndexEntryReaderTest,
       DirectPlainSliceCancellationStopsAsyncReadRetry) {
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
    std::vector<uint8_t> target(128);
    auto read_future = std::async(std::launch::async, [&]() {
        folly::coro::blockingWait(
            reader->ReadSliceIntoAsync("data",
                                       0,
                                       target.data(),
                                       target.size(),
                                       cancellation_source.getToken()));
    });

    ASSERT_TRUE(direct_file->WaitForCallCount(1));
    cancellation_source.requestCancellation();
    direct_file->Complete(
        0,
        milvus_storage::MakeExtendError(
            milvus_storage::ExtendStatusCode::StorageTransientThrottling,
            "throttled"));

    auto wait_status = read_future.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(wait_status, std::future_status::ready);
    if (wait_status != std::future_status::ready) {
        direct_file->SetAutoComplete(true);
        ASSERT_TRUE(direct_file->WaitForCallCount(2));
        direct_file->Complete(1);
        ASSERT_EQ(read_future.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
    }

    try {
        read_future.get();
        FAIL() << "expected cancellation";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::ErrorCode::FollyCancel);
    }
    EXPECT_EQ(direct_file->DirectReadCalls().size(), 1);
}

TEST_F(AsyncIndexEntryReaderTest,
       MaterializerCombinesOutOfOrderSlicesAcrossEntries) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_round_robin";
    const size_t slice_size = kStreamSliceAlignment;
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
    IndexLoadPlan plan;
    plan.priority = milvus::proto::common::LoadPriority::HIGH;
    plan.max_inflight_slices = 4;
    plan.entries.push_back(MakeEntryLoadPlan(
        reader->Catalog(),
        "a",
        MemoryEntryTarget{target_a, target_a->data(), target_a->size()},
        slice_size));
    plan.entries.push_back(MakeEntryLoadPlan(
        reader->Catalog(),
        "b",
        MemoryEntryTarget{target_b, target_b->data(), target_b->size()},
        slice_size));

    auto materialize_future =
        std::async(std::launch::async,
                   [reader = reader.get(), plan = std::move(plan)]() mutable {
                       return folly::coro::blockingWait(
                           MaterializeIndexAsync(*reader, std::move(plan)));
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
        std::get<PlainEntrySource>(reader->Catalog().At("a").source);
    const auto& source_b =
        std::get<PlainEntrySource>(reader->Catalog().At("b").source);
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

    auto artifact = materialize_future.get();
    EXPECT_EQ(*target_a, data_a);
    EXPECT_EQ(*target_b, data_b);
    EXPECT_TRUE(artifact.At("a").ready);
    EXPECT_TRUE(artifact.At("b").ready);
    EXPECT_EQ(direct_file->PeakInflight(), 4);
}

TEST_F(AsyncIndexEntryReaderTest,
       MaterializerUsesBufferedAsyncReadWhenNativeReadIntoIsUnavailable) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_buffered";
    const size_t slice_size = kStreamSliceAlignment;
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
    IndexLoadPlan plan;
    plan.max_inflight_slices = 2;
    plan.entries.push_back(MakeEntryLoadPlan(
        reader->Catalog(),
        "data",
        MemoryEntryTarget{target, target->data(), target->size()},
        slice_size));

    auto artifact = folly::coro::blockingWait(
        MaterializeIndexAsync(*reader, std::move(plan)));

    EXPECT_TRUE(artifact.At("data").ready);
    EXPECT_EQ(*target, data);
    EXPECT_EQ(fallback_file->AsyncReadCalls(), 2);
    EXPECT_EQ(fallback_file->ReadAtCalls(), 0);
}

TEST_F(AsyncIndexEntryReaderTest, MaterializerHonorsMaxInflightSlices) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_inflight";
    const size_t slice_size = kStreamSliceAlignment;
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
    IndexLoadPlan plan;
    plan.priority = milvus::proto::common::LoadPriority::LOW;
    plan.max_inflight_slices = 2;
    plan.entries.push_back(MakeEntryLoadPlan(
        reader->Catalog(),
        "data",
        MemoryEntryTarget{target, target->data(), target->size()},
        slice_size));

    auto materialize_future =
        std::async(std::launch::async,
                   [reader = reader.get(), plan = std::move(plan)]() mutable {
                       return folly::coro::blockingWait(
                           MaterializeIndexAsync(*reader, std::move(plan)));
                   });
    auto drain_on_failure = folly::makeGuard([&] {
        direct_file->SetAutoComplete(true);
        const auto submitted = direct_file->DirectReadCalls().size();
        for (size_t i = 0; i < submitted; ++i) {
            direct_file->Complete(i);
        }
    });

    ASSERT_TRUE(direct_file->WaitForCallCount(2));
    EXPECT_EQ(direct_file->DirectReadCalls().size(), 2);
    direct_file->Complete(1);
    ASSERT_TRUE(direct_file->WaitForCallCount(3));
    EXPECT_EQ(direct_file->DirectReadCalls().size(), 3);
    direct_file->Complete(0);
    ASSERT_TRUE(direct_file->WaitForCallCount(4));
    EXPECT_EQ(direct_file->DirectReadCalls().size(), 4);
    direct_file->Complete(3);
    direct_file->Complete(2);

    auto artifact = materialize_future.get();
    EXPECT_TRUE(artifact.At("data").ready);
    EXPECT_EQ(*target, data);
    EXPECT_LE(direct_file->PeakInflight(), 2);
}

TEST_F(AsyncIndexEntryReaderTest,
       MaterializerSharesSlotsAndObservesCapacityChanges) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_joint_slots";
    const size_t slice_size = kStreamSliceAlignment;
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
    IndexLoadPlan plan;
    plan.priority = milvus::proto::common::LoadPriority::LOW;
    plan.max_inflight_slices = 4;
    plan.entries.push_back(MakeEntryLoadPlan(
        reader->Catalog(),
        "data",
        MemoryEntryTarget{target, target->data(), target->size()},
        slice_size));

    auto materialize_future =
        std::async(std::launch::async,
                   [reader = reader.get(), plan = std::move(plan)]() mutable {
                       return folly::coro::blockingWait(
                           MaterializeIndexAsync(*reader, std::move(plan)));
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

    auto artifact = materialize_future.get();
    EXPECT_TRUE(artifact.At("data").ready);
    EXPECT_EQ(*target, data);
    EXPECT_LE(direct_file->PeakInflight(), 2);
    admission.SetCapacitySlots(2);
    ASSERT_TRUE(admission.TryAcquire({0, 2}, LoadAdmissionPriority::High));
    admission.Release({0, 2});
}

TEST_F(AsyncIndexEntryReaderTest,
       MaterializerRejectsOverlappingMemoryEntryTargetsBeforeRead) {
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
    IndexLoadPlan plan;
    plan.entries.push_back(
        MakeEntryLoadPlan(reader->Catalog(),
                          "a",
                          MemoryEntryTarget{target, target->data(), entry_size},
                          entry_size));
    plan.entries.push_back(MakeEntryLoadPlan(
        reader->Catalog(),
        "b",
        MemoryEntryTarget{target, target->data() + entry_size / 2, entry_size},
        entry_size));

    EXPECT_THROW(folly::coro::blockingWait(
                     MaterializeIndexAsync(*reader, std::move(plan))),
                 milvus::SegcoreError);
    EXPECT_TRUE(direct_file->DirectReadCalls().empty());
}

TEST_F(AsyncIndexEntryReaderTest,
       MaterializerRejectsOverlappingMmapEntryTargetsBeforePrepare) {
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

    auto staging = std::make_shared<MmapFileTarget>(
        MmapFileTarget{staging_path, 2 * entry_size, false, nullptr});
    IndexLoadPlan plan;
    plan.entries.push_back(
        MakeEntryLoadPlan(reader->Catalog(),
                          "a",
                          MmapEntryTarget{staging, 0, entry_size},
                          entry_size));
    plan.entries.push_back(
        MakeEntryLoadPlan(reader->Catalog(),
                          "b",
                          MmapEntryTarget{staging, entry_size / 2, entry_size},
                          entry_size));

    EXPECT_THROW(folly::coro::blockingWait(
                     MaterializeIndexAsync(*reader, std::move(plan))),
                 milvus::SegcoreError);
    EXPECT_TRUE(direct_file->DirectReadCalls().empty());
    EXPECT_FALSE(std::filesystem::exists(staging_path));
}

TEST_F(AsyncIndexEntryReaderTest,
       MaterializerFinishesWritableMmapBeforeIndexFinalize) {
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
    auto staging = std::make_shared<MmapFileTarget>(
        MmapFileTarget{staging_path, data.size(), false, nullptr});
    IndexLoadPlan plan;
    plan.entries.push_back(
        MakeEntryLoadPlan(reader->Catalog(),
                          "data",
                          MmapEntryTarget{staging, 0, data.size()},
                          kStreamSliceAlignment));

    {
        auto artifact = folly::coro::blockingWait(
            MaterializeIndexAsync(*reader, std::move(plan)));
        EXPECT_TRUE(artifact.At("data").ready);
        ASSERT_NE(staging->file, nullptr);
        EXPECT_THROW((void)staging->file->Region(0, 1), milvus::SegcoreError);
        EXPECT_TRUE(std::filesystem::exists(staging_path));
    }
    EXPECT_FALSE(std::filesystem::exists(staging_path));
}

TEST_F(AsyncIndexEntryReaderTest,
       MaterializerRejectsCombinedCrcAndRemovesStagingFile) {
    milvus::test::ScopedLoadTransientBudget budget(/*capacity_bytes=*/0);
    const std::string file_path = kV3FilePath + "_materialize_crc";
    const std::string staging_path = GetRootPath() + "/materialize_crc.mmap";
    const size_t slice_size = kStreamSliceAlignment;
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
        std::get<PlainEntrySource>(reader->Catalog().At("data").source);
    direct_file->CorruptRemoteByte(source.remote_offset + slice_size + 7);

    IndexLoadPlan plan;
    plan.max_inflight_slices = 2;
    auto staging = std::make_shared<MmapFileTarget>(
        MmapFileTarget{staging_path, data.size(), false, nullptr});
    plan.entries.push_back(
        MakeEntryLoadPlan(reader->Catalog(),
                          "data",
                          MmapEntryTarget{staging, 0, data.size()},
                          slice_size));

    EXPECT_THROW(folly::coro::blockingWait(
                     MaterializeIndexAsync(*reader, std::move(plan))),
                 milvus::SegcoreError);
    EXPECT_FALSE(std::filesystem::exists(staging_path));
}

TEST_F(AsyncIndexEntryReaderTest, MaterializerCancelsQueuedMmapPreparation) {
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
    auto staging = std::make_shared<MmapFileTarget>(
        MmapFileTarget{staging_path, data.size(), false, nullptr});
    IndexLoadPlan plan;
    plan.entries.push_back(
        MakeEntryLoadPlan(reader->Catalog(),
                          "data",
                          MmapEntryTarget{staging, 0, data.size()},
                          kStreamSliceAlignment));
    std::string cleanup_thread;
    plan.finalize_context = std::shared_ptr<void>(nullptr, [&](void*) {
        cleanup_thread = folly::getCurrentThreadName().value_or("");
    });
    LocalFileIOBlocker blocker;
    folly::CancellationSource cancellation;
    auto load = std::async(std::launch::async, [&] {
        return folly::coro::blockingWait(MaterializeIndexAsync(
            *reader, std::move(plan), cancellation.getToken()));
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
    EXPECT_TRUE(cleanup_thread.starts_with("MILVUS_LF_IO_"));
}

TEST_F(AsyncIndexEntryReaderTest,
       MaterializerFinishesAndCleansMmapOnLocalFileIOPool) {
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
            const auto& source =
                std::get<PlainEntrySource>(reader->Catalog().At("data").source);
            direct_file->CorruptRemoteByte(source.remote_offset + 7);
        }
        direct_file->SetAutoComplete(false);
        auto staging = std::make_shared<MmapFileTarget>(
            MmapFileTarget{staging_path, data.size(), false, nullptr});
        std::string cleanup_thread;
        IndexLoadPlan plan;
        plan.finalize_context = std::shared_ptr<void>(nullptr, [&](void*) {
            cleanup_thread = folly::getCurrentThreadName().value_or("");
        });
        plan.entries.push_back(
            MakeEntryLoadPlan(reader->Catalog(),
                              "data",
                              MmapEntryTarget{staging, 0, data.size()},
                              kStreamSliceAlignment));
        folly::CancellationSource cancellation;
        auto load = std::async(std::launch::async, [&] {
            return folly::coro::blockingWait(MaterializeIndexAsync(
                *reader, std::move(plan), cancellation.getToken()));
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
        ASSERT_NE(staging->file, nullptr);
        EXPECT_NO_THROW((void)staging->file->Region(0, 1));
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
            EXPECT_TRUE(cleanup_thread.starts_with("MILVUS_LF_IO_"));
        } else {
            auto artifact = load.get();
            EXPECT_TRUE(artifact.At("data").ready);
            EXPECT_THROW((void)staging->file->Region(0, 1),
                         milvus::SegcoreError);
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
    auto staging = std::make_shared<MmapFileTarget>(
        MmapFileTarget{staging_path, data.size(), false, nullptr});
    IndexLoadPlan plan;
    plan.entries.push_back(
        MakeEntryLoadPlan(reader->Catalog(),
                          "data",
                          MmapEntryTarget{staging, 0, data.size()},
                          kStreamSliceAlignment));
    auto load = std::async(std::launch::async, [&] {
        return folly::coro::blockingWait(
            MaterializeIndexAsync(*reader, std::move(plan)));
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
    auto artifact = load.get();
    EXPECT_TRUE(artifact.At("data").ready);
    EXPECT_THROW((void)staging->file->Region(0, 1), milvus::SegcoreError);
}

TEST_F(AsyncIndexEntryReaderTest,
       MaterializerCancelsPendingAdmissionAndDrainsIssuedReads) {
    const size_t slice_size = kStreamSliceAlignment;
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
    IndexLoadPlan plan;
    plan.max_inflight_slices = 4;
    plan.entries.push_back(MakeEntryLoadPlan(
        reader->Catalog(),
        "data",
        MemoryEntryTarget{target, target->data(), target->size()},
        slice_size));

    auto materialize_future =
        std::async(std::launch::async,
                   [reader = reader.get(), plan = std::move(plan)]() mutable {
                       return folly::coro::blockingWait(
                           MaterializeIndexAsync(*reader, std::move(plan)));
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
    const auto data = GeneratePattern(2 * kStreamSliceAlignment + 71);
    const auto path = kV3FilePath + "_encrypted_materialize";
    {
        IndexEntryEncryptedLocalWriter writer(
            path, fs_, cipher, 1, 100, GetRootPath(), kStreamSliceAlignment);
        writer.WriteEntry("data", data.data(), data.size());
        writer.Finish();
    }
    // Wrapping InputStream exercises the non-native backend inside the new
    // reader, including encrypted metadata and multi-slice plaintext targets.
    for (const auto priority : {milvus::proto::common::LoadPriority::HIGH,
                                milvus::proto::common::LoadPriority::LOW}) {
        auto input =
            std::make_shared<RecordingInputStream>(CreateInputStream(path));
        auto reader = folly::coro::blockingWait(
            AsyncIndexEntryReader::Open(input, GetFileSize(path), 100, priority)
                .scheduleOn(
                    milvus::storage::ResolveAsyncLoadExecutor({}, priority)));
        auto target = std::make_shared<std::vector<uint8_t>>(data.size());
        IndexLoadPlan plan;
        plan.priority = priority;
        plan.entries.push_back(MakeEntryLoadPlan(
            reader->Catalog(),
            "data",
            MemoryEntryTarget{target, target->data(), target->size()},
            kStreamSliceAlignment));
        ASSERT_EQ(plan.entries[0].slices.size(), 3);
        const auto& first = plan.entries[0].slices.front();
        EXPECT_EQ(first.admission_bytes,
                  2 * (kStreamSliceAlignment + 1) + kStreamSliceAlignment);
        auto artifact = folly::coro::blockingWait(
            MaterializeIndexAsync(*reader, std::move(plan))
                .scheduleOn(
                    milvus::storage::ResolveAsyncLoadExecutor({}, priority)));
        EXPECT_EQ(*target, data);
        EXPECT_TRUE(artifact.At("data").ready);
        for (const auto& read : input->ReadRanges()) {
            EXPECT_TRUE(read.thread_name.starts_with("MILVUS_ASYNC"))
                << read.thread_name;
        }
        EXPECT_TRUE(budget.TryAcquire({kStreamSliceAlignment, 1},
                                      LoadAdmissionPriority::High));
        budget.Release({kStreamSliceAlignment, 1});
    }
}
