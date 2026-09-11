// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <set>

#include "arrow/filesystem/localfs.h"
#include "folly/ScopeGuard.h"
#include "folly/system/ThreadName.h"
#include "index/json_stats/bson_inverted.h"
#include "segcore/storagev1translator/BsonInvertedIndexTranslator.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/LegacyIndexLoader.h"
#include "storage/FileWriter.h"
#include "storage/IndexData.h"
#include "storage/LocalFileIOPool.h"
#include "test_utils/AsyncLoadTestUtils.h"
#include "test_utils/TmpPath.h"

namespace milvus::index {
namespace {

using segcore::storagev1translator::BsonInvertedIndexLoadInfo;
using segcore::storagev1translator::BsonInvertedIndexTranslator;
constexpr auto kPriority = proto::common::LoadPriority::HIGH;

class BsonLoadSource : public storage::LocalChunkManager {
 public:
    using LocalChunkManager::LocalChunkManager;
    uint64_t
    Read(const std::string& path, void* data, uint64_t bytes) override {
        Observe();
        return LocalChunkManager::Read(path, data, bytes);
    }
    uint64_t
    Read(const std::string& path,
         uint64_t offset,
         void* data,
         uint64_t bytes) override {
        Observe();
        return LocalChunkManager::Read(path, offset, data, bytes);
    }
    void
    Observe() {
        ++reads;
        if (observe) {
            const auto thread = folly::getCurrentThreadName().value_or("");
            EXPECT_EQ(thread.starts_with("MILVUS_ASYNC"), expect_async);
            if (expect_async) {
                auto& admission =
                    storage::LoadAdmissionController::GetInstance();
                const bool acquired = admission.TryAcquire(
                    {0, 1}, storage::LoadAdmissionPriority::High);
                EXPECT_FALSE(acquired);
                if (acquired) {
                    admission.Release({0, 1});
                }
            }
        }
    }
    bool observe{false};
    bool expect_async{false};
    std::atomic<size_t> reads{0};
};

class BsonNativeFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    BsonNativeFileSystem()
        : SubTreeFileSystem("",
                            std::make_shared<arrow::fs::LocalFileSystem>()) {
    }
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
            "MILVUS_ASYNC"));
        if (on_open) {
            on_open(path);
        }
        return std::static_pointer_cast<arrow::io::RandomAccessFile>(
            files.at(path));
    }
    std::map<std::string, std::shared_ptr<test::ControlledDirectReadFile>>
        files;
    std::function<void(const std::string&)> on_open;
};

class BsonInvertedIndexAsyncLoadTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        old_enabled_ =
            segcore::storagev2translator::StorageV2AsyncLoadEnabled();
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(true);
        old_workers_ = storage::GetAsyncLoadThreadPoolSize();
        storage::SetAsyncLoadThreadPoolSize(1);
        auto& admission = storage::LoadAdmissionController::GetInstance();
        old_slots_ = admission.CapacitySlots();
        admission.SetCapacitySlots(1);
        storage::LocalFileIOPool::GetInstance().Configure(1);
        source_ = std::make_shared<BsonLoadSource>(temporary_.get().string());
        old_remote_root_ =
            std::exchange(kOverrideRootPathForUT, temporary_.get().string());
        std::filesystem::create_directories(
            (temporary_.get() / "build").string());
        BsonInvertedIndex built((temporary_.get() / "build").string(),
                                101,
                                Context(),
                                TANTIVY_INDEX_LATEST_VERSION);
        for (uint32_t row = 0; row < 64; ++row) {
            built.AddRecord("shared", row, row * 17);
            built.AddRecord("other", row, row * 17 + 1);
        }
        built.BuildIndex();
        files_ = built.UploadIndex()->GetIndexFiles();
        ASSERT_FALSE(files_.empty());
        source_->reads = 0;
    }
    void
    TearDown() override {
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const bool acquired =
            admission.TryAcquire({1, 1}, storage::LoadAdmissionPriority::High);
        EXPECT_TRUE(acquired);
        if (acquired) {
            admission.Release({1, 1});
        }
        kOverrideRootPathForUT = std::move(old_remote_root_);
        admission.SetCapacitySlots(old_slots_);
        storage::LocalFileIOPool::GetInstance().Configure(0);
        storage::SetAsyncLoadThreadPoolSize(old_workers_);
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(
            old_enabled_);
    }
    storage::FileManagerContext
    Context() const {
        proto::schema::FieldSchema schema;
        schema.set_data_type(proto::schema::DataType::JSON);
        return storage::FileManagerContext(
            storage::FieldDataMeta{1, 2, 3, 101, schema},
            storage::IndexMeta{3, 101, 92001, 2},
            source_,
            nullptr);
    }
    BsonInvertedIndexLoadInfo
    LoadInfo(bool mmap) const {
        BsonInvertedIndexLoadInfo info{};
        info.enable_mmap = mmap;
        info.segment_id = 3;
        info.field_id = 101;
        info.index_files = files_;
        info.load_priority = kPriority;
        info.warmup_policy = "disable";
        // Exercise file-aware estimates even when the caller has no size.
        info.index_size = 0;
        return info;
    }
    void
    CheckQuery(BsonInvertedIndex& index) const {
        std::set<std::pair<uint32_t, uint32_t>> actual;
        index.TermQuery(
            "shared",
            [&](const uint32_t* rows, const uint32_t* offsets, int64_t count) {
                for (int64_t i = 0; i < count; ++i) {
                    actual.emplace(rows[i], offsets[i]);
                }
            });
        ASSERT_EQ(actual.size(), 64);
        for (uint32_t row = 0; row < 64; ++row) {
            EXPECT_TRUE(actual.contains({row, row * 17}));
        }
        index.TermQuery("missing",
                        [](const uint32_t*, const uint32_t*, int64_t count) {
                            EXPECT_EQ(count, 0);
                        });
    }
    std::shared_ptr<BsonNativeFileSystem>
    NativeSource() {
        auto fs = std::make_shared<BsonNativeFileSystem>();
        for (const auto& path : files_) {
            std::vector<uint8_t> bytes(source_->Size(path));
            source_->Read(path, bytes.data(), bytes.size());
            fs->files.emplace(path,
                              std::make_shared<test::ControlledDirectReadFile>(
                                  std::move(bytes)));
        }
        return fs;
    }
    test::TmpPath temporary_;
    test::ScopedLoadTransientBudget budget_{64 * 1024 * 1024};
    std::shared_ptr<BsonLoadSource> source_;
    std::vector<std::string> files_;
    std::string old_remote_root_;
    bool old_enabled_;
    int old_workers_;
    size_t old_slots_;
};

TEST_F(BsonInvertedIndexAsyncLoadTest, HeapAndMmapRoutingAndReload) {
    for (const bool mmap : {false, true}) {
        for (const auto priority :
             {kPriority, proto::common::LoadPriority::LOW}) {
            auto info = LoadInfo(mmap);
            info.load_priority = priority;
            BsonInvertedIndexTranslator translator(info, Context());
            const auto estimate = translator.estimated_byte_size_of_cell(0);
            for (const bool enabled : {true, false, true}) {
                segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(
                    enabled);
                source_->observe = true;
                source_->expect_async = enabled;
                source_->reads = 0;
                auto cells = translator.get_cells(nullptr, {0});
                ASSERT_EQ(cells.size(), 1);
                EXPECT_GT(source_->reads.load(), 0);
                CheckQuery(*cells.front().second);
                const auto actual = cells.front().second->CellByteSize();
                EXPECT_GE(estimate.first.memory_bytes, actual.memory_bytes);
                EXPECT_GE(estimate.first.file_bytes, actual.file_bytes);
                EXPECT_EQ(estimate, translator.estimated_byte_size_of_cell(0));
                source_->observe = false;
            }
        }
    }
}

TEST_F(BsonInvertedIndexAsyncLoadTest, CompatibilityEntryRemainsSynchronous) {
    source_->observe = true;
    source_->expect_async = false;
    auto manager = std::make_shared<storage::DiskFileManagerImpl>(Context());
    BsonInvertedIndex loaded(manager);
    loaded.LoadIndex(files_, kPriority, true);
    CheckQuery(loaded);
}

TEST_F(BsonInvertedIndexAsyncLoadTest, MultiplePersistedSlices) {
    // Force actual legacy slice envelopes, including numeric suffixes >= 10.
    const auto old_slice_size = FILE_SLICE_SIZE.exchange(32);
    const auto restore =
        folly::makeGuard([&] { FILE_SLICE_SIZE = old_slice_size; });
    std::filesystem::create_directories(
        (temporary_.get() / "sliced-build").string());
    BsonInvertedIndex built((temporary_.get() / "sliced-build").string(),
                            101,
                            Context(),
                            TANTIVY_INDEX_LATEST_VERSION);
    for (uint32_t row = 0; row < 64; ++row) {
        built.AddRecord("shared", row, row * 17);
    }
    built.BuildIndex();
    const auto files = built.UploadIndex()->GetIndexFiles();
    ASSERT_TRUE(std::any_of(files.begin(), files.end(), [](const auto& path) {
        return path.ends_with("_10");
    }));
    for (const bool mmap : {false, true}) {
        auto manager =
            std::make_shared<storage::DiskFileManagerImpl>(Context());
        BsonInvertedIndex loaded(manager);
        loaded.LoadIndex(files, kPriority, mmap, nullptr);
        CheckQuery(loaded);
        EXPECT_EQ(std::filesystem::exists(
                      manager->GetLocalJsonStatsSharedIndexPrefix()),
                  mmap);
    }
}

TEST_F(BsonInvertedIndexAsyncLoadTest,
       NativePayloadReadSuspendsAndDrainsOnCancel) {
    for (const bool cancel_load : {false, true}) {
        auto fs = NativeSource();
        auto context = Context();
        context.fs = fs;
        auto manager = std::make_shared<storage::DiskFileManagerImpl>(context);
        BsonInvertedIndex loaded(manager);
        const auto directory = manager->GetLocalJsonStatsPrefix();
        // Pause after inspection, once a local destination exists.
        const auto first_path = fs->files.begin()->first;
        auto first = fs->files.begin()->second;
        size_t opens = 0;
        std::promise<void> payload_opened;
        auto payload_ready = payload_opened.get_future();
        fs->on_open = [&](const std::string& path) {
            if (path == first_path && ++opens == 2) {
                first->ResetCounters();
                first->SetAutoComplete(false);
                payload_opened.set_value();
            }
        };
        folly::CancellationSource cancel;
        OpContext op;
        op.cancellation_token = cancel.getToken();
        auto task = std::async(std::launch::async, [&] {
            loaded.LoadIndex(files_, kPriority, true, &op);
        });
        auto started = std::make_shared<std::promise<void>>();
        auto ready = started->get_future();
        auto cleanup = folly::makeGuard([&] {
            cancel.requestCancellation();
            first->SetAutoComplete(true);
            for (size_t i = 0; i < first->DirectReadCalls().size(); ++i) {
                first->Complete(i);
            }
            task.wait();
        });
        ASSERT_EQ(payload_ready.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        ASSERT_TRUE(first->WaitForCallCount(1));
        // Queue a probe behind the metadata work on the single async worker.
        storage::ResolveAsyncLoadExecutor({}, kPriority)->add([started] {
            started->set_value();
        });
        ASSERT_EQ(ready.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        ASSERT_TRUE(first->WaitForCallCount(1));
        EXPECT_TRUE(std::filesystem::exists(directory));
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const bool acquired =
            admission.TryAcquire({0, 1}, storage::LoadAdmissionPriority::High);
        EXPECT_FALSE(acquired);
        if (acquired) {
            admission.Release({0, 1});
        }
        if (cancel_load) {
            cancel.requestCancellation();
        }
        EXPECT_EQ(task.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        first->SetAutoComplete(true);
        first->Complete(0);
        task.wait();
        cleanup.dismiss();
        if (cancel_load) {
            try {
                task.get();
                FAIL() << "cancelled load must fail";
            } catch (const SegcoreError& error) {
                EXPECT_EQ(error.get_error_code(), FollyCancel);
            }
            EXPECT_FALSE(std::filesystem::exists(directory));
        } else {
            EXPECT_NO_THROW(task.get());
            CheckQuery(loaded);
        }
        for (const auto& [path, file] : fs->files) {
            EXPECT_EQ(file->ReadAtCalls(), 0);
            EXPECT_EQ(file->AsyncReadCalls(), 0);
        }
    }
}

TEST_F(BsonInvertedIndexAsyncLoadTest, CancelWaitingForAdmission) {
    auto& admission = storage::LoadAdmissionController::GetInstance();
    ASSERT_TRUE(
        admission.TryAcquire({0, 1}, storage::LoadAdmissionPriority::High));
    const auto release = folly::makeGuard([&] { admission.Release({0, 1}); });
    auto manager = std::make_shared<storage::DiskFileManagerImpl>(Context());
    BsonInvertedIndex loaded(manager);
    folly::CancellationSource cancel;
    OpContext op;
    op.cancellation_token = cancel.getToken();
    auto task = std::async(std::launch::async, [&] {
        loaded.LoadIndex(files_, kPriority, true, &op);
    });
    EXPECT_EQ(task.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    cancel.requestCancellation();
    EXPECT_THROW(task.get(), SegcoreError);
    EXPECT_EQ(source_->reads.load(), 0);
    EXPECT_FALSE(std::filesystem::exists(manager->GetLocalJsonStatsPrefix()));
}

TEST_F(BsonInvertedIndexAsyncLoadTest, ReadAndTantivyFailuresCleanStaging) {
    for (const auto expected :
         {StorageError, DataFormatBroken, UnexpectedError}) {
        auto fs = NativeSource();
        if (expected == StorageError) {
            fs->files.begin()->second->SetNextCompletion(
                arrow::Status::IOError("injected read failure"));
        } else {
            // Distinguish a broken envelope from a valid envelope containing
            // invalid Tantivy metadata, which fails after files are staged.
            std::vector<uint8_t> bytes(100, 'x');
            if (expected == UnexpectedError) {
                storage::IndexData codec(bytes.data(), bytes.size());
                codec.SetFieldDataMeta(Context().fieldDataMeta);
                codec.set_index_meta(Context().indexMeta);
                bytes = codec.Serialize(storage::StorageType::Remote);
            }
            for (auto& [path, file] : fs->files) {
                if (path.ends_with("meta.json_0")) {
                    file =
                        std::make_shared<test::ControlledDirectReadFile>(bytes);
                }
            }
        }
        auto context = Context();
        context.fs = fs;
        auto manager = std::make_shared<storage::DiskFileManagerImpl>(context);
        BsonInvertedIndex loaded(manager);
        try {
            loaded.LoadIndex(files_, kPriority, true, nullptr);
            FAIL() << "injected load failure must propagate";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), expected);
        }
        EXPECT_FALSE(
            std::filesystem::exists(manager->GetLocalJsonStatsPrefix()));
        EXPECT_THROW(
            loaded.TermQuery("shared",
                             [](const uint32_t*, const uint32_t*, int64_t) {}),
            SegcoreError);
    }
}

TEST_F(BsonInvertedIndexAsyncLoadTest,
       DestinationFailureKeepsOtherLoadDirectory) {
    auto manager = std::make_shared<storage::DiskFileManagerImpl>(Context());
    auto sibling = std::make_shared<storage::DiskFileManagerImpl>(Context());
    BsonInvertedIndex other(sibling);
    other.LoadIndex(files_, kPriority, true, nullptr);
    BsonInvertedIndex loaded(manager);
    const auto directory = manager->GetLocalJsonStatsPrefix();
    const auto blocked = manager->GetLocalJsonStatsSharedIndexPrefix();
    std::filesystem::create_directories(directory);
    // A regular file occupies the directory needed for staging.
    std::ofstream(std::filesystem::path(blocked).parent_path()) << "blocked";
    EXPECT_THROW(loaded.LoadIndex(files_, kPriority, true, nullptr),
                 boost::filesystem::filesystem_error);
    EXPECT_FALSE(std::filesystem::exists(directory));
    CheckQuery(other);
    EXPECT_TRUE(
        std::filesystem::exists(sibling->GetLocalJsonStatsSharedIndexPrefix()));
}

TEST_F(BsonInvertedIndexAsyncLoadTest,
       ResourceEstimateSurvivesConfigurationChanges) {
    for (const bool mmap : {false, true}) {
        BsonInvertedIndexTranslator translator(LoadInfo(mmap), Context());
        const auto estimate = translator.estimated_byte_size_of_cell(0);
        EXPECT_GE(estimate.second.memory_bytes,
                  storage::FileWriter::MAX_BUFFER_SIZE);
        EXPECT_GT(
            mmap ? estimate.first.file_bytes : estimate.first.memory_bytes, 0);
        storage::SetAsyncLoadThreadPoolSize(4);
        storage::LoadAdmissionController::GetInstance().SetCapacitySlots(8);
        test::ScopedLoadTransientBudget unlimited(0);
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(false);
        BsonInvertedIndexTranslator disabled(LoadInfo(mmap), Context());
        EXPECT_EQ(estimate, disabled.estimated_byte_size_of_cell(0));
        EXPECT_EQ(estimate, translator.estimated_byte_size_of_cell(0));
        storage::SetAsyncLoadThreadPoolSize(1);
        storage::LoadAdmissionController::GetInstance().SetCapacitySlots(1);
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(true);
    }
}

}  // namespace
}  // namespace milvus::index
