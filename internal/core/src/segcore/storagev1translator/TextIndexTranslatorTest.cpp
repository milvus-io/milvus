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

#include <arrow/filesystem/localfs.h>
#include <chrono>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "folly/ScopeGuard.h"
#include "folly/system/ThreadName.h"
#include "common/OpContext.h"
#include "index/Families.h"
#include "index/LoadResource.h"
#include "index/contracts/Registry.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "segcore/storagev1translator/TextMatchIndexTranslator.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/test_utils/TextPublicationTestUtils.h"
#include "test_utils/AsyncLoadTestUtils.h"

namespace milvus::segcore::storagev1translator {
namespace {

class TextLoadFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    explicit TextLoadFileSystem(std::shared_ptr<arrow::fs::FileSystem> fs)
        : SubTreeFileSystem("", std::move(fs)) {
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        ++sync_opens;
        return SubTreeFileSystem::OpenInputFile(path);
    }

    arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFileAsync(const std::string& path) override {
        ++async_opens;
        EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
            "MILVUS_ASYNC"));
        if (auto found = controlled.find(path); found != controlled.end()) {
            return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
                MakeFinished(
                    std::static_pointer_cast<arrow::io::RandomAccessFile>(
                        found->second));
        }
        return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
            MakeFinished(SubTreeFileSystem::OpenInputFile(path));
    }

    void
    ResetCounts() {
        sync_opens = async_opens = 0;
    }
    int sync_opens{};
    int async_opens{};
    std::map<std::string, std::shared_ptr<test::ControlledDirectReadFile>>
        controlled;
};

void
CheckTextReader(const index::IIndexReaderBase& reader) {
    ASSERT_EQ(reader.Count(), 3);
    const auto* text = dynamic_cast<const index::ITextMatchReader*>(&reader);
    const auto* nulls = dynamic_cast<const index::INullReader*>(&reader);
    ASSERT_NE(text, nullptr);
    ASSERT_NE(nulls, nullptr);
    const auto alpha = text->MatchQuery("alpha", 1);
    ASSERT_EQ(alpha.size(), 3);
    EXPECT_TRUE(alpha[0]);
    EXPECT_FALSE(alpha[1]);
    EXPECT_FALSE(alpha[2]);
    const auto beta = text->MatchQuery("beta", 1);
    EXPECT_EQ(beta.count(), 1);
    EXPECT_TRUE(beta[2]);
    const auto is_null = nulls->IsNull();
    EXPECT_EQ(is_null.count(), 1);
    EXPECT_TRUE(is_null[1]);
    EXPECT_EQ(reader.CellByteSize().memory_bytes, reader.MemoryUsage());
}

class TextIndexTranslatorTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        old_enabled_ = storagev2translator::StorageV2AsyncLoadEnabled();
        old_workers_ = storage::GetAsyncLoadThreadPoolSize();
        old_slots_ =
            storage::LoadAdmissionController::GetInstance().CapacitySlots();
        storage::SetAsyncLoadThreadPoolSize(1);
        storage::LoadAdmissionController::GetInstance().SetCapacitySlots(1);
        storagev2translator::SetStorageV2AsyncLoadEnabled(true);
    }
    void
    TearDown() override {
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const bool available =
            admission.TryAcquire({0, 1}, storage::LoadAdmissionPriority::High);
        EXPECT_TRUE(available);
        if (available)
            admission.Release({0, 1});
        admission.SetCapacitySlots(old_slots_);
        storage::SetAsyncLoadThreadPoolSize(old_workers_);
        storagev2translator::SetStorageV2AsyncLoadEnabled(old_enabled_);
    }
    test::ScopedLoadTransientBudget budget_{64 * 1024 * 1024};
    bool old_enabled_{};
    int old_workers_{};
    size_t old_slots_{};
};

TEST_F(TextIndexTranslatorTest, LegacyResourceEstimateIncludesNullableBitmap) {
    storage::text_test::TextPublicationFixture fixture;
    const auto stats = fixture.Publish(false);
    ASSERT_GT(stats.MemSize(), 0);
    const auto bitmap_bytes =
        static_cast<int64_t>(TargetBitmap(3).size_in_bytes());
    for (const bool mmap : {false, true}) {
        auto config = fixture.LoadConfig(stats, mmap);
        TextMatchIndexTranslator translator(
            {mmap, 3, 101, "{}", stats.MemSize(), 3, "", ""},
            fixture.context,
            config);
        auto [loaded, overhead] = translator.estimated_byte_size_of_cell(0);
        EXPECT_EQ(loaded.memory_bytes,
                  (mmap ? bitmap_bytes : stats.MemSize() + bitmap_bytes) +
                      index::kScalarIndexFixedResidentBytes);
        EXPECT_EQ(loaded.file_bytes, mmap ? stats.MemSize() : 0);
        EXPECT_EQ(overhead.memory_bytes, mmap ? stats.MemSize() : 0);
        EXPECT_EQ(overhead.file_bytes,
                  mmap ? stats.MemSize() : 2 * stats.MemSize());
        // Zero index size adds no disk overhead, even with nullable rows.
        TextMatchIndexTranslator zero_size_translator(
            {mmap, 3, 101, "{}", 0, 3, "", ""}, fixture.context, config);
        const auto [zero_loaded, zero_overhead] =
            zero_size_translator.estimated_byte_size_of_cell(0);
        EXPECT_EQ(zero_loaded.memory_bytes,
                  bitmap_bytes + index::kScalarIndexFixedResidentBytes);
        EXPECT_EQ(zero_overhead.file_bytes, 0);
        auto cells = translator.get_cells(nullptr, {0});
        ASSERT_EQ(cells.size(), 1);
        CheckTextReader(*cells.front().second);
        // The fixed reservation covers the known reader and directory metadata;
        // the legacy admission formula remains an estimate, not a heap census.
        EXPECT_GE(cells.front().second->MemoryUsage(), bitmap_bytes);
        EXPECT_EQ(cells.front().second->CellByteSize().file_bytes > 0, mmap);
    }
}

TEST_F(TextIndexTranslatorTest,
       PackedReloadPinsPlanningModeAcrossRolloutChanges) {
    storage::text_test::TextPublicationFixture fixture;
    const auto stats = fixture.Publish(true);
    auto fs = std::make_shared<TextLoadFileSystem>(fixture.context.fs);
    fixture.context.fs = fs;
    for (const bool mmap : {false, true}) {
        auto config = fixture.LoadConfig(stats, mmap);
        for (const bool planned_async : {false, true}) {
            storagev2translator::SetStorageV2AsyncLoadEnabled(planned_async);
            TextMatchIndexTranslator translator(
                {mmap, 3, 101, "{}", stats.MemSize(), 3, "", ""},
                fixture.context,
                config);
            const auto estimate = translator.estimated_byte_size_of_cell(0);
            for (const bool current_async : {true, false}) {
                storagev2translator::SetStorageV2AsyncLoadEnabled(
                    current_async);
                fs->ResetCounts();
                auto cells = translator.get_cells(nullptr, {0});
                ASSERT_EQ(cells.size(), 1);
                CheckTextReader(*cells.front().second);
                EXPECT_EQ(fs->sync_opens, planned_async ? 0 : 1);
                EXPECT_EQ(fs->async_opens, planned_async ? 1 : 0);
                EXPECT_EQ(estimate, translator.estimated_byte_size_of_cell(0));
                EXPECT_GE(estimate.first.memory_bytes,
                          cells.front().second->CellByteSize().memory_bytes);
                EXPECT_GE(estimate.first.file_bytes,
                          cells.front().second->CellByteSize().file_bytes);
                EXPECT_EQ(cells.front().second->CellByteSize().file_bytes > 0,
                          mmap);

                // Direct registry callers can pin a mode too; an unpinned
                // context resolves the rollout setting for each complete load.
                for (const bool pinned : {false, true}) {
                    auto context = fixture.context;
                    context.use_async_load =
                        pinned ? std::optional<bool>(planned_async)
                               : std::nullopt;
                    storage::LoadOptions options;
                    options.enable_mmap = mmap;
                    options.params = config;
                    options.params["field_type"] = DataType::VARCHAR;
                    options.params["value_type"] = DataType::VARCHAR;
                    options.params["nested"] = false;
                    options.params["analyzer_params"] = "{}";
                    fs->ResetCounts();
                    auto reader =
                        index::LoaderRegistry::Instance()
                            .Lookup(index::families::kText)
                            .Load({index::IndexFiles{
                                       context,
                                       {stats.Files().front().file_name},
                                       index::PackedIndexStorageConfig{
                                           storage::ArtifactStorageNamespace::
                                               TextLog}},
                                   options});
                    CheckTextReader(*reader);
                    const bool expected_async =
                        pinned ? planned_async : current_async;
                    EXPECT_EQ(fs->sync_opens, expected_async ? 0 : 1);
                    EXPECT_EQ(fs->async_opens, expected_async ? 1 : 0);
                }
            }
        }
    }
    EXPECT_TRUE(std::filesystem::is_empty(fixture.root->Path() + "/loaded"));
}

TEST_F(TextIndexTranslatorTest,
       NativeReadSuspendsAndCancellationDrainsBeforeCleanup) {
    for (const bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        storage::text_test::TextPublicationFixture fixture;
        const auto stats = fixture.Publish(true);
        auto config = fixture.LoadConfig(stats, mmap);
        config[LOAD_PRIORITY] = proto::common::LoadPriority::LOW;
        auto fs = std::make_shared<TextLoadFileSystem>(fixture.context.fs);
        for (const auto& name :
             config.at(index::INDEX_FILES).get<std::vector<std::string>>()) {
            const auto path =
                config.at(STATS_BASE_PATH_KEY).get<std::string>() + "/" + name;
            auto input = fixture.context.fs->OpenInputFile(path).ValueOrDie();
            const auto size = input->GetSize().ValueOrDie();
            std::vector<uint8_t> bytes(size);
            ASSERT_EQ(input->ReadAt(0, size, bytes.data()).ValueOrDie(), size);
            fs->controlled.emplace(
                path,
                std::make_shared<test::ControlledDirectReadFile>(
                    std::move(bytes)));
        }
        fixture.context.fs = fs;
        TextMatchIndexTranslator translator(
            {mmap, 3, 101, "{}", stats.MemSize(), 3, "", ""},
            fixture.context,
            config);
        auto first = fs->controlled.begin()->second;
        first->ResetCounters();
        first->SetAutoComplete(false);
        folly::CancellationSource cancel;
        OpContext operation(cancel.getToken());
        auto pending = std::async(std::launch::async, [&] {
            return translator.get_cells(&operation, {0});
        });
        auto drain = folly::makeGuard([&] {
            cancel.requestCancellation();
            first->SetAutoComplete(true);
            for (size_t i = 0; i < first->DirectReadCalls().size(); ++i)
                first->Complete(i);
            pending.wait();
        });
        // Magic, footer and directory precede admission; the next read is the
        // admitted metadata payload. Keep it outstanding during cancellation.
        for (size_t i = 0; i < 3; ++i) {
            ASSERT_TRUE(first->WaitForCallCount(i + 1));
            first->Complete(i);
        }
        ASSERT_TRUE(first->WaitForCallCount(4));
        auto probe = std::make_shared<std::promise<void>>();
        auto ready = probe->get_future();
        storage::ResolveAsyncLoadExecutor({}, proto::common::LoadPriority::LOW)
            ->add([probe] { probe->set_value(); });
        EXPECT_EQ(ready.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const bool available =
            admission.TryAcquire({0, 1}, storage::LoadAdmissionPriority::High);
        EXPECT_FALSE(available);
        if (available)
            admission.Release({0, 1});
        cancel.requestCancellation();
        EXPECT_EQ(pending.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        first->SetAutoComplete(true);
        first->Complete(3);
        pending.wait();
        drain.dismiss();
        try {
            static_cast<void>(pending.get());
            FAIL() << "cancelled text load must not publish a reader";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), FollyCancel);
        }
        for (const auto& [path, file] : fs->controlled) {
            EXPECT_EQ(file->ReadAtCalls(), 0);
            EXPECT_EQ(file->AsyncReadCalls(), 0);
        }
        const auto loaded = fixture.root->Path() + "/loaded";
        EXPECT_TRUE(!std::filesystem::exists(loaded) ||
                    std::filesystem::is_empty(loaded));
    }
}

}  // namespace
}  // namespace milvus::segcore::storagev1translator
