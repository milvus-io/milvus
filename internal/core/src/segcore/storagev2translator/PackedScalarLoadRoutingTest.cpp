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

#include <gtest/gtest.h>

#include <arrow/filesystem/localfs.h>
#include <any>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/OpContext.h"
#include "folly/ScopeGuard.h"
#include "folly/coro/BlockingWait.h"
#include "folly/system/ThreadName.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/PackedIndexLoad.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LocalFileIOPool.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Util.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::segcore::storagev2translator {
namespace {

class RecordingOpenFileSystem final : public arrow::fs::SubTreeFileSystem {
 public:
    explicit RecordingOpenFileSystem(std::shared_ptr<arrow::fs::FileSystem> fs)
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
        if (cancel_on_open) {
            cancel_on_open->requestCancellation();
            return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
                MakeFinished(arrow::Status::IOError("open failed after cancel"));
        }
        // Avoid calling the overridden synchronous overload while counting.
        return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
            MakeFinished(SubTreeFileSystem::OpenInputFile(path));
    }

    int sync_opens{0};
    int async_opens{0};
    folly::CancellationSource* cancel_on_open{nullptr};
};

class PackedScalarLoadRoutingTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        root_ = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "scalar-load-route-XXXXXX",
            "scalar load routing test");
        storage::StorageConfig config;
        config.storage_type = "local";
        config.root_path = root_->Path();
        storage::FieldDataMeta field{1, 2, 3, 101};
        field.field_schema.set_data_type(proto::schema::DataType::Int32);
        context_ = storage::FileManagerContext(field,
                                               storage::IndexMeta{3, 101, 1, 1},
                                               storage::CreateChunkManager(config),
                                               storage::InitArrowFileSystem(config));
        previous_mode_ = StorageV2AsyncLoadEnabled();
    }

    void
    TearDown() override {
        SetStorageV2AsyncLoadEnabled(previous_mode_);
        storage::LocalFileIOPool::GetInstance().Configure(0);
    }

    std::string
    WritePayload() const {
        storage::MemFileManagerImpl manager(context_);
        auto writer = manager.CreateIndexEntryWriterUnified("route.v3");
        constexpr int32_t payload = 42;
        writer->WriteEntry("payload", &payload, sizeof(payload));
        writer->Finish();
        return manager.GetRemoteIndexObjectPrefix() + "/route.v3";
    }

    std::string
    WriteSorted() const {
        const std::vector<int32_t> values{30, 10, 20, 10};
        const index::ScalarBuildBatch<int32_t> batch{values, {}};
        auto builder = index::BuilderRegistry<index::ScalarBuildInput<int32_t>>::
            Instance().Create(index::families::kSort, Params());
        auto artifact = std::move(*builder).Build({std::span(&batch, 1)});
        storage::MemFileManagerImpl manager(context_);
        const auto name = index::PackedScalarIndexFileName(index::ScalarIndexType::STLSORT);
        auto writer = manager.CreateIndexEntryWriterUnified(name);
        artifact->Serialize(*writer);
        writer->Finish();
        return manager.GetRemoteIndexObjectPrefix() + "/" + name;
    }

    static Config
    Params() {
        return {{"field_type", DataType::INT32},
                {"value_type", DataType::INT32},
                {"nested", false},
                {"nullable", false}};
    }

    index::IIndexReaderBasePtr
    LoadSorted(const std::string& path, OpContext* operation = nullptr) const {
        storage::LoadOptions options;
        options.params = Params();
        options.enable_mmap = true;
        options.mmap_dir_path = root_->Path() + "/loaded";
        options.op_ctx = operation;
        return index::LoaderRegistry::Instance().Lookup(index::families::kSort).Load(
            {index::IndexFiles{context_, {path}, index::PackedIndexStorageConfig{}}, options});
    }

    std::shared_ptr<storage::LocalDirectory> root_;
    storage::FileManagerContext context_;
    bool previous_mode_{};
};

TEST_F(PackedScalarLoadRoutingTest, CancellationWinsOverOpenFailure) {
    const auto path = WriteSorted();
    auto fs = std::make_shared<RecordingOpenFileSystem>(context_.fs);
    context_.fs = fs;
    context_.use_async_load = true;
    folly::CancellationSource cancelled;
    fs->cancel_on_open = &cancelled;
    OpContext operation(cancelled.getToken());
    try {
        static_cast<void>(LoadSorted(path, &operation));
        FAIL() << "cancelled open must not return a reader";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FollyCancel);
    }
    EXPECT_EQ(fs->sync_opens, 0);
    EXPECT_EQ(fs->async_opens, 1);
}

TEST_F(PackedScalarLoadRoutingTest, GlobalSwitchAndPinnedContextSelectCompleteLoadPath) {
    const auto path = WriteSorted();
    auto fs = std::make_shared<RecordingOpenFileSystem>(context_.fs);
    context_.fs = fs;
    for (const bool global_async : {false, true}) {
        SetStorageV2AsyncLoadEnabled(global_async);
        for (const std::optional<bool> pinned :
             {std::optional<bool>{}, std::optional<bool>{false}, std::optional<bool>{true}}) {
            context_.use_async_load = pinned;
            for (const int priority : {0, 1}) {
                SCOPED_TRACE(::testing::Message() << global_async << "/" << pinned.value_or(global_async) << "/" << priority);
                fs->sync_opens = fs->async_opens = 0;
                OpContext operation;
                operation.runtime_load_priority = priority;
                auto reader = LoadSorted(path, &operation);
                ASSERT_NE(reader, nullptr);
                EXPECT_EQ(reader->Count(), 4);
                const bool async = pinned.value_or(global_async);
                EXPECT_EQ(fs->sync_opens, async ? 0 : 1);
                EXPECT_EQ(fs->async_opens, async ? 1 : 0);
                const auto* predicate = dynamic_cast<const index::IScalarPredicateReader<int32_t>*>(reader.get());
                ASSERT_NE(predicate, nullptr);
                const int32_t key = 10;
                const auto hits = predicate->In(1, &key);
                ASSERT_EQ(hits.size(), 4);
                EXPECT_EQ(hits.count(), 2);
                EXPECT_TRUE(hits[1]);
                EXPECT_TRUE(hits[3]);
            }
        }
    }
}

struct LoadObservation {
    bool file_target{false};
    bool fail_read{false};
    bool fail_finish{false};
    folly::CancellationSource* cancel_on_finish{nullptr};
    std::string path;
    std::string planned_thread;
    std::string finish_thread;
    std::string cleanup_thread;
    int finish_calls{0};
    int32_t payload{0};
    std::shared_ptr<storage::IndexFileTarget> file;
};

struct LoadContext {
    explicit LoadContext(std::shared_ptr<LoadObservation> observation)
        : observation(std::move(observation)) {
    }
    ~LoadContext() {
        observation->cleanup_thread = folly::getCurrentThreadName().value_or("");
    }
    std::shared_ptr<LoadObservation> observation;
};

class PayloadReader final : public index::IIndexReaderBase {
 public:
    index::ReaderCaps Caps() const override { return {}; }
    index::Domain CoordDomain() const override { return index::Domain::Row; }
    int64_t Count() const override { return 1; }
    DataType ValueType() const override { return DataType::INT32; }
    int64_t MemoryUsage() const override { return sizeof(int32_t); }
    cachinglayer::ResourceUsage CellByteSize() const override { return {sizeof(int32_t), 0}; }
};

folly::coro::Task<index::IIndexReaderBasePtr>
FinishPayload(index::IndexLoadPlan& plan,
              const storage::LoadOptions&,
              bool) {
    auto state = std::any_cast<std::shared_ptr<LoadContext>>(plan.load_context)->observation;
    ++state->finish_calls;
    state->finish_thread = folly::getCurrentThreadName().value_or("");
    if (state->fail_finish) {
        ThrowInfo(FileWriteFailed, "injected finish failure");
    }
    const auto& target = plan.At("payload").target;
    if (const auto* memory = std::get_if<storage::MemoryEntryTarget>(&target)) {
        std::memcpy(&state->payload, memory->data, sizeof(state->payload));
    } else {
        const auto& file = std::get<storage::FileEntryTarget>(target);
        EXPECT_THROW(file.staging->WriteAt(0, nullptr, 0), SegcoreError);
        std::ifstream input(file.staging->path, std::ios::binary);
        input.read(reinterpret_cast<char*>(&state->payload), sizeof(state->payload));
        EXPECT_TRUE(input.good());
    }
    if (state->cancel_on_finish) {
        state->cancel_on_finish->requestCancellation();
    }
    co_return std::make_unique<PayloadReader>();
}

TEST_F(PackedScalarLoadRoutingTest, FinalizationCancellationAndFailuresNeverCommitTargets) {
    const auto path = WritePayload();
    context_.use_async_load = true;
    enum class Outcome { Success, ReadFailure, FinishFailure, CancelDuringFinish };
    for (const int workers : {0, 1}) {
        storage::LocalFileIOPool::GetInstance().Configure(workers);
        for (const int priority : {0, 1}) {
            for (const bool file : {false, true}) {
                for (const auto outcome : {Outcome::Success, Outcome::ReadFailure,
                                           Outcome::FinishFailure, Outcome::CancelDuringFinish}) {
                    SCOPED_TRACE(::testing::Message() << workers << "/" << priority << "/" << file << "/" << static_cast<int>(outcome));
                    auto state = std::make_shared<LoadObservation>();
                    state->file_target = file;
                    state->fail_read = outcome == Outcome::ReadFailure;
                    state->fail_finish = outcome == Outcome::FinishFailure;
                    state->path = root_->Path() + "/target/payload";
                    folly::CancellationSource cancelled;
                    if (outcome == Outcome::CancelDuringFinish) {
                        state->cancel_on_finish = &cancelled;
                    }
                    OpContext operation(cancelled.getToken());
                    operation.runtime_load_priority = priority;
                    index::PackedIndexSource source{
                        std::shared_ptr<storage::AsyncIndexEntryReader>(
                            index::InspectPackedIndexFile({path}, context_))};
                    auto plan = [state](const storage::IndexEntryDirectory& directory,
                                         const nlohmann::json&,
                                         const storage::LoadOptions&) {
                        state->planned_thread = folly::getCurrentThreadName().value_or("");
                        const auto size = directory.At("payload").plaintext_size;
                        index::IndexLoadPlan result;
                        result.load_context = std::make_shared<LoadContext>(state);
                        storage::EntryTarget target;
                        if (state->file_target) {
                            state->file = std::make_shared<storage::IndexFileTarget>(state->path, size, true);
                            target = storage::FileEntryTarget{state->file, 0, size};
                        } else {
                            auto bytes = std::make_shared<std::vector<uint8_t>>(size);
                            target = storage::MemoryEntryTarget{bytes, bytes->data(), size};
                        }
                        result.entries.push_back({state->fail_read ? "missing" : "payload", std::move(target)});
                        return result;
                    };
                    auto load = [&] {
                        auto task = index::RunPackedIndexLoad(source, {}, plan, &FinishPayload, &operation);
                        return folly::coro::blockingWait(folly::coro::co_withExecutor(
                            storage::ResolveAsyncLoadExecutor({}, priority == 0 ? proto::common::LoadPriority::HIGH : proto::common::LoadPriority::LOW),
                            std::move(task)));
                    };
                    if (outcome == Outcome::Success) {
                        auto reader = load();
                        ASSERT_NE(reader, nullptr);
                        EXPECT_EQ(state->payload, 42);
                    } else {
                        try {
                            static_cast<void>(load());
                            FAIL() << "failure must prevent reader publication";
                        } catch (const SegcoreError& error) {
                            if (outcome == Outcome::FinishFailure) {
                                EXPECT_EQ(error.get_error_code(), FileWriteFailed);
                            } else if (outcome == Outcome::CancelDuringFinish) {
                                EXPECT_EQ(error.get_error_code(), FollyCancel);
                            } else {
                                EXPECT_NE(std::string(error.what()).find("missing"), std::string::npos);
                            }
                        }
                    }
                    EXPECT_EQ(state->finish_calls, outcome == Outcome::ReadFailure ? 0 : 1);
                    const auto local_prefix = workers > 0 ? "MILVUS_LF_IO_" : "MILVUS_ASYNC";
                    EXPECT_TRUE(state->planned_thread.starts_with(local_prefix));
                    EXPECT_TRUE(state->cleanup_thread.starts_with(local_prefix));
                    if (outcome != Outcome::ReadFailure) {
                        EXPECT_TRUE(state->finish_thread.starts_with("MILVUS_ASYNC"));
                    }
                    if (file) {
                        ASSERT_NE(state->file, nullptr);
                        EXPECT_EQ(state->file->Committed(), outcome == Outcome::Success);
                        EXPECT_EQ(std::filesystem::exists(state->path), outcome == Outcome::Success);
                        if (outcome == Outcome::Success) {
                            ASSERT_TRUE(std::filesystem::remove(state->path));
                        }
                    }
                }
            }
        }
    }
}

TEST_F(PackedScalarLoadRoutingTest, SortedMmapLoadsWithSingleLocalFileWorker) {
    SetStorageV2AsyncLoadEnabled(true);
    storage::LocalFileIOPool::GetInstance().Configure(1);
    const auto path = WriteSorted();
    OpContext operation;
    operation.runtime_load_priority = 1;
    auto reader = LoadSorted(path, &operation);
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 4);
    const auto* predicate = dynamic_cast<const index::IScalarPredicateReader<int32_t>*>(reader.get());
    ASSERT_NE(predicate, nullptr);
    const int32_t key = 10;
    const auto hits = predicate->In(1, &key);
    ASSERT_EQ(hits.size(), 4);
    EXPECT_EQ(hits.count(), 2);
    EXPECT_TRUE(hits[1]);
    EXPECT_TRUE(hits[3]);
}

}  // namespace
}  // namespace milvus::segcore::storagev2translator
