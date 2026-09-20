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
#include <atomic>
#include <map>
#include <fstream>
#include <array>
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileWriter.h"
#include "storage/LocalFileIOPool.h"
#include "test_utils/TmpPath.h"
#include "common/Slice.h"
#include "arrow/filesystem/localfs.h"
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

class LegacyFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    LegacyFileSystem()
        : SubTreeFileSystem("",
                            std::make_shared<arrow::fs::LocalFileSystem>()) {
    }
    arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFileAsync(const std::string& path) override {
        return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
            MakeFinished(OpenInputFile(path));
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
    // Direct transport tests have no engine; commit only after the task succeeds.
    void
    Run(folly::coro::Task<std::vector<EntryLoadPlan>> task,
        proto::common::LoadPriority priority = kPriority) {
        auto entries = folly::coro::blockingWait(
            std::move(task).scheduleOn(ResolveAsyncLoadExecutor({}, priority)));
        for (const auto& entry : entries) {
            std::get<FileEntryTarget>(entry.target).staging->Commit();
        }
    }
    template <typename T>
    T
    Run(folly::coro::Task<T> task,
        proto::common::LoadPriority priority = kPriority) {
        return folly::coro::blockingWait(
            std::move(task).scheduleOn(ResolveAsyncLoadExecutor({}, priority)));
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
    LegacyIndexFile
    MakeFile(const std::string& name,
             const std::vector<uint8_t>& bytes,
             DataType type = DataType::NONE,
             std::shared_ptr<CPluginContext> encryption = {}) {
        auto input = Open(Encode(bytes, type, std::move(encryption)));
        auto info = Run(InspectLegacyIndexFileAsync(*input, kPriority));
        file_->ResetCounters();
        file_->SetAutoComplete(false);
        source_->files[name] = file_;
        return {name, info};
    }
    void
    CompleteReads() {
        for (const auto& [name, file] : source_->files) {
            file->SetAutoComplete(true);
        }
        for (const auto& [name, file] : source_->files) {
            for (size_t i = 0; i < file->DirectReadCalls().size(); ++i) {
                file->Complete(i);
            }
        }
    }
    LoadAdmissionController& budget_ = LoadAdmissionController::GetInstance();
    test::ScopedLoadTransientBudget bytes_guard_{1024 * 1024};
    size_t old_slots_{};
    int old_threads_{};
    std::shared_ptr<test::ControlledDirectReadFile> file_;
    std::shared_ptr<LegacyFileSystem> source_ =
        std::make_shared<LegacyFileSystem>();
    milvus_storage::ArrowFileSystemPtr fs_ = source_;
    ChunkManagerPtr no_chunk_manager_;
};

TEST_F(LegacyIndexLoaderTest, ConcurrentEncryptedUnitsReuseDecoder) {
    auto& plugins = PluginLoader::GetInstance();
    plugins.registerPluginForTest(
        std::make_shared<test::CollectionBoundPlannerCipherPlugin>(1));
    auto unregister = folly::makeGuard(
        [&] { plugins.unregisterPluginForTest("CipherPlugin"); });
    auto encryption = std::make_shared<CPluginContext>();
    encryption->collection_id = 1;
    encryption->ez_id = 2;
    budget_.SetCapacitySlots(3);
    SetAsyncLoadThreadPoolSize(3);
    const std::vector<LegacyIndexFile> files{
        MakeFile("raw", {1, 2}, DataType::NONE, encryption),
        MakeFile("parquet", {3, 4}, DataType::INT8, encryption),
        MakeFile("string", {5, 6}, DataType::STRING, encryption)};
    std::vector<uint8_t> output(6);
    const EntryTarget consume =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
    auto load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFilesAsync(
            files, no_chunk_manager_, fs_, consume, kPriority));
    });
    auto drain = folly::makeGuard([&] {
        CompleteReads();
        load.wait();
    });
    for (const auto& file : files) {
        ASSERT_TRUE(source_->files.at(file.path)->WaitForCallCount(1));
    }
    for (auto it = files.rbegin(); it != files.rend(); ++it) {
        source_->files.at(it->path)->Complete(0);
    }
    EXPECT_NO_THROW(load.get());
    drain.dismiss();
    EXPECT_EQ(output, (std::vector<uint8_t>{1, 2, 3, 4, 5, 6}));
}

TEST_F(LegacyIndexLoaderTest, FileTargetsRetainOwnershipUntilFinalization) {
    LocalFileIOPool::GetInstance().Configure(1);
    auto stop =
        folly::makeGuard([] { LocalFileIOPool::GetInstance().Configure(0); });
    test::TmpPath directory;
    const auto file = MakeFile("owned_0", {1, 2, 3, 4});
    CompleteReads();
    FileManagerContext context{
        FieldDataMeta{1, 2, 3, 101},
        IndexMeta{3, 101, 93002, 2},
        std::make_shared<LocalChunkManager>(directory.get().string()),
        fs_};
    DiskFileManagerImpl manager(context);
    for (const bool success : {false, true}) {
        auto entries = folly::coro::blockingWait(
            manager
                .CacheIndexToDiskAsync(
                    {file.path}, directory.get().string(), kPriority)
                .scheduleOn(ResolveAsyncLoadExecutor({}, kPriority)));
        ASSERT_EQ(entries.size(), 1);
        const auto target =
            std::get<FileEntryTarget>(entries.front().target).staging;
        EXPECT_TRUE(std::filesystem::exists(target->path));
        EXPECT_FALSE(target->Committed());
        Run(RunLocalFileIOAsync(
            [&] {
                if (success)
                    target->Commit();
                CleanupUncommittedFileTargets(CollectIndexFileTargets(entries));
            },
            kPriority));
        EXPECT_EQ(std::filesystem::exists(target->path), success);
        EXPECT_EQ(target->Committed(), success);
    }
}

TEST_F(LegacyIndexLoaderTest, ManagersReuseImmutableEnvelopeInspection) {
    LocalFileIOPool::GetInstance().Configure(1);
    auto stop =
        folly::makeGuard([] { LocalFileIOPool::GetInstance().Configure(0); });
    test::TmpPath directory;
    const std::vector<uint8_t> payload{1, 2, 3, 4};
    auto file = MakeFile("entry_0", payload);
    CompleteReads();
    FileManagerContext context{
        FieldDataMeta{1, 2, 3, 101},
        IndexMeta{3, 101, 93002, 2},
        std::make_shared<LocalChunkManager>(directory.get().string()),
        fs_};
    const std::vector<std::string> paths{file.path};
    context.legacy_index_files = Run(InspectLegacyIndexFilesAsync(
        paths, context.chunkManagerPtr, context.fs, kPriority));
    folly::CancellationSource cancelled;
    cancelled.requestCancellation();
    file_->ResetCounters();
    EXPECT_THROW(Run(InspectLegacyIndexFileAsync(file.path,
                                                 context.chunkManagerPtr,
                                                 context.fs,
                                                 context.legacy_index_files,
                                                 kPriority,
                                                 cancelled.getToken())),
                 SegcoreError);
    EXPECT_TRUE(file_->DirectReadCalls().empty());
    for (const bool disk : {false, true}) {
        file_->ResetCounters();
        if (disk) {
            DiskFileManagerImpl manager(context);
            Run(manager.CacheIndexToDiskAsync(
                paths, directory.get().string(), kPriority));
            std::ifstream input(directory.get() / "entry", std::ios::binary);
            const std::vector<uint8_t> actual(
                (std::istreambuf_iterator<char>(input)), {});
            EXPECT_EQ(actual, payload);
        } else {
            MemFileManagerImpl manager(context);
            auto binary =
                Run(manager.LoadIndexBinarySetAsync(paths, kPriority));
            const auto entry = binary.GetByName("entry_0");
            ASSERT_NE(entry, nullptr);
            EXPECT_EQ(std::vector<uint8_t>(entry->data.get(),
                                           entry->data.get() + entry->size),
                      payload);
        }
        const auto reads = file_->DirectReadCalls();
        ASSERT_EQ(reads.size(), 1);
        EXPECT_EQ(reads.front().position, file.info.payload_offset);
        EXPECT_EQ(reads.front().nbytes, payload.size());
    }
}

TEST_F(LegacyIndexLoaderTest, ManagersPlaceReversedSlicesAndDrainLocalWrites) {
    const auto old_mode = FileWriter::GetMode();
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    auto restore_mode =
        folly::makeGuard([&] { FileWriter::SetMode(old_mode); });
    constexpr auto priority = proto::common::LoadPriority::LOW;
    budget_.SetCapacitySlots(3);
    LocalFileIOPool::GetInstance().Configure(1);
    auto stop_pool =
        folly::makeGuard([&] { LocalFileIOPool::GetInstance().Configure(0); });
    for (const bool disk : {false, true}) {
        test::TmpPath directory;
        auto chunk_manager =
            std::make_shared<LocalChunkManager>(directory.get().string());
        const FileManagerContext context{FieldDataMeta{1, 2, 3, 101},
                                         IndexMeta{3, 101, 93001, 2},
                                         chunk_manager,
                                         fs_};
        std::vector<uint8_t> expected;
        for (size_t i = 0; i < 3; ++i) {
            const std::vector<uint8_t> bytes(i == 2 ? 19 : 64 * 1024,
                                             uint8_t(i + 1));
            MakeFile("entry_" + std::to_string(i), bytes);
            expected.insert(expected.end(), bytes.begin(), bytes.end());
        }
        std::vector<std::string> remote_files{"entry_2", "entry_0", "entry_1"};
        if (!disk) {
            const auto metadata =
                Config{{META,
                        Config::array({{{NAME, "entry"},
                                        {SLICE_NUM, 3},
                                        {TOTAL_LEN, expected.size()}}})}}
                    .dump();
            MakeFile(INDEX_FILE_SLICE_META,
                     std::vector<uint8_t>(metadata.begin(), metadata.end()));
            remote_files.push_back(INDEX_FILE_SLICE_META);
        }
        CompleteReads();
        std::map<std::string, size_t> opens;
        std::array<std::promise<void>, 3> payload_opened;
        std::array<std::future<void>, 3> opened{payload_opened[0].get_future(),
                                                payload_opened[1].get_future(),
                                                payload_opened[2].get_future()};
        source_->on_open = [&](const std::string& path) {
            if (++opens[path] == 2 && path != INDEX_FILE_SLICE_META) {
                auto& file = source_->files.at(path);
                file->ResetCounters();
                file->SetAutoComplete(false);
                payload_opened.at(path.back() - '0').set_value();
            }
        };
        const auto local = (directory.get() / "staging").string();
        std::vector<uint8_t> output;
        folly::CancellationSource cancel;
        auto load = std::async(std::launch::async, [&] {
            if (disk) {
                DiskFileManagerImpl manager(context);
                Run(manager.CacheIndexToDiskAsync(
                    remote_files, local, priority, cancel.getToken()));
                std::ifstream input(std::filesystem::path(local) / "entry",
                                    std::ios::binary);
                output.assign(std::istreambuf_iterator<char>(input), {});
            } else {
                MemFileManagerImpl manager(context);
                auto binary = Run(manager.LoadIndexBinarySetAsync(
                    remote_files, priority, cancel.getToken()));
                const auto entry = binary.GetByName("entry");
                output.assign(entry->data.get(),
                              entry->data.get() + entry->size);
            }
        });
        auto drain = folly::makeGuard([&] {
            cancel.requestCancellation();
            CompleteReads();
            load.wait();
            source_->on_open = {};
        });
        for (size_t i = 0; i < 3; ++i) {
            ASSERT_EQ(opened[i].wait_for(std::chrono::seconds(5)),
                      std::future_status::ready);
            ASSERT_TRUE(source_->files.at("entry_" + std::to_string(i))
                            ->WaitForCallCount(1));
        }
        source_->files.at("entry_2")->Complete(0);
        source_->files.at("entry_1")->Complete(0);
        EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        source_->files.at("entry_0")->Complete(0);
        EXPECT_NO_THROW(load.get());
        drain.dismiss();
        source_->on_open = {};
        EXPECT_EQ(output, expected);
    }
}

TEST_F(LegacyIndexLoaderTest, ReverseCompletionWithMemoryTargets) {
    budget_.SetCapacitySlots(3);
    const std::vector<std::vector<uint8_t>> payloads{
        std::vector<uint8_t>(17, 1),
        std::vector<uint8_t>(19, 2),
        std::vector<uint8_t>(23, 3)};
    const std::vector<LegacyIndexFile> files{
        MakeFile("0", payloads[0], DataType::INT8),
        MakeFile("1", payloads[1]),
        MakeFile("2", payloads[2], DataType::STRING)};
    std::vector<uint8_t> expected;
    for (const auto& payload : payloads) {
        expected.insert(expected.end(), payload.begin(), payload.end());
    }
    std::vector<uint8_t> output(expected.size());
    const EntryTarget consume =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
    auto load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFilesAsync(
            files, no_chunk_manager_, fs_, consume, kPriority));
    });
    auto drain = folly::makeGuard([&] {
        CompleteReads();
        load.wait();
    });
    for (const auto& file : files) {
        ASSERT_TRUE(source_->files.at(file.path)->WaitForCallCount(1));
    }
    source_->files.at("2")->Complete(0);
    source_->files.at("1")->Complete(0);
    source_->files.at("0")->Complete(0);
    EXPECT_NO_THROW(load.get());
    drain.dismiss();
    EXPECT_EQ(output, expected);
}

TEST_F(LegacyIndexLoaderTest,
       RawRangesCrossFileBoundariesWithoutStrandingHead) {
    budget_.SetCapacityBytes(DEFAULT_FIELD_MAX_MEMORY_LIMIT);
    budget_.SetCapacitySlots(4);
    const auto window = DefaultStreamSliceSize();
    const std::vector<uint8_t> first(2 * window + 7, 42);
    const std::vector<LegacyIndexFile> files{
        MakeFile("head", first), MakeFile("tail", {1, 2, 3}, DataType::INT8)};
    std::vector<uint8_t> output(first.size() + 3);
    const EntryTarget consume =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
    auto load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFilesAsync(
            files, no_chunk_manager_, fs_, consume, kPriority));
    });
    auto drain = folly::makeGuard([&] {
        CompleteReads();
        load.wait();
    });
    auto head = source_->files.at("head");
    auto tail = source_->files.at("tail");
    ASSERT_TRUE(head->WaitForCallCount(3));
    ASSERT_TRUE(tail->WaitForCallCount(1));
    tail->Complete(0);
    head->Complete(2);
    head->Complete(1);
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    head->Complete(0);
    EXPECT_NO_THROW(load.get());
    drain.dismiss();
    auto expected = first;
    expected.insert(expected.end(), {1, 2, 3});
    EXPECT_EQ(output, expected);
}

TEST_F(LegacyIndexLoaderTest, ScratchEstimateIncludesTailAndEncodedUnits) {
    const auto raw = MakeFile("raw", {1});
    EXPECT_EQ(raw.info.TotalTransientBytes(false), raw.info.file_bytes);
    EXPECT_GE(raw.info.TotalTransientBytes(), size_t{4097});
    EXPECT_GE(raw.info.TotalTransientBytes(), raw.info.max_transient_bytes);
    const auto encoded = MakeFile("encoded", {1}, DataType::INT8);
    EXPECT_EQ(encoded.info.TotalTransientBytes(),
              encoded.info.max_transient_bytes);
    auto huge = raw.info;
    huge.payload_bytes = std::numeric_limits<size_t>::max();
    EXPECT_EQ(huge.TotalTransientBytes(), std::numeric_limits<size_t>::max());
}

TEST_F(LegacyIndexLoaderTest, LegacyScratchRetainsBatchAllowance) {
    constexpr size_t mib = 1 << 20;
    const auto old_slice_size = FILE_SLICE_SIZE.exchange(16 * mib);
    const auto restore =
        folly::makeGuard([&] { FILE_SLICE_SIZE = old_slice_size; });
    EXPECT_EQ(LegacyIndexLoadTransientBytes(12 * 16 * mib, 16 * mib, false),
              128 * mib);
    EXPECT_EQ(LegacyIndexLoadTransientBytes(12 * 32 * mib, 32 * mib, true),
              384 * mib);
    EXPECT_EQ(LegacyIndexLoadTransientBytes(1024, 1024, false), 128 * mib);
    // Encoded objects can need more than the nominal download allowance.
    EXPECT_EQ(LegacyIndexLoadTransientBytes(12 * 64 * mib, 64 * mib, false),
              8 * 64 * mib);
    FILE_SLICE_SIZE = 32 * mib;
    EXPECT_EQ(LegacyIndexLoadTransientBytes(12 * 64 * mib, 64 * mib, false),
              4 * 64 * mib);
    EXPECT_EQ(LegacyIndexLoadTransientBytes(2 * 160 * mib, 160 * mib, false),
              2 * 160 * mib);
    const auto max = std::numeric_limits<size_t>::max();
    EXPECT_EQ(LegacyIndexLoadTransientBytes(max, max, false), max);
}

TEST_F(LegacyIndexLoaderTest, UnlimitedAdmissionDispatchesAllSlices) {
    budget_.SetCapacityBytes(0);
    budget_.SetCapacitySlots(0);
    std::vector<LegacyIndexFile> files;
    for (size_t i = 0; i < 12; ++i) {
        files.push_back(MakeFile(std::to_string(i), {42}));
    }
    std::vector<uint8_t> output(files.size());
    const EntryTarget consume =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
    auto load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFilesAsync(
            files, no_chunk_manager_, fs_, consume, kPriority));
    });
    auto drain = folly::makeGuard([&] {
        CompleteReads();
        load.wait();
    });
    for (size_t i = 0; i < files.size(); ++i) {
        ASSERT_TRUE(source_->files.at(std::to_string(i))->WaitForCallCount(1));
    }
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    CompleteReads();
    EXPECT_NO_THROW(load.get());
    drain.dismiss();
    EXPECT_EQ(output, std::vector<uint8_t>(files.size(), 42));
}

TEST_F(LegacyIndexLoaderTest, GlobalByteLimitAndOversizedDecodeRunAlone) {
    budget_.SetCapacityBytes(size_t{256} << 20);
    budget_.SetCapacitySlots(0);
    for (const size_t charge : {size_t{64} << 20, size_t{257} << 20}) {
        std::vector<LegacyIndexFile> files;
        for (size_t i = 0; i < 5; ++i) {
            files.push_back(MakeFile(std::to_string(i), {42}, DataType::INT8));
            // A conservative inspected decode charge; actual tiny data keeps
            // this policy test independent of allocator/Parquet compression.
            files.back().info.max_transient_bytes = charge;
        }
        std::vector<uint8_t> output(files.size());
        const EntryTarget consume =
            MemoryEntryTarget{nullptr, output.data(), output.size()};
        auto load = std::async(std::launch::async, [&] {
            Run(StreamLegacyIndexFilesAsync(
                files, no_chunk_manager_, fs_, consume, kPriority));
        });
        auto drain = folly::makeGuard([&] {
            CompleteReads();
            load.wait();
        });
        ASSERT_TRUE(source_->files.at("0")->WaitForCallCount(1));
        const size_t active = charge > budget_.CapacityBytes() ? 1 : 4;
        for (size_t i = 1; i < active; ++i) {
            ASSERT_TRUE(
                source_->files.at(std::to_string(i))->WaitForCallCount(1));
        }
        EXPECT_FALSE(source_->files.at(std::to_string(active))
                         ->WaitForCallCount(1, std::chrono::milliseconds(30)));
        CompleteReads();
        EXPECT_NO_THROW(load.get());
        drain.dismiss();
    }
}

TEST_F(LegacyIndexLoaderTest, CancellationAndFailureDrainAllIssuedSlices) {
    budget_.SetCapacitySlots(3);
    for (const bool fail_target : {false, true}) {
        const std::vector<LegacyIndexFile> files{
            MakeFile("0", {1}), MakeFile("1", {2}), MakeFile("2", {3})};
        folly::CancellationSource cancel;
        std::vector<uint8_t> output(3);
        const EntryTarget consume =
            MemoryEntryTarget{nullptr, output.data(), fail_target ? 2u : 3u};
        auto load = std::async(std::launch::async, [&] {
            Run(StreamLegacyIndexFilesAsync(files,
                                            no_chunk_manager_,
                                            fs_,
                                            consume,
                                            kPriority,
                                            cancel.getToken()));
        });
        auto drain = folly::makeGuard([&] {
            CompleteReads();
            load.wait();
        });
        for (const auto& file : files) {
            ASSERT_TRUE(source_->files.at(file.path)->WaitForCallCount(1));
        }
        if (fail_target) {
            source_->files.at("2")->Complete(0);
        } else {
            source_->files.at("2")->Complete(0);
            cancel.requestCancellation();
        }
        EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        const LoadAdmissionRequest all{budget_.CapacityBytes(), 0};
        const bool acquired =
            budget_.TryAcquire(all, LoadAdmissionPriority::High);
        EXPECT_FALSE(acquired);
        if (acquired)
            budget_.Release(all);
        CompleteReads();
        try {
            load.get();
            FAIL() << "load must fail after draining";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(),
                      fail_target ? UnexpectedError : FollyCancel);
        }
        drain.dismiss();
    }
}

TEST_F(LegacyIndexLoaderTest, ShrinkingGlobalLimitsDrainsAdmittedSlices) {
    budget_.SetCapacityBytes(16 * 1024);
    budget_.SetCapacitySlots(2);
    const std::vector<LegacyIndexFile> files{
        MakeFile("0", std::vector<uint8_t>(4096)),
        MakeFile("1", std::vector<uint8_t>(4096)),
        MakeFile("2", std::vector<uint8_t>(4096))};
    std::vector<uint8_t> output(3 * 4096);
    const EntryTarget consume =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
    auto load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFilesAsync(
            files, no_chunk_manager_, fs_, consume, kPriority));
    });
    auto drain = folly::makeGuard([&] {
        CompleteReads();
        load.wait();
    });
    ASSERT_TRUE(source_->files.at("0")->WaitForCallCount(1));
    ASSERT_TRUE(source_->files.at("1")->WaitForCallCount(1));
    budget_.SetCapacityBytes(128);
    budget_.SetCapacitySlots(1);
    SetAsyncLoadThreadPoolSize(2);
    source_->files.at("0")->Complete(0);
    EXPECT_FALSE(source_->files.at("2")->WaitForCallCount(
        1, std::chrono::milliseconds(30)));
    source_->files.at("1")->Complete(0);
    ASSERT_TRUE(source_->files.at("2")->WaitForCallCount(1));
    source_->files.at("2")->Complete(0);
    EXPECT_NO_THROW(load.get());
    drain.dismiss();
}

TEST_F(LegacyIndexLoaderTest, HighPriorityProgressesAheadOfPendingLowSlice) {
    const std::vector<LegacyIndexFile> low{MakeFile("low0", {1}),
                                           MakeFile("low1", {2})};
    const std::vector<LegacyIndexFile> high{MakeFile("high", {3})};
    std::vector<uint8_t> output(3);
    const EntryTarget consume =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
    constexpr auto low_priority = proto::common::LoadPriority::LOW;
    auto low_load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFilesAsync(
                low, no_chunk_manager_, fs_, consume, low_priority),
            low_priority);
    });
    std::future<void> high_load;
    auto drain = folly::makeGuard([&] {
        CompleteReads();
        low_load.wait();
        if (high_load.valid())
            high_load.wait();
    });
    ASSERT_TRUE(source_->files.at("low0")->WaitForCallCount(1));
    high_load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFilesAsync(
            high, no_chunk_manager_, fs_, consume, kPriority));
    });
    EXPECT_EQ(high_load.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    source_->files.at("low0")->Complete(0);
    ASSERT_TRUE(source_->files.at("high")->WaitForCallCount(1));
    EXPECT_TRUE(source_->files.at("low1")->DirectReadCalls().empty());
    source_->files.at("high")->Complete(0);
    ASSERT_TRUE(source_->files.at("low1")->WaitForCallCount(1));
    source_->files.at("low1")->Complete(0);
    EXPECT_NO_THROW(low_load.get());
    EXPECT_NO_THROW(high_load.get());
    drain.dismiss();
}

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
    file_->ResetCounters();
    file_->SetAutoComplete(false);
    std::vector<uint8_t> output(info.payload_bytes);
    const EntryTarget consumer =
        MemoryEntryTarget{nullptr, output.data(), output.size()};
    auto load = std::async(std::launch::async, [&] {
        Run(StreamLegacyIndexFileAsync(*input, info, consumer, kPriority));
    });
    std::future<LegacyIndexFileInfo> sibling;
    auto cleanup = folly::makeGuard([&] {
        file_->SetAutoComplete(true);
        if (file_->WaitForCallCount(1))
            file_->Complete(0);
        load.wait();
        if (sibling.valid())
            sibling.wait();
    });
    ASSERT_TRUE(file_->WaitForCallCount(1));
    ExpectReserved();
    sibling = std::async(std::launch::async, [&] {
        return Run(InspectLegacyIndexFileAsync(*input, kPriority));
    });
    EXPECT_EQ(sibling.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    file_->SetAutoComplete(true);
    file_->Complete(0);
    EXPECT_NO_THROW(load.get());
    EXPECT_EQ(output, (std::vector<uint8_t>{1, 2, 3}));
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
