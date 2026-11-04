// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <array>
#include <atomic>
#include <thread>
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include <future>
#include <functional>
#include <fstream>
#include <map>
#include <set>
#include <numeric>
#include "arrow/filesystem/localfs.h"
#include "milvus-storage/filesystem/fs.h"
#include "common/Geometry.h"
#include "common/OpContext.h"
#include "common/Slice.h"
#include "folly/ScopeGuard.h"
#include "index/IndexTypeAdapter.h"
#include "index/scalar/bitmap/BitmapIndexLoader.h"
#include "index/LegacyIndexLoad.h"
#include "index/LoadResource.h"
#include "segcore/Types.h"
#include "segcore/storagev1translator/SealedIndexTranslator.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorMemBuilder.h"
#include "index/vector/VectorDiskLoader.h"
#include "storage/artifact/DiskEngineFileHandle.h"
#include "index/Meta.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/contracts/query/ISpatialReader.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"
#include "knowhere/version.h"
#include "storage/LocalFileIOPool.h"
#include "storage/LegacyIndexFileIO.h"
#include "storage/FileWriter.h"
#include "storage/Util.h"
#include "storage/artifact/FileSink.h"
#include "test_utils/AsyncLoadTestUtils.h"
#include "test_utils/TmpPath.h"

namespace milvus::index::test {
namespace {
using ControlledFile = milvus::test::ControlledDirectReadFile;

// Real V1DiskSink bytes, exposed by the existing controllable native async file.
class LegacyTestFileSystem final : public arrow::fs::SubTreeFileSystem {
 public:
    LegacyTestFileSystem()
        : SubTreeFileSystem("",
                            std::make_shared<arrow::fs::LocalFileSystem>()) {
    }
    arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFileAsync(const std::string& path) override {
        ++opens;
        const auto it = files.find(path);
        if (it == files.end()) {
            return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
                MakeFinished(arrow::Status::IOError("missing test object"));
        }
        return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
            MakeFinished(std::static_pointer_cast<arrow::io::RandomAccessFile>(
                it->second));
    }
    std::map<std::string, std::shared_ptr<ControlledFile>> files;
    std::atomic<size_t> opens{0};
};

class LegacyIndexLoadTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        storage::LocalFileIOPool::GetInstance().Configure(1);
        auto& admission = storage::LoadAdmissionController::GetInstance();
        old_slots_ = admission.CapacitySlots();
        admission.SetCapacitySlots(4);
        old_slice_ = FILE_SLICE_SIZE.exchange(256);
        storage::StorageConfig config;
        config.storage_type = "local";
        config.root_path = (root_.get() / "objects").string() + "/";
        std::filesystem::create_directories(config.root_path);
        context_ =
            storage::FileManagerContext({1, 2, 3, 100},
                                        {3,
                                         100,
                                         1000,
                                         1,
                                         "legacy_async",
                                         "field",
                                         DataType::INT64,
                                         1,
                                         false},
                                        storage::CreateChunkManager(config),
                                        storage::InitArrowFileSystem(config));
        local_fs_ = context_.fs;
        context_.use_async_load = true;
        staging_ = (root_.get() / "staging").string();
        std::filesystem::create_directories(staging_);
    }
    void
    TearDown() override {
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const bool released =
            admission.TryAcquire({1, 4}, storage::LoadAdmissionPriority::High);
        EXPECT_TRUE(released);
        if (released)
            admission.Release({1, 4});
        admission.SetCapacitySlots(old_slots_);
        FILE_SLICE_SIZE.store(old_slice_);
        storage::LocalFileIOPool::GetInstance().Configure(0);
    }
    void
    Persist(const storage::Artifact& artifact) {
        auto write_context = context_;
        write_context.fs = local_fs_;
        storage::V1DiskSink sink(write_context);
        artifact.Serialize(sink);
        const auto stats = sink.Finish();
        paths_.clear();
        fs_ = std::make_shared<LegacyTestFileSystem>();
        for (const auto& file : stats.Files()) {
            paths_.push_back(file.file_name);
            std::vector<uint8_t> bytes(
                context_.chunkManagerPtr->Size(file.file_name));
            context_.chunkManagerPtr->Read(
                file.file_name, bytes.data(), bytes.size());
            fs_->files.emplace(
                file.file_name,
                std::make_shared<ControlledFile>(std::move(bytes)));
        }
        sink.ReleaseLocalStaging();
        context_.fs = fs_;
    }
    storage::LoadOptions
    Options(Config params, bool mmap) const {
        storage::LoadOptions options;
        options.params = std::move(params);
        options.enable_mmap = mmap;
        options.mmap_dir_path = staging_;
        return options;
    }
    template <typename T>
    T
    Run(folly::coro::Task<T> task) {
        return folly::coro::blockingWait(folly::coro::co_withExecutor(
            storage::ResolveAsyncLoadExecutor(
                {}, proto::common::LoadPriority::HIGH),
            std::move(task)));
    }
    uint64_t
    PhysicalBytes() const {
        uint64_t total = 0;
        for (const auto& [path, file] : fs_->files)
            total += file->GetSize().ValueOrDie();
        return total;
    }
    std::shared_ptr<ControlledFile>
    Object(std::string_view name) const {
        for (const auto& [path, file] : fs_->files) {
            if (std::filesystem::path(path).filename().string() == name)
                return file;
        }
        return {};
    }
    void
    ExpectNativeReads() const {
        ASSERT_FALSE(fs_->files.empty());
        for (const auto& [path, file] : fs_->files) {
            SCOPED_TRACE(path);
            EXPECT_FALSE(file->DirectReadCalls().empty());
            EXPECT_EQ(file->ReadAtCalls(), 0);
            EXPECT_EQ(file->AsyncReadCalls(), 0);
        }
    }
    std::shared_ptr<ControlledFile>
    DataFile() const {
        for (const auto& [path, file] : fs_->files) {
            const auto name = std::filesystem::path(path).filename().string();
            if (name == "index_data" || name.starts_with("index_data_")) {
                return file;
            }
        }
        return {};
    }
    void
    PersistSorted() {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
        ScalarTestData<std::string_view> data(
            {"alpha", "beta", "beta", "null"});
        data.validity.reset(3);
        const ScalarTestInput<std::string_view> input(data);
        Persist(*backend.Build(input.View(), {.row_count = 4}));
    }
    template <typename F>
    void
    ExpectError(ErrorCode code, F&& operation) {
        try {
            operation();
            FAIL() << "expected classified legacy load error";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), code);
        }
    }
    milvus::test::TmpPath root_;
    storage::FileManagerContext context_;
    std::shared_ptr<LegacyTestFileSystem> fs_;
    milvus_storage::ArrowFileSystemPtr local_fs_;
    std::vector<std::string> paths_;
    std::string staging_;
    int64_t old_slice_{};
    size_t old_slots_{};
    milvus::test::ScopedLoadTransientBudget budget_{64 * 1024 * 1024};
};

TEST_F(LegacyIndexLoadTest, TranslatorConsumesInspectedMetadataOnFirstLoad) {
    for (const bool vector : {false, true}) {
        SCOPED_TRACE(vector);
        if (vector) {
            auto adapted = AdaptIndexType({
                .index_type = "FLAT",
                .field_type = DataType::VECTOR_FLOAT,
                .element_type = DataType::NONE,
                .index_engine_version =
                    knowhere::Version::GetCurrentVersion().VersionNumber(),
                .params = {{METRIC_TYPE, "L2"}, {DIM_KEY, 4}},
            });
            auto builder =
                BuilderRegistry<VectorBuildInput<float>>::Instance().Create(
                    adapted.family, adapted.params);
            const std::vector<float> values(16, 1);
            Persist(*std::move(*builder).Build({.physical_values = values,
                                                .logical_rows = 4,
                                                .physical_rows = 4,
                                                .dim = 4}));
        } else {
            PersistSorted();
        }
        segcore::LoadIndexInfo info{};
        info.field_type = vector ? DataType::VECTOR_FLOAT : DataType::VARCHAR;
        info.element_type = DataType::NONE;
        info.field_id = 100;
        info.segment_id = 3;
        info.index_id = 1000;
        info.index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber();
        info.num_rows = 4;
        info.dim = vector ? 4 : 0;
        info.index_files = paths_;
        info.index_size = PhysicalBytes();
        info.mmap_dir_path = staging_;
        info.index_params = {{INDEX_TYPE, vector ? "FLAT" : ASCENDING_SORT},
                             {METRIC_TYPE, "L2"},
                             {SCALAR_INDEX_ENGINE_VERSION, "1"},
                             {"nullable", vector ? "false" : "true"}};
        for (const auto* warmup : {"sync", "async", "disable"}) {
            SCOPED_TRACE(warmup);
            info.warmup_policy = warmup;
            const bool retain_preparation =
                std::string_view(warmup) != "disable";
            // Normal load, pre-cancelled load, and an injected scalar payload error.
            for (const int failure : {0, 1, 2}) {
                if ((vector || !retain_preparation) && failure == 2) {
                    continue;
                }
                SCOPED_TRACE(failure);
                folly::CancellationSource construction;
                OpContext creation(construction.getToken());
                segcore::storagev1translator::SealedIndexTranslator translator(
                    &info,
                    tracer::TraceContext{},
                    context_,
                    Config(info.index_params),
                    &creation);
                construction.requestCancellation();
                const auto probes = [&] {
                    size_t count = 0;
                    for (const auto& [_, file] : fs_->files) {
                        const auto size = file->GetSize().ValueOrDie();
                        for (const auto& call : file->DirectReadCalls())
                            count += call.position == 0 && call.nbytes < size;
                    }
                    return count;
                };
                const auto inspected = probes();
                ASSERT_GT(inspected, 0);
                if (failure == 1) {
                    folly::CancellationSource cancelled;
                    cancelled.requestCancellation();
                    OpContext rejected(cancelled.getToken());
                    ExpectError(FollyCancel,
                                [&] { translator.get_cells(&rejected, {0}); });
                    EXPECT_EQ(probes(), inspected);
                } else if (failure == 2) {
                    auto file = DataFile();
                    ASSERT_NE(file, nullptr);
                    file->SetNextCompletion(
                        arrow::Status::IOError("payload read failed"));
                    OpContext failed;
                    ExpectError(StorageError,
                                [&] { translator.get_cells(&failed, {0}); });
                    EXPECT_EQ(probes(), inspected);
                }
                OpContext load;
                auto cells = translator.get_cells(&load, {0});
                ASSERT_EQ(cells.size(), 1);
                EXPECT_EQ(cells.front().second->Count(), 4);
                if (failure != 0 || !retain_preparation) {
                    EXPECT_GT(probes(), inspected);
                } else {
                    EXPECT_EQ(probes(), inspected);
                }
                cells.clear();
                // The preparation source ends with the first load, so eviction/reload
                // does not retain its metadata outside the reader's cache lifetime.
                cells = translator.get_cells(&load, {0});
                EXPECT_EQ(cells.front().second->Count(), 4);
                EXPECT_GT(probes(), inspected);
            }
        }
    }
}

// Counts actual sync transport bytes; size inspection must not fetch payloads.
class CountingLegacyChunkManager final : public storage::LocalChunkManager {
 public:
    using LocalChunkManager::LocalChunkManager;
    uint64_t
    Size(const std::string& path) override {
        ++size_calls;
        return LocalChunkManager::Size(path);
    }
    uint64_t
    Read(const std::string& path, void* data, uint64_t bytes) override {
        ++whole_reads;
        const auto read = LocalChunkManager::Read(path, 0, data, bytes);
        if (after_read)
            after_read(path);
        return read;
    }
    uint64_t
    Read(const std::string& path,
         uint64_t offset,
         void* data,
         uint64_t bytes) override {
        range_bytes.fetch_add(bytes);
        return LocalChunkManager::Read(path, offset, data, bytes);
    }
    std::atomic<size_t> whole_reads{0};
    std::atomic<size_t> range_bytes{0};
    std::atomic<size_t> size_calls{0};
    std::function<void(const std::string&)> after_read;
};

TEST_F(LegacyIndexLoadTest, SyncUnslicedReadDownloadsOnceWithoutRemoteProbes) {
    FILE_SLICE_SIZE.store(1024 * 1024);
    auto manager = std::make_shared<CountingLegacyChunkManager>(
        context_.chunkManagerPtr->GetRootPath());
    context_.chunkManagerPtr = manager;
    storage::V1DiskSink sink(context_);
    const std::vector<uint8_t> payload(64 * 1024, 17);
    sink.WriteEntry("payload", payload.data(), payload.size());
    const auto stats = sink.Finish();
    ASSERT_EQ(stats.Files().size(), 1);
    storage::V1RemoteSource source(context_, {stats.Files()[0].file_name}, {});
    manager->size_calls = 0;
    manager->whole_reads = 0;
    manager->range_bytes = 0;
    EXPECT_EQ(source.ReadEntry("payload"), payload);
    EXPECT_EQ(manager->size_calls.load(), 1);
    EXPECT_EQ(manager->whole_reads.load(), 1);
    EXPECT_EQ(manager->range_bytes.load(), 0);
    EXPECT_EQ(source.EntrySize("payload"), payload.size());
    EXPECT_EQ(manager->size_calls.load(), 1);
    EXPECT_EQ(manager->range_bytes.load(), 0);
    sink.ReleaseLocalStaging();
}

TEST_F(LegacyIndexLoadTest, SyncMaterializationWritesDirectlyToTarget) {
    context_.use_async_load = false;
    context_.indexMeta.build_id = 6076;
    auto manager = std::make_shared<CountingLegacyChunkManager>(
        context_.chunkManagerPtr->GetRootPath());
    context_.chunkManagerPtr = manager;
    storage::V1DiskSink sink(context_);
    const std::vector<uint8_t> a(517, 17), b(389, 23), empty;
    for (const auto& [name, bytes] :
         std::map<std::string, std::vector<uint8_t>>{
             {"a", a}, {"b", b}, {"empty", empty}})
        sink.WriteEntry(name, bytes.data(), bytes.size());
    const auto stats = sink.Finish();
    std::vector<std::string> paths;
    size_t payload_files = 0;
    for (const auto& file : stats.Files()) {
        paths.push_back(file.file_name);
        if (std::filesystem::path(file.file_name).filename() !=
            INDEX_FILE_SLICE_META)
            ++payload_files;
    }
    sink.ReleaseLocalStaging();
    ASSERT_GT(payload_files, 3);
    const auto cache_root = storage::GenIndexPathPrefix(
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager(),
        context_.indexMeta.build_id,
        context_.indexMeta.index_version,
        context_.indexMeta.segment_id,
        context_.indexMeta.field_id,
        false);
    auto cache_parent = std::filesystem::path(cache_root);
    if (cache_parent.filename().empty())
        cache_parent = cache_parent.parent_path();
    cache_parent = cache_parent.parent_path();
    auto cached_files = [&] {
        std::set<std::string> files;
        if (std::filesystem::exists(cache_parent))
            for (const auto& entry :
                 std::filesystem::recursive_directory_iterator(cache_parent))
                if (entry.is_regular_file())
                    files.insert(entry.path().string());
        return files;
    };
    auto read = [](const std::string& path) {
        std::ifstream input(path, std::ios::binary);
        return std::vector<uint8_t>(std::istreambuf_iterator<char>(input), {});
    };
    const std::vector<std::string> names{"b", "empty", "a"};
    for (const auto layout : {storage::V1SourceLayout::MemoryEntries,
                              storage::V1SourceLayout::DiskFiles}) {
        for (int mode : {0, 1, 2}) {
            SCOPED_TRACE(::testing::Message()
                         << "mode=" << mode << " layout=" << int(layout));
            storage::V1RemoteSource source(
                context_,
                paths,
                {},
                storage::ArtifactStorageNamespace::Index,
                layout);
            const auto before = cached_files();
            manager->whole_reads = manager->range_bytes = 0;
            const auto target = staging_ + "/combined";
            if (mode == 0) {
                source.ReadEntryToLocalFile("a", target);
                EXPECT_EQ(read(target), a);
            } else if (mode == 1) {
                source.ReadEntriesToLocalFile(names, target);
                auto combined = b;
                combined.insert(combined.end(), a.begin(), a.end());
                EXPECT_EQ(read(target), combined);
            } else {
                const auto outputs =
                    source.ReadEntriesToLocalDir(names, staging_);
                ASSERT_EQ(outputs.size(), 3);
                EXPECT_EQ(read(outputs[0]), b);
                EXPECT_EQ(read(outputs[1]), empty);
                EXPECT_EQ(read(outputs[2]), a);
            }
            EXPECT_EQ(cached_files(), before);
            EXPECT_EQ(manager->range_bytes, 0);
            if (mode != 0)
                EXPECT_EQ(manager->whole_reads, payload_files);

            auto materialize = [&] {
                if (mode == 0)
                    source.ReadEntryToLocalFile("a", target);
                else if (mode == 1)
                    source.ReadEntriesToLocalFile(names, target);
                else
                    source.ReadEntriesToLocalDir(names, staging_);
            };
            auto outputs = [&] {
                std::map<std::string, std::vector<uint8_t>> contents;
                for (const auto& entry :
                     std::filesystem::directory_iterator(staging_))
                    if (entry.is_regular_file())
                        contents.emplace(entry.path().string(),
                                         read(entry.path()));
                return contents;
            };
            const auto published = outputs();
            folly::CancellationSource cancel;
            source.SetLoadContext(proto::common::LoadPriority::HIGH,
                                  cancel.getToken());
            manager->after_read = [&](const auto&) {
                cancel.requestCancellation();
            };
            ExpectError(FollyCancel, materialize);
            manager->after_read = {};
            EXPECT_EQ(outputs(), published);
            source.SetLoadContext(proto::common::LoadPriority::HIGH, {});

            const auto slice =
                *std::find_if(paths.begin(), paths.end(), [](const auto& path) {
                    return std::filesystem::path(path).filename() == "a_0";
                });
            std::vector<uint8_t> encoded(manager->Size(slice));
            manager->Read(slice, encoded.data(), encoded.size());
            auto restore = folly::makeGuard(
                [&] { manager->Write(slice, encoded.data(), encoded.size()); });
            uint8_t corrupt = 0;
            manager->Write(slice, &corrupt, 1);
            ExpectError(DataFormatBroken, materialize);
            EXPECT_EQ(outputs(), published);
            manager->Write(slice, encoded.data(), encoded.size());
            materialize();
            EXPECT_EQ(outputs(), published);
        }
    }
}

TEST_F(LegacyIndexLoadTest, SyncMaterializationUsesWritePermitAndDirectIO) {
    context_.use_async_load = false;
    FILE_SLICE_SIZE.store(128 * 1024);
    auto manager = std::make_shared<CountingLegacyChunkManager>(
        context_.chunkManagerPtr->GetRootPath());
    context_.chunkManagerPtr = manager;
    const std::vector<uint8_t> a(storage::FileWriter::DEFAULT_BUFFER_SIZE + 17,
                                 17),
        b(389, 23);
    storage::V1DiskSink sink(context_);
    sink.WriteEntry("a", a.data(), a.size());
    sink.WriteEntry("b", b.data(), b.size());
    std::vector<std::string> paths;
    const auto stats = sink.Finish();
    for (const auto& file : stats.Files()) paths.push_back(file.file_name);
    sink.ReleaseLocalStaging();
    storage::V1RemoteSource source(context_, paths, {});
    source.SetLoadContext(proto::common::LoadPriority::LOW, {});
    const auto old_mode = storage::FileWriter::GetMode();
    auto restore =
        folly::makeGuard([&] { storage::FileWriter::SetMode(old_mode); });
    storage::FileWriter::SetMode(storage::FileWriter::WriteMode::DIRECT);
    auto read = [](const std::string& path) {
        std::ifstream input(path, std::ios::binary);
        return std::vector<uint8_t>(std::istreambuf_iterator<char>(input), {});
    };
    for (const int mode : {0, 1, 2}) {
        SCOPED_TRACE(mode);
        const auto target = staging_ + "/combined";
        auto permit =
            storage::LocalFileIOPool::GetInstance().AcquireWritePermit();
        std::promise<void> read_started;
        std::once_flag first_read;
        manager->after_read = [&](const auto&) {
            std::call_once(first_read, [&] { read_started.set_value(); });
        };
        auto write = std::async(std::launch::async, [&] {
            if (mode == 0)
                source.ReadEntryToLocalFile("a", target);
            else if (mode == 1)
                source.ReadEntriesToLocalFile({"a", "b"}, target);
            else
                source.ReadEntriesToLocalDir({"a", "b"}, staging_);
        });
        auto release = folly::makeGuard([&] { permit = {}; });
        EXPECT_EQ(read_started.get_future().wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        EXPECT_EQ(write.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        permit = {};
        write.get();
        manager->after_read = {};
        if (mode == 2) {
            EXPECT_EQ(read(staging_ + "/a"), a);
            EXPECT_EQ(read(staging_ + "/b"), b);
        } else {
            auto expected = a;
            if (mode == 1)
                expected.insert(expected.end(), b.begin(), b.end());
            EXPECT_EQ(read(target), expected);
        }
    }
}

TEST_F(LegacyIndexLoadTest, AsyncEnvelopeInspectionBoundsEachBatch) {
    FILE_SLICE_SIZE.store(1024 * 1024);
    PersistSorted();
    const auto file = DataFile();
    ASSERT_NE(file, nullptr);
    auto& admission = storage::LoadAdmissionController::GetInstance();
    auto restore = folly::makeGuard([&] { admission.SetCapacitySlots(4); });
    struct Scenario {
        size_t slots;
        size_t batches;
        size_t expected_inflight;
    };
    for (const auto scenario : {Scenario{0, 1, 16},
                                Scenario{0, 2, 32},
                                Scenario{32, 1, 16},
                                Scenario{8, 2, 8},
                                Scenario{1, 1, 1}}) {
        SCOPED_TRACE(::testing::Message() << "slots=" << scenario.slots
                                          << " batches=" << scenario.batches);
        admission.SetCapacitySlots(scenario.slots);
        fs_->files.clear();
        fs_->opens = 0;
        file->ResetCounters();
        file->SetAutoComplete(false);
        std::vector<std::vector<std::string>> batches(scenario.batches);
        for (size_t batch = 0; batch < batches.size(); ++batch) {
            batches[batch].reserve(64);
            for (size_t i = 0; i < 64; ++i) {
                auto path = std::to_string(batch) + "/" + std::to_string(i);
                batches[batch].push_back(path);
                // Aliased immutable objects let the file track aggregate I/O.
                fs_->files.emplace(std::move(path), file);
            }
        }
        folly::CancellationSource cancel;
        std::vector<
            std::future<std::shared_ptr<const storage::LegacyIndexFileInfos>>>
            pending;
        pending.reserve(batches.size());
        auto drain = folly::makeGuard([&] {
            cancel.requestCancellation();
            file->SetAutoComplete(true);
            for (size_t i = 0; i < file->DirectReadCalls().size(); ++i)
                file->Complete(i);
            for (auto& result : pending)
                if (result.valid())
                    result.wait();
        });
        for (size_t batch = 0; batch < batches.size(); ++batch) {
            pending.push_back(std::async(std::launch::async, [&, batch] {
                return Run(storage::InspectLegacyIndexFilesAsync(
                    batches[batch],
                    context_.chunkManagerPtr,
                    context_.fs,
                    proto::common::LoadPriority::HIGH,
                    cancel.getToken()));
            }));
        }
        ASSERT_TRUE(file->WaitForCallCount(scenario.expected_inflight));
        EXPECT_FALSE(file->WaitForCallCount(scenario.expected_inflight + 1,
                                            std::chrono::milliseconds(30)));
        EXPECT_EQ(file->PeakInflight(), scenario.expected_inflight);
        if (scenario.slots == 0 || scenario.slots >= 16)
            EXPECT_EQ(fs_->opens.load(), 16 * batches.size());
        file->SetAutoComplete(true);
        for (size_t i = 0; i < file->DirectReadCalls().size(); ++i)
            file->Complete(i);
        for (auto& result : pending) {
            const auto infos = result.get();
            EXPECT_EQ(infos->size(), 64);
            for (const auto& [path, info] : *infos)
                EXPECT_GT(info.payload_bytes, 0);
        }
        EXPECT_EQ(fs_->opens.load(), 64 * batches.size());
        EXPECT_LE(file->PeakInflight(), scenario.expected_inflight);
    }
}

TEST_F(LegacyIndexLoadTest, AsyncEnvelopeInspectionOverlapsAndDrainsOnFailure) {
    auto& admission = storage::LoadAdmissionController::GetInstance();
    auto restore = folly::makeGuard([&] { admission.SetCapacitySlots(4); });
    for (const auto [slots, mode] :
         {std::pair{0, 0}, {0, 1}, {0, 2}, {4, 0}, {4, 1}, {4, 2}}) {
        SCOPED_TRACE(::testing::Message()
                     << "slots=" << slots << " mode=" << mode);
        admission.SetCapacitySlots(slots);
        FILE_SLICE_SIZE.store(1024 * 1024);
        PersistSorted();
        auto source =
            Run(storage::V1RemoteSource::OpenAsync(context_, paths_, {}));
        ASSERT_GE(source->EntryNames().size(), 2);
        const auto first = fs_->files.begin()->second;
        const auto second = std::next(fs_->files.begin())->second;
        for (const auto& [path, file] : fs_->files) {
            file->ResetCounters();
        }
        first->SetAutoComplete(false);
        second->SetAutoComplete(false);
        folly::CancellationSource cancel;
        if (mode == 0)
            source->SetLoadContext(proto::common::LoadPriority::HIGH,
                                   cancel.getToken());
        auto load = std::async(std::launch::async, [&] {
            const auto names = source->EntryNames();
            auto task = source->InspectLoadBytesAsync(names);
            return mode == 1 ? Run(folly::coro::co_withCancellation(
                                   cancel.getToken(), std::move(task)))
                             : Run(std::move(task));
        });
        auto drain = folly::makeGuard([&] {
            cancel.requestCancellation();
            for (const auto& [path, file] : fs_->files) {
                file->SetAutoComplete(true);
                for (size_t i = 0; i < file->DirectReadCalls().size(); ++i)
                    file->Complete(i);
            }
            if (load.valid())
                load.wait();
        });
        ASSERT_TRUE(first->WaitForCallCount(1));
        ASSERT_TRUE(second->WaitForCallCount(1));
        if (mode == 2)
            second->Complete(
                0, arrow::Status::IOError("injected envelope read failure"));
        else
            cancel.requestCancellation();
        EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        for (const auto& [path, file] : fs_->files) {
            file->SetAutoComplete(true);
            for (size_t i = 0; i < file->DirectReadCalls().size(); ++i)
                file->Complete(i);
        }
        ExpectError(mode == 2 ? StorageError : FollyCancel,
                    [&] { load.get(); });
        drain.dismiss();
        source->SetLoadContext(proto::common::LoadPriority::HIGH, {});
        EXPECT_GT(
            Run(source->InspectLoadBytesAsync(source->EntryNames())).payload,
            0);
    }
}

TEST_F(LegacyIndexLoadTest, SyncEnvelopeBypassesPayloadAdmission) {
    FILE_SLICE_SIZE.store(1024 * 1024);
    auto manager = std::make_shared<CountingLegacyChunkManager>(
        context_.chunkManagerPtr->GetRootPath());
    context_.chunkManagerPtr = manager;
    storage::V1DiskSink sink(context_);
    const std::vector<uint8_t> payload(64 * 1024, 17);
    sink.WriteEntry("payload", payload.data(), payload.size());
    const auto stats = sink.Finish();
    ASSERT_EQ(stats.Files().size(), 1);
    storage::V1RemoteSource source(context_, {stats.Files()[0].file_name}, {});
    folly::CancellationSource cancel;
    source.SetLoadContext(proto::common::LoadPriority::LOW, cancel.getToken());
    auto& budget = storage::LoadAdmissionController::GetInstance();
    auto lease = Run([&]() -> folly::coro::Task<storage::LoadAdmissionLease> {
        co_return co_await budget.AcquireAsync(
            {1, 4}, storage::LoadAdmissionPriority::High);
    }());
    manager->whole_reads = 0;
    auto load = std::async(std::launch::async,
                           [&] { return source.ReadEntry("payload"); });
    auto drain = folly::makeGuard([&] {
        cancel.requestCancellation();
        lease.Release();
        if (load.valid())
            load.wait();
    });
    EXPECT_EQ(load.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    lease.Release();
    EXPECT_EQ(load.get(), payload);
    EXPECT_EQ(manager->whole_reads.load(), 1);
    cancel.requestCancellation();
    ExpectError(FollyCancel, [&] { source.ReadEntry("payload"); });
    EXPECT_EQ(manager->whole_reads.load(), 1);
    sink.ReleaseLocalStaging();
}

TEST_F(LegacyIndexLoadTest, AsyncEnvelopeOpenWaitsForSharedSlots) {
    FILE_SLICE_SIZE.store(1024 * 1024);
    PersistSorted();
    auto source = Run(storage::V1RemoteSource::OpenAsync(context_, paths_, {}));
    fs_->opens = 0;
    auto& budget = storage::LoadAdmissionController::GetInstance();
    auto lease = Run([&]() -> folly::coro::Task<storage::LoadAdmissionLease> {
        co_return co_await budget.AcquireAsync(
            {1, 4}, storage::LoadAdmissionPriority::High);
    }());
    folly::CancellationSource cancel;
    source->SetLoadContext(proto::common::LoadPriority::HIGH,
                           cancel.getToken());
    auto load = std::async(std::launch::async, [&] {
        return Run(source->InspectLoadBytesAsync(source->EntryNames()));
    });
    auto drain = folly::makeGuard([&] {
        cancel.requestCancellation();
        lease.Release();
        if (load.valid())
            load.wait();
    });
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    EXPECT_EQ(fs_->opens.load(), 0);
    lease.Release();
    EXPECT_GT(load.get().payload, 0);
    EXPECT_GT(fs_->opens.load(), 0);
}

TEST_F(LegacyIndexLoadTest, SyncSizeReadsOnlyHeadersAndDoesNotCachePayload) {
    FILE_SLICE_SIZE.store(1024 * 1024);
    auto manager = std::make_shared<CountingLegacyChunkManager>(
        context_.chunkManagerPtr->GetRootPath());
    context_.chunkManagerPtr = manager;
    storage::V1DiskSink sink(context_);
    std::vector<uint8_t> payload(64 * 1024, 17);
    sink.WriteEntry("payload", payload.data(), payload.size());
    const auto stats = sink.Finish();
    std::vector<std::string> paths;
    for (const auto& file : stats.Files()) paths.push_back(file.file_name);
    ASSERT_EQ(paths.size(), 1);
    storage::V1RemoteSource source(context_, paths, {});
    manager->whole_reads.store(0);
    manager->range_bytes.store(0);
    EXPECT_EQ(source.EntrySize("payload"), payload.size());
    EXPECT_EQ(manager->whole_reads.load(), 0);
    EXPECT_GT(manager->range_bytes.load(), 0);
    EXPECT_LT(manager->range_bytes.load(), payload.size());
    const auto inspected_bytes = manager->range_bytes.load();
    EXPECT_EQ(source.EntrySize("payload"), payload.size());
    EXPECT_EQ(manager->range_bytes.load(), inspected_bytes);
    EXPECT_EQ(source.ReadEntry("payload"), payload);
    const auto after_first = manager->whole_reads.load();
    EXPECT_GT(after_first, 0);
    EXPECT_EQ(source.ReadEntry("payload"), payload);
    EXPECT_GT(manager->whole_reads.load(), after_first);
    const auto local = staging_ + "/payload";
    source.ReadEntryToLocalFile("payload", local);
    std::ifstream input(local, std::ios::binary);
    const std::vector<uint8_t> actual((std::istreambuf_iterator<char>(input)),
                                      {});
    EXPECT_EQ(actual, payload);
    sink.ReleaseLocalStaging();
}

// Remote ChunkManagers implement only whole-object reads.
class WholeObjectLegacyChunkManager final : public storage::LocalChunkManager {
 public:
    using LocalChunkManager::LocalChunkManager;
    uint64_t
    Read(const std::string& path, void* data, uint64_t bytes) override {
        ++whole_reads;
        // LocalChunkManager's whole read dispatches to the virtual range read.
        return LocalChunkManager::Read(path, 0, data, bytes);
    }
    uint64_t
    Read(const std::string&, uint64_t, void*, uint64_t) override {
        ThrowInfo(NotImplemented, "Read with offset not implement");
    }
    std::atomic<size_t> whole_reads{0};
};

TEST_F(LegacyIndexLoadTest, SyncSizeReadsHeadersThroughRemoteFilesystem) {
    FILE_SLICE_SIZE.store(1024 * 1024);
    auto manager = std::make_shared<WholeObjectLegacyChunkManager>(
        context_.chunkManagerPtr->GetRootPath());
    context_.chunkManagerPtr = manager;
    storage::V1DiskSink sink(context_);
    std::vector<uint8_t> payload(64 * 1024, 23);
    sink.WriteEntry("payload", payload.data(), payload.size());
    const auto stats = sink.Finish();
    std::vector<std::string> paths;
    for (const auto& file : stats.Files()) paths.push_back(file.file_name);
    ASSERT_EQ(paths.size(), 1);
    // A local filesystem keeps the ChunkManager as the range reader.
    ExpectError(ErrorCode::NotImplemented, [&] {
        storage::V1RemoteSource source(context_, paths, {});
        source.EntrySize("payload");
    });
    context_.fs = std::make_shared<arrow::fs::SubTreeFileSystem>(
        "", std::make_shared<arrow::fs::LocalFileSystem>());
    ASSERT_FALSE(milvus_storage::IsLocalFileSystem(context_.fs));
    storage::V1RemoteSource source(context_, paths, {});
    manager->whole_reads.store(0);
    EXPECT_EQ(source.EntrySize("payload"), payload.size());
    EXPECT_EQ(manager->whole_reads.load(), 0);
    EXPECT_EQ(source.ReadEntry("payload"), payload);
    EXPECT_GT(manager->whole_reads.load(), 0);
    sink.ReleaseLocalStaging();
}

TEST_F(LegacyIndexLoadTest, AsyncFamilyRunsOutsideLocalFilePool) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    ScalarTestData<int64_t> data({10, 20, 10});
    const ScalarTestInput<int64_t> rows(data);
    const auto artifact =
        SerializeV1V2(*backend.Build(rows.View(), {.row_count = 3}));
    auto source = std::make_shared<TestArtifactSource>(
        artifact, storage::Generation::V1V2);
    std::thread::id file_thread;
    Run(storage::RunLocalFileIOAsync(
        [&] { file_thread = std::this_thread::get_id(); },
        proto::common::LoadPriority::HIGH));
    const auto caller = std::this_thread::get_id();
    for (bool use_async : {false, true}) {
        LegacyIndexSource input{source, use_async};
        LegacyLoadFn load =
            [&](storage::FileSource&,
                const storage::LoadOptions&,
                bool async) -> folly::coro::Task<IIndexReaderBasePtr> {
            if (async)
                EXPECT_NE(std::this_thread::get_id(), file_thread);
            else
                EXPECT_EQ(std::this_thread::get_id(), caller);
            co_return OpenV1V2(backend, artifact, {.row_count = 3});
        };
        storage::LoadOptions options;
        auto task = RunLegacyLoad(input, options, std::move(load), nullptr);
        auto reader = use_async ? Run(std::move(task))
                                : folly::coro::blockingWait(std::move(task));
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->Count(), 3);
    }
}

TEST_F(LegacyIndexLoadTest, CreateKeepsMetadataForFreshLoadContexts) {
    for (const auto* name : {"BitmapVarchar", "SortedVarchar"}) {
        SCOPED_TRACE(name);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        ScalarTestData<std::string_view> data(
            {"alpha", "beta", "beta", "null"});
        data.validity.reset(3);
        const ScalarTestInput<std::string_view> values(data);
        Persist(*backend.Build(values.View(), {.row_count = 4}));
        folly::CancellationSource opening;
        OpContext open_context(opening.getToken());
        auto options = Options(backend.LoadParams({.row_count = 4}), true);
        options.op_ctx = &open_context;
        const auto loader_entry =
            LoaderRegistry::Instance().Lookup(backend.Family());
        auto source = std::shared_ptr<storage::V1RemoteSource>(
            Run(storage::V1RemoteSource::OpenAsync(
                context_,
                paths_,
                options,
                storage::ArtifactStorageNamespace::Index,
                storage::V1SourceLayout::MemoryEntries)));
        auto create_options = options;
        create_options.op_ctx = nullptr;
        auto loader = Run(loader_entry.create(
            OpenedIndexSource{LegacyIndexSource{source, true}},
            std::move(create_options)));
        source->SetLoadContext(proto::common::LoadPriority::HIGH, {});
        ASSERT_NE(loader, nullptr);
        EXPECT_TRUE(std::filesystem::is_empty(staging_));
        std::map<std::string, size_t> metadata_reads;
        for (const auto& [path, file] : fs_->files) {
            const auto count = file->DirectReadCalls().size();
            if (count != 0)
                metadata_reads.emplace(path, count);
        }
        ASSERT_FALSE(metadata_reads.empty());
        opening.requestCancellation();
        folly::CancellationSource first;
        OpContext first_context(first.getToken());
        auto reader1 = Run(loader->Load(&first_context));
        first.requestCancellation();
        OpContext second_context;
        auto reader2 = Run(loader->Load(&second_context));
        for (const auto& [path, count] : metadata_reads)
            EXPECT_EQ(fs_->files.at(path)->DirectReadCalls().size(), count);
        loader.reset();
        for (const auto* reader : {reader1.get(), reader2.get()}) {
            const auto* pattern =
                dynamic_cast<const IPatternMatchReader*>(reader);
            ASSERT_NE(pattern, nullptr);
            ExpectHits(pattern->PatternMatch("beta", PatternOp::PrefixMatch),
                       4,
                       {1, 2});
            const auto* nulls = dynamic_cast<const INullReader*>(reader);
            ASSERT_NE(nulls, nullptr);
            ExpectHits(nulls->IsNull(), 4, {3});
        }
        reader1.reset();
        reader2.reset();
        EXPECT_TRUE(std::filesystem::is_empty(staging_));
    }
}

TEST_F(LegacyIndexLoadTest, FailedCreateDetachesBorrowedSourceContext) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("BitmapVarchar");
    ScalarTestData<std::string_view> data({"alpha", "beta"});
    const ScalarTestInput<std::string_view> input(data);
    Persist(*backend.Build(input.View(), {.row_count = 2}));
    auto options = Options(backend.LoadParams({.row_count = 2}), false);
    auto source = std::shared_ptr<storage::V1RemoteSource>(
        Run(storage::V1RemoteSource::OpenAsync(
            context_,
            paths_,
            options,
            storage::ArtifactStorageNamespace::Index,
            storage::V1SourceLayout::MemoryEntries)));
    folly::CancellationSource opening;
    OpContext context(opening.getToken());
    options.op_ctx = &context;
    options.params["field_type"] = DataType::NONE;
    options.params["value_type"] = DataType::NONE;
    IndexLoadRequest request{OpenedIndexSource{LegacyIndexSource{source, true}},
                             options};
    const auto loader_entry =
        LoaderRegistry::Instance().Lookup(backend.Family());
    ExpectError(DataTypeInvalid, [&] {
        static_cast<void>(loader_entry.Load(std::move(request)));
    });
    opening.requestCancellation();
    // No new loader/context bind is allowed to hide a leaked opening token.
    EXPECT_FALSE(Run(source->ReadEntryAsync(BITMAP_INDEX_META)).empty());
    EXPECT_TRUE(std::filesystem::is_empty(staging_));
}

TEST_F(LegacyIndexLoadTest, CancelledLoadDoesNotPoisonPreparedMetadata) {
    PersistSorted();
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
    auto options = Options(backend.LoadParams({.row_count = 4}), true);
    const auto loader_entry =
        LoaderRegistry::Instance().Lookup(backend.Family());
    auto source = std::shared_ptr<storage::V1RemoteSource>(
        Run(storage::V1RemoteSource::OpenAsync(
            context_,
            paths_,
            options,
            storage::ArtifactStorageNamespace::Index,
            storage::V1SourceLayout::MemoryEntries)));
    auto loader = Run(loader_entry.create(
        OpenedIndexSource{LegacyIndexSource{source, true}}, options));
    source->SetLoadContext(proto::common::LoadPriority::HIGH, {});
    folly::CancellationSource cancelled;
    cancelled.requestCancellation();
    OpContext rejected(cancelled.getToken());
    ExpectError(FollyCancel, [&] { Run(loader->Load(&rejected)); });
    EXPECT_TRUE(std::filesystem::is_empty(staging_));
    auto reader = Run(loader->Load());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 4);
}

TEST_F(LegacyIndexLoadTest,
       ScalarHeapAndMmapReadRealLegacySlicesAsynchronously) {
    for (const auto* name : {"BitmapVarchar",
                             "SortedVarchar",
                             "InvertedVarchar",
                             "MarisaVarchar"}) {
        for (bool mmap : {false, true}) {
            SCOPED_TRACE(name);
            SCOPED_TRACE(mmap);
            const auto& backend =
                ScalarReaderBackends().Get<std::string_view>(name);
            ScalarTestData<std::string_view> data(
                {"alpha", "beta", "beta", "null"});
            data.validity.reset(3);
            const ScalarTestInput<std::string_view> input(data);
            Persist(*backend.Build(input.View(), {.row_count = 4}));
            auto options = Options(backend.LoadParams({.row_count = 4}), mmap);
            const auto loader_entry =
                LoaderRegistry::Instance().Lookup(backend.Family());
            auto reader = loader_entry.Load(
                {IndexFiles{context_,
                            paths_,
                            LegacyIndexStorageConfig{
                                backend.Family() == families::kInverted
                                    ? storage::V1SourceLayout::DiskFiles
                                    : storage::V1SourceLayout::MemoryEntries}},
                 options});
            const auto* pattern =
                dynamic_cast<const IPatternMatchReader*>(reader.get());
            ASSERT_NE(pattern, nullptr);
            ExpectHits(pattern->PatternMatch("beta", PatternOp::PrefixMatch),
                       4,
                       {1, 2});
            const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
            ASSERT_NE(nulls, nullptr);
            ExpectHits(nulls->IsNull(), 4, {3});
            ExpectNativeReads();
            reader.reset();
            EXPECT_TRUE(std::filesystem::is_empty(staging_));
        }
    }
}

TEST_F(LegacyIndexLoadTest, AdditionalFamiliesPreserveAsyncQueryResults) {
    for (const auto* name :
         {"NgramVarcharMin2Max4Heap", "SpatialRTreeHeap", "JsonFlatV7"}) {
        for (bool mmap : {false, true}) {
            SCOPED_TRACE(name);
            SCOPED_TRACE(mmap);
            const auto& backend =
                ScalarReaderBackends().Get<std::string_view>(name);
            const bool spatial = backend.Family() == families::kRTree;
            const bool json = backend.Family() == families::kJsonFlat;
            std::vector<std::string> values{"alpha", "beta", "beta", "null"};
            if (spatial) {
                values.clear();
                for (const char* wkt :
                     {"POINT(0 0)", "POINT(1 1)", "POINT(1 1)", "POINT(2 2)"}) {
                    values.push_back(Geometry(GetThreadLocalGEOSContext(), wkt)
                                         .to_wkb_string());
                }
            } else if (json) {
                values = {R"({"a":"alpha"})",
                          R"({"a":"beta"})",
                          R"({"a":"beta"})",
                          R"({"a":"ignored"})"};
            }
            ScalarTestData<std::string_view> data(std::move(values));
            data.validity.reset(3);
            const ScalarTestInput<std::string_view> input(data);
            Persist(*backend.Build(input.View(), {.row_count = 4}));
            auto reader =
                LoaderRegistry::Instance()
                    .Lookup(backend.Family())
                    .Load(
                        {IndexFiles{context_,
                                    paths_,
                                    LegacyIndexStorageConfig{
                                        storage::V1SourceLayout::DiskFiles}},
                         Options(backend.LoadParams({.row_count = 4}), mmap)});
            ASSERT_NE(reader, nullptr);
            EXPECT_EQ(reader->Count(), 4);
            if (spatial) {
                const auto* spatial_reader =
                    dynamic_cast<const ISpatialReader*>(reader.get());
                ASSERT_NE(spatial_reader, nullptr);
                Geometry query(GetThreadLocalGEOSContext(), "POINT(1 1)");
                ExpectHits(
                    spatial_reader->Candidates(SpatialOp::Intersects, query),
                    4,
                    {1, 2});
            } else if (json) {
                const auto* json_reader =
                    dynamic_cast<const IJsonIndexReader*>(reader.get());
                ASSERT_NE(json_reader, nullptr);
                ExpectHits(json_reader->Exists("/a"), 4, {0, 1, 2});
                auto resolved = json_reader->Resolve(
                    "/a", JsonCastType::FromString("VARCHAR"));
                ASSERT_TRUE(resolved);
                const auto* scalar = dynamic_cast<
                    const IScalarPredicateReader<std::string_view>*>(
                    resolved.get());
                ASSERT_NE(scalar, nullptr);
                const std::string_view term = "beta";
                ExpectHits(scalar->In(1, &term), 4, {1, 2});
                const auto* nulls =
                    dynamic_cast<const INullReader*>(resolved.get());
                ASSERT_NE(nulls, nullptr);
                ExpectHits(nulls->IsNull(), 4, {3});
            } else {
                const auto* ngram =
                    dynamic_cast<const INgramReader*>(reader.get());
                ASSERT_NE(ngram, nullptr);
                ASSERT_TRUE(ngram->CanHandle("beta", PatternOp::InnerMatch));
                TargetBitmap candidates(4, true);
                ngram->Candidates("beta", PatternOp::InnerMatch, candidates);
                ExpectHits(candidates, 4, {1, 2});
            }
            if (!json) {
                const auto* nulls =
                    dynamic_cast<const INullReader*>(reader.get());
                ASSERT_NE(nulls, nullptr);
                ExpectHits(nulls->IsNull(), 4, {3});
            }
            ExpectNativeReads();
            reader.reset();
            EXPECT_TRUE(std::filesystem::is_empty(staging_));
        }
    }
}

TEST_F(LegacyIndexLoadTest, InvertedSlicedNullOffsetsPreserveRows) {
    FILE_SLICE_SIZE.store(64);
    constexpr size_t rows = 10000;
    const auto& backend = ScalarReaderBackends().Get<int64_t>("InvertedInt64");
    std::vector<int64_t> values(rows);
    std::iota(values.begin(), values.end(), 0);
    ScalarTestData<int64_t> data(std::move(values));
    for (size_t row = 0; row < rows; row += 5) data.validity.reset(row);
    const ScalarTestInput<int64_t> input(data);
    Persist(*backend.Build(input.View(), {.row_count = rows}));
    ASSERT_NE(Object(INDEX_FILE_SLICE_META), nullptr);
    ASSERT_NE(Object(std::string(INDEX_NULL_OFFSET) + "_0"), nullptr);
    ASSERT_NE(Object(std::string(INDEX_NULL_OFFSET) + "_1"), nullptr);
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        auto reader =
            LoaderRegistry::Instance()
                .Lookup(backend.Family())
                .Load({IndexFiles{context_,
                                  paths_,
                                  LegacyIndexStorageConfig{
                                      storage::V1SourceLayout::DiskFiles}},
                       Options(backend.LoadParams({.row_count = rows}), mmap)});
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->Count(), rows);
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        const auto* predicate =
            dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        ASSERT_NE(predicate, nullptr);
        EXPECT_EQ(nulls->IsNull().count(), rows / 5);
        const int64_t valid_key = 9999;
        const int64_t null_key = 9995;
        ExpectHits(predicate->In(1, &valid_key), rows, {9999});
        ExpectHits(predicate->In(1, &null_key), rows, {});
        ExpectNativeReads();
        reader.reset();
        EXPECT_TRUE(std::filesystem::is_empty(staging_));
    }
}

TEST_F(LegacyIndexLoadTest, TextSlicedNullOffsetsPreserveAllNullRows) {
    FILE_SLICE_SIZE.store(4 * 1024);
    constexpr size_t rows = 4 * 1024 / sizeof(size_t) + 1;
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharV5Heap");
    ScalarTestData<std::string_view> data(
        std::vector<std::string>(rows, "no indexed token"));
    for (size_t row = 0; row < rows; ++row) data.validity.reset(row);
    const ScalarTestInput<std::string_view> input(data);
    Persist(*backend.Build(input.View(), {.row_count = rows}));
    ASSERT_NE(Object(INDEX_FILE_SLICE_META), nullptr);
    ASSERT_NE(Object(std::string(INDEX_NULL_OFFSET) + "_0"), nullptr);
    ASSERT_NE(Object(std::string(INDEX_NULL_OFFSET) + "_1"), nullptr);
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        auto reader =
            LoaderRegistry::Instance()
                .Lookup(backend.Family())
                .Load({IndexFiles{context_,
                                  paths_,
                                  LegacyIndexStorageConfig{
                                      storage::V1SourceLayout::DiskFiles}},
                       Options(backend.LoadParams({.row_count = rows}), mmap)});
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->Count(), rows);
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        const auto* text = dynamic_cast<const ITextMatchReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        ASSERT_NE(text, nullptr);
        EXPECT_EQ(nulls->IsNull().count(), rows);
        EXPECT_EQ(nulls->IsNotNull().count(), 0);
        EXPECT_EQ(text->MatchQuery("token", 1).count(), 0);
        ExpectNativeReads();
        reader.reset();
        EXPECT_TRUE(std::filesystem::is_empty(staging_));
    }
}

TEST_F(LegacyIndexLoadTest, RTreeSlicedNullOffsetsPreserveCandidates) {
    FILE_SLICE_SIZE.store(64);
    constexpr size_t rows = 24;
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SpatialRTreeHeap");
    std::vector<std::string> values;
    values.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        const auto wkt =
            "POINT(" + std::to_string(row) + " " + std::to_string(row) + ")";
        values.push_back(
            Geometry(GetThreadLocalGEOSContext(), wkt.c_str()).to_wkb_string());
    }
    ScalarTestData<std::string_view> data(std::move(values));
    for (size_t row = 0; row < rows; row += 2) data.validity.reset(row);
    const ScalarTestInput<std::string_view> input(data);
    Persist(*backend.Build(input.View(), {.row_count = rows}));
    ASSERT_NE(Object(INDEX_FILE_SLICE_META), nullptr);
    ASSERT_NE(Object(std::string(INDEX_NULL_OFFSET) + "_0"), nullptr);
    ASSERT_NE(Object(std::string(INDEX_NULL_OFFSET) + "_1"), nullptr);
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        auto reader =
            LoaderRegistry::Instance()
                .Lookup(backend.Family())
                .Load({IndexFiles{context_,
                                  paths_,
                                  LegacyIndexStorageConfig{
                                      storage::V1SourceLayout::DiskFiles}},
                       Options(backend.LoadParams({.row_count = rows}), mmap)});
        ASSERT_NE(reader, nullptr);
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        const auto* spatial = dynamic_cast<const ISpatialReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        ASSERT_NE(spatial, nullptr);
        EXPECT_EQ(reader->Count(), rows);
        EXPECT_EQ(nulls->IsNull().count(), rows / 2);
        Geometry point(GetThreadLocalGEOSContext(), "POINT(23 23)");
        ExpectHits(
            spatial->Candidates(SpatialOp::Intersects, point), rows, {23});
        ExpectNativeReads();
        reader.reset();
        EXPECT_TRUE(std::filesystem::is_empty(staging_));
    }
}

TEST_F(LegacyIndexLoadTest, JsonProjectedSlicedOffsetsRemainDistinct) {
    FILE_SLICE_SIZE.store(64);
    constexpr size_t rows = 30;
    const auto& backend =
        ScalarReaderBackends().Get<double>("JsonProjectedInvertedDouble");
    enum class OffsetProfile {
        MissingPathOnly,
        PresentInvalidOnly,
        NullParent,
    };
    // A NULL parent is both non-existent at /a and null in the typed index.
    // An existing path with an invalid typed value has only a null offset.
    for (const auto profile : {OffsetProfile::MissingPathOnly,
                               OffsetProfile::PresentInvalidOnly,
                               OffsetProfile::NullParent}) {
        SCOPED_TRACE(static_cast<int>(profile));
        const bool has_non_exist = profile != OffsetProfile::PresentInvalidOnly;
        const bool has_nulls = profile != OffsetProfile::MissingPathOnly;
        ScalarTestData<double> data(std::vector<double>(rows, 1.0));
        std::vector<size_t> missing;
        for (size_t row = 0; row < 20; ++row) {
            if (has_non_exist) {
                missing.push_back(row);
            }
            if (has_nulls) {
                data.validity.reset(row);
            }
        }
        BackendCaseMetadata metadata{
            .row_count = rows, .values = {{"non_exist_offsets", missing}}};
        const ScalarTestInput<double> input(data);
        Persist(*backend.Build(input.View(), metadata));
        ASSERT_NE(Object(INDEX_FILE_SLICE_META), nullptr);
        std::vector<const char*> sliced_sidecars;
        if (has_non_exist) {
            sliced_sidecars.push_back(INDEX_NON_EXIST_OFFSET_FILE_NAME);
        }
        if (has_nulls) {
            sliced_sidecars.push_back(INDEX_NULL_OFFSET);
        }
        for (const auto* sidecar : sliced_sidecars) {
            ASSERT_NE(Object(std::string(sidecar) + "_0"), nullptr);
            ASSERT_NE(Object(std::string(sidecar) + "_1"), nullptr);
        }
        for (bool mmap : {false, true}) {
            SCOPED_TRACE(mmap);
            auto options = Options(backend.LoadParams(metadata), mmap);
            auto source = std::shared_ptr<storage::V1RemoteSource>(
                Run(storage::V1RemoteSource::OpenAsync(
                    context_,
                    paths_,
                    options,
                    storage::ArtifactStorageNamespace::Index,
                    storage::V1SourceLayout::DiskFiles)));
            options.params = AnnotateJsonProjectionCompleteness(
                std::move(options.params), *source);
            auto reader =
                LoaderRegistry::Instance()
                    .Lookup(ResolveLoadFamily(backend.Family(), *source))
                    .Load({OpenedIndexSource{LegacyIndexSource{source, true}},
                           options});
            ASSERT_NE(reader, nullptr);
            EXPECT_EQ(reader->Count(), rows);
            const auto* json =
                dynamic_cast<const IJsonIndexReader*>(reader.get());
            ASSERT_NE(json, nullptr);
            const auto exists = json->Exists("/a");
            ASSERT_EQ(exists.size(), rows);
            for (size_t row = 0; row < rows; ++row) {
                EXPECT_EQ(exists[row], !has_non_exist || row >= 20)
                    << "Exists row=" << row;
            }
            {
                auto resolved =
                    json->Resolve("/a", JsonCastType::FromString("DOUBLE"));
                ASSERT_TRUE(resolved);
                const auto* nulls =
                    dynamic_cast<const INullReader*>(resolved.get());
                ASSERT_NE(nulls, nullptr);
                const auto is_null = nulls->IsNull();
                ASSERT_EQ(is_null.size(), rows);
                for (size_t row = 0; row < rows; ++row) {
                    EXPECT_EQ(is_null[row], has_nulls && row < 20)
                        << "IsNull row=" << row;
                }
            }
            for (const auto* sidecar : sliced_sidecars) {
                EXPECT_FALSE(Object(std::string(sidecar) + "_0")
                                 ->DirectReadCalls()
                                 .empty());
                EXPECT_FALSE(Object(std::string(sidecar) + "_1")
                                 ->DirectReadCalls()
                                 .empty());
            }
            reader.reset();
            EXPECT_TRUE(std::filesystem::is_empty(staging_));
        }
    }
}

TEST_F(LegacyIndexLoadTest, ReversedSliceCompletionPreservesEveryRow) {
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
        std::vector<std::string> values;
        for (int i = 0; i < 32; ++i)
            values.push_back("row_" + std::to_string(i) +
                             std::string(40, 'a' + i % 26));
        ScalarTestData<std::string_view> data(values);
        data.validity.reset(17);
        const ScalarTestInput<std::string_view> input(data);
        Persist(*backend.Build(input.View(), {.row_count = values.size()}));
        auto options =
            Options(backend.LoadParams({.row_count = values.size()}), mmap);
        std::shared_ptr<storage::V1RemoteSource> source =
            Run(storage::V1RemoteSource::OpenAsync(
                context_,
                paths_,
                options,
                storage::ArtifactStorageNamespace::Index,
                storage::V1SourceLayout::MemoryEntries));
        // Finish inspection before gating the concurrently dispatched payloads.
        Run(source->InspectLoadBytesAsync(source->EntryNames()));
        auto first = Object("index_data_0");
        auto second = Object("index_data_1");
        ASSERT_NE(first, nullptr);
        ASSERT_NE(second, nullptr);
        for (const auto& file : {first, second}) {
            file->ResetCounters();
            file->SetAutoComplete(false);
        }
        auto load = std::async(std::launch::async, [&] {
            return LoaderRegistry::Instance()
                .Lookup(backend.Family())
                .Load({OpenedIndexSource{LegacyIndexSource{source, true}},
                       options});
        });
        auto drain = folly::makeGuard([&] {
            for (const auto& file : {first, second}) {
                file->SetAutoComplete(true);
                for (size_t i = 0; i < file->DirectReadCalls().size(); ++i)
                    file->Complete(i);
            }
            if (load.valid())
                load.wait();
        });
        ASSERT_TRUE(first->WaitForCallCount(1));
        ASSERT_TRUE(second->WaitForCallCount(1));
        second->Complete(0);
        EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        first->Complete(0);
        auto reader = load.get();
        drain.dismiss();
        const auto* lookup =
            dynamic_cast<const IScalarValueReader<std::string_view>*>(
                reader.get());
        const auto* scalar =
            dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
                reader.get());
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(lookup, nullptr);
        ASSERT_NE(scalar, nullptr);
        ASSERT_NE(nulls, nullptr);
        for (size_t row = 0; row < values.size(); ++row) {
            SCOPED_TRACE(row);
            const std::string_view term = values[row];
            if (row == 17) {
                EXPECT_FALSE(lookup->Lookup(row).has_value());
                ExpectHits(scalar->In(1, &term), values.size(), {});
            } else {
                EXPECT_EQ(lookup->Lookup(row), values[row]);
                ExpectHits(scalar->In(1, &term), values.size(), {row});
            }
        }
        ExpectHits(nulls->IsNull(), values.size(), {17});
        ExpectNativeReads();
        reader.reset();
        EXPECT_TRUE(std::filesystem::is_empty(staging_));
    }
}

TEST_F(LegacyIndexLoadTest, MidReadCancellationDrainsThenRetriesSameLoader) {
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        PersistSorted();
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
        auto options = Options(backend.LoadParams({.row_count = 4}), mmap);
        auto source = Run(storage::V1RemoteSource::OpenAsync(
            context_,
            paths_,
            options,
            storage::ArtifactStorageNamespace::Index,
            storage::V1SourceLayout::MemoryEntries));
        Run(source->InspectLoadBytesAsync(source->EntryNames()));
        auto loader = Run(
            LoaderRegistry::Instance()
                .Lookup(backend.Family())
                .create(
                    OpenedIndexSource{LegacyIndexSource{
                        std::shared_ptr<storage::FileSource>(std::move(source)),
                        true}},
                    options));
        auto file = DataFile();
        ASSERT_NE(file, nullptr);
        file->ResetCounters();
        file->SetAutoComplete(false);
        folly::CancellationSource cancel;
        OpContext interrupted(cancel.getToken());
        auto load = std::async(std::launch::async,
                               [&] { return Run(loader->Load(&interrupted)); });
        auto drain = folly::makeGuard([&] {
            cancel.requestCancellation();
            file->SetAutoComplete(true);
            for (size_t i = 0; i < file->DirectReadCalls().size(); ++i)
                file->Complete(i);
            if (load.valid())
                load.wait();
        });
        ASSERT_TRUE(file->WaitForCallCount(1));
        cancel.requestCancellation();
        EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
                  std::future_status::timeout);
        file->Complete(0);
        ExpectError(FollyCancel, [&] { load.get(); });
        drain.dismiss();
        EXPECT_TRUE(std::filesystem::is_empty(staging_));
        file->SetAutoComplete(true);
        OpContext fresh;
        auto reader = Run(loader->Load(&fresh));
        const auto* pattern =
            dynamic_cast<const IPatternMatchReader*>(reader.get());
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        const auto* lookup =
            dynamic_cast<const IScalarValueReader<std::string_view>*>(
                reader.get());
        ASSERT_NE(pattern, nullptr);
        ASSERT_NE(nulls, nullptr);
        ASSERT_NE(lookup, nullptr);
        ExpectHits(
            pattern->PatternMatch("beta", PatternOp::PrefixMatch), 4, {1, 2});
        ExpectHits(nulls->IsNull(), 4, {3});
        EXPECT_EQ(lookup->Lookup(0), "alpha");
        EXPECT_EQ(lookup->Lookup(1), "beta");
        EXPECT_EQ(lookup->Lookup(2), "beta");
        EXPECT_FALSE(lookup->Lookup(3).has_value());
        EXPECT_GT(file->DirectReadCalls().size(), 1);
        loader.reset();
        reader.reset();
        EXPECT_TRUE(std::filesystem::is_empty(staging_));
    }
}

TEST_F(LegacyIndexLoadTest, VectorHeapAndMmapPreserveNullableAndAllNullRows) {
    for (bool all_null : {false, true}) {
        for (bool mmap : {false, true}) {
            SCOPED_TRACE(all_null);
            SCOPED_TRACE(mmap);
            auto adapted = AdaptIndexType({
                .index_type = "FLAT",
                .field_type = DataType::VECTOR_FLOAT,
                .element_type = DataType::NONE,
                .index_engine_version =
                    knowhere::Version::GetCurrentVersion().VersionNumber(),
                .params = {{METRIC_TYPE, "L2"}, {DIM_KEY, 4}},
            });
            adapted.params["nullable"] = true;
            adapted.params[INDEX_NUM_ROWS_KEY] = 4;
            auto builder =
                BuilderRegistry<VectorBuildInput<float>>::Instance().Create(
                    adapted.family, adapted.params);
            ASSERT_NE(builder, nullptr);
            const std::array<bool, 4> validity{
                !all_null, false, !all_null, !all_null};
            const std::vector<float> values =
                all_null
                    ? std::vector<float>{}
                    : std::vector<float>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
            const VectorBuildInput<float> input{
                .physical_values = values,
                .logical_rows = 4,
                .physical_rows = all_null ? 0 : 3,
                .dim = 4,
                .parent_validity = ValidityView::FromExpanded(validity.data()),
            };
            Persist(*std::move(*builder).Build(input));
            auto options = Options(adapted.params, mmap);
            LoadResourceRequest base{};
            base.final_memory_cost = 128;
            base.max_memory_cost = 128;
            auto source = Run(
                storage::V1RemoteSource::OpenAsync(context_, paths_, options));
            const auto estimate = Run(LegacyVectorFileLoadResourceAsync(
                base, false, options, *source));
            EXPECT_EQ(estimate.final_memory_cost, base.final_memory_cost);
            EXPECT_GT(estimate.max_memory_cost,
                      base.max_memory_cost + values.size() * sizeof(float));

            auto reader =
                LoaderRegistry::Instance()
                    .Lookup(adapted.family)
                    .Load({IndexFiles{
                               context_,
                               paths_,
                               LegacyIndexStorageConfig{
                                   storage::V1SourceLayout::MemoryEntries}},
                           options});
            const auto* vectors =
                dynamic_cast<const IVectorReader*>(reader.get());
            ASSERT_NE(vectors, nullptr);
            EXPECT_EQ(vectors->ValidCount(), all_null ? 0 : 3);
            for (int64_t row = 0; row < 4; ++row) {
                EXPECT_EQ(vectors->IsRowValid(row), validity[row]);
            }
            if (!all_null) {
                int64_t id = 2;
                const auto actual = vectors->GetVector(GenIdsDataset(1, &id));
                ASSERT_EQ(actual.size(), 4 * sizeof(float));
                EXPECT_EQ(std::memcmp(
                              actual.data(), values.data() + 4, actual.size()),
                          0);
            }
            ExpectNativeReads();
            reader.reset();
            EXPECT_TRUE(std::filesystem::is_empty(staging_));
        }
    }
}

TEST_F(LegacyIndexLoadTest, EmptyEmbeddingListsKeepValidParentRows) {
    auto adapted = AdaptIndexType({
        .index_type = "FLAT",
        .field_type = DataType::VECTOR_ARRAY,
        .element_type = DataType::VECTOR_FLOAT,
        .index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber(),
        .params = {{METRIC_TYPE, "MAX_SIM_L2"}, {DIM_KEY, 4}},
    });
    adapted.params["nullable"] = true;
    adapted.params[INDEX_NUM_ROWS_KEY] = 3;
    auto builder = BuilderRegistry<VectorBuildInput<float>>::Instance().Create(
        adapted.family, adapted.params);
    ASSERT_NE(builder, nullptr);
    const std::array<bool, 3> validity{true, false, true};
    const std::array<size_t, 3> offsets{0, 0, 0};
    const VectorBuildInput<float> input{
        .physical_values = {},
        .logical_rows = 3,
        .physical_rows = 0,
        .dim = 4,
        .parent_validity = ValidityView::FromExpanded(validity.data()),
        .embedding_offsets = std::span<const size_t>(offsets),
    };
    Persist(*std::move(*builder).Build(input));
    for (bool mmap : {false, true}) {
        auto options = Options(adapted.params, mmap);
        LoadResourceRequest base{};
        base.final_memory_cost = 64;
        base.max_memory_cost = 64;
        auto source =
            Run(storage::V1RemoteSource::OpenAsync(context_, paths_, options));
        const auto estimate = Run(
            LegacyVectorFileLoadResourceAsync(base, false, options, *source));
        EXPECT_EQ(estimate.final_memory_cost, 64);
        EXPECT_GT(estimate.max_memory_cost,
                  64 + offsets.size() * sizeof(size_t));
        auto reader =
            LoaderRegistry::Instance()
                .Lookup(adapted.family)
                .Load({IndexFiles{context_,
                                  paths_,
                                  LegacyIndexStorageConfig{
                                      storage::V1SourceLayout::MemoryEntries}},
                       options});
        const auto* vectors = dynamic_cast<const IVectorReader*>(reader.get());
        ASSERT_NE(vectors, nullptr);
        EXPECT_EQ(vectors->ValidCount(), 2);
        EXPECT_TRUE(vectors->IsRowValid(0));
        EXPECT_FALSE(vectors->IsRowValid(1));
        EXPECT_TRUE(vectors->IsRowValid(2));
        EXPECT_EQ(vectors->Dim(), 4);
    }
    ExpectNativeReads();
    EXPECT_TRUE(std::filesystem::is_empty(staging_));
}

// Before element-level metrics became ordinary vector input, a VECTOR_ARRAY
// field indexed under an element-level metric (L2, IP, COSINE) was built in
// embedding-list mode: the engine received the list offsets and the
// row-domain validity of a nullable field, knowhere indexed every element and
// kept an element-domain identity map, and a segment whose lists were all
// empty persisted only the empty-list offsets. Such indexes must still load
// through the production loader and answer element-level searches.
TEST_F(LegacyIndexLoadTest,
       LegacyElementLevelEmbeddingListIndexLoadsAndSearches) {
    constexpr int64_t dim = 4;
    // An embedding-list engine metric reproduces the old build mode; the
    // knowhere build itself still runs under the element-level metric from
    // the build params, as it did then.
    auto build_legacy = [&](const Config& params,
                            const VectorBuildInput<float>& input) {
        VectorMemBuilder<float> builder(
            DataType::VECTOR_FLOAT,
            params.at(INDEX_TYPE).get<std::string>(),
            knowhere::metric::MAX_SIM_L2,
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            dim,
            params);
        Persist(*std::move(builder).Build(input));
    };
    auto load =
        [&](const Config& params, bool mmap, const std::string& family) {
            return LoaderRegistry::Instance().Lookup(family).Load(
                {IndexFiles{context_,
                            paths_,
                            LegacyIndexStorageConfig{
                                storage::V1SourceLayout::MemoryEntries}},
                 Options(params, mmap)});
        };
    const std::vector<float> query{0, 1, 0, 0};
    auto search = [&](const IVectorReader& vectors, const std::string& metric) {
        VectorSearchParams params{
            .search_params_ = {{knowhere::indexparam::EF, 16}},
            .metric_type_ = metric,
            .topk_ = 3,
        };
        SearchResult result;
        vectors.Search(knowhere::GenDataSet(1, dim, query.data()),
                       params,
                       BitsetView{},
                       nullptr,
                       result);
        return result;
    };

    for (const std::string metric : {"L2", "IP", "COSINE"}) {
        for (bool nullable : {false, true}) {
            for (bool empty : {false, true}) {
                for (bool mmap : {false, true}) {
                    SCOPED_TRACE(metric);
                    SCOPED_TRACE(nullable);
                    SCOPED_TRACE(empty);
                    SCOPED_TRACE(mmap);
                    auto adapted = AdaptIndexType({
                        .index_type = "HNSW",
                        .field_type = DataType::VECTOR_ARRAY,
                        .element_type = DataType::VECTOR_FLOAT,
                        .index_engine_version =
                            knowhere::Version::GetCurrentVersion()
                                .VersionNumber(),
                        .params = {{METRIC_TYPE, metric},
                                   {DIM_KEY, dim},
                                   {knowhere::indexparam::HNSW_M, 8},
                                   {knowhere::indexparam::EFCONSTRUCTION, 16}},
                    });
                    adapted.params["nullable"] = nullable;
                    adapted.params[INDEX_NUM_ROWS_KEY] = 4;

                    // Rows: [e0, e1], [], null (or [] when not nullable),
                    // [e2]. The empty variant keeps every list empty.
                    const std::array<bool, 4> validity{
                        true, true, !nullable, true};
                    const std::vector<float> values =
                        empty ? std::vector<float>{}
                              : std::vector<float>{
                                    1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0};
                    std::vector<size_t> offsets;
                    if (empty) {
                        offsets.assign(nullable ? 4 : 5, 0);
                    } else if (nullable) {
                        offsets = {0, 2, 2, 3};
                    } else {
                        offsets = {0, 2, 2, 2, 3};
                    }
                    const VectorBuildInput<float> input{
                        .physical_values = values,
                        .logical_rows = 4,
                        .physical_rows = empty ? 0 : 3,
                        .dim = dim,
                        .parent_validity =
                            nullable
                                ? ValidityView::FromExpanded(validity.data())
                                : ValidityView{},
                        .embedding_offsets = std::span<const size_t>(offsets),
                    };
                    build_legacy(adapted.params, input);

                    auto reader = load(adapted.params, mmap, adapted.family);
                    const auto* vectors =
                        dynamic_cast<const IVectorReader*>(reader.get());
                    ASSERT_NE(vectors, nullptr);
                    EXPECT_EQ(vectors->Dim(), dim);
                    EXPECT_EQ(reader->Count(), empty ? 0 : 3);

                    const auto result = search(*vectors, metric);
                    EXPECT_EQ(result.total_nq_, 1);
                    ASSERT_EQ(result.seg_offsets_.size(), 3);
                    if (empty) {
                        for (const auto offset : result.seg_offsets_) {
                            EXPECT_EQ(offset, INVALID_SEG_OFFSET);
                        }
                    } else {
                        // Element ids index the stored vectors in order.
                        EXPECT_EQ(result.seg_offsets_[0], 1);
                        EXPECT_NEAR(
                            result.distances_[0], metric == "L2" ? 0 : 1, 1e-5);
                        std::set<int64_t> ids(result.seg_offsets_.begin(),
                                              result.seg_offsets_.end());
                        EXPECT_EQ(ids, (std::set<int64_t>{0, 1, 2}));
                    }
                    reader.reset();
                    EXPECT_TRUE(std::filesystem::is_empty(staging_));
                }
            }
        }
    }
}

TEST_F(LegacyIndexLoadTest,
       CancellationBeforeQueuedInitializationReadsNoPayload) {
    PersistSorted();
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
    folly::CancellationSource cancel;
    OpContext op(cancel.getToken());
    auto options = Options(backend.LoadParams({.row_count = 4}), true);
    options.op_ctx = &op;
    std::shared_ptr<storage::V1RemoteSource> source =
        Run(storage::V1RemoteSource::OpenAsync(
            context_,
            paths_,
            options,
            storage::ArtifactStorageNamespace::Index,
            storage::V1SourceLayout::MemoryEntries));
    for (const auto& [path, file] : fs_->files) file->ResetCounters();
    const auto old_threads = storage::GetAsyncLoadThreadPoolSize();
    storage::SetAsyncLoadThreadPoolSize(1);
    auto restore_threads = folly::makeGuard(
        [&] { storage::SetAsyncLoadThreadPoolSize(old_threads); });
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future();
    storage::ResolveAsyncLoadExecutor({}, proto::common::LoadPriority::HIGH)
        ->add([&] {
            entered.set_value();
            released.wait();
        });
    entered.get_future().get();
    auto load = std::async(std::launch::async, [&] {
        return LoaderRegistry::Instance()
            .Lookup(backend.Family())
            .Load(
                {OpenedIndexSource{LegacyIndexSource{source, true}}, options});
    });
    auto drain = folly::makeGuard([&] {
        cancel.requestCancellation();
        release.set_value();
        load.wait();
    });
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    cancel.requestCancellation();
    release.set_value();
    drain.dismiss();
    ExpectError(FollyCancel, [&] { load.get(); });
    for (const auto& [path, file] : fs_->files)
        EXPECT_TRUE(file->DirectReadCalls().empty());
    EXPECT_TRUE(std::filesystem::is_empty(staging_));
}

TEST_F(LegacyIndexLoadTest, DisabledAsyncUsesLegacySynchronousTransport) {
    PersistSorted();
    context_.use_async_load = false;
    context_.fs = local_fs_;
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
    auto options = Options(backend.LoadParams({.row_count = 4}), true);
    auto reader =
        LoaderRegistry::Instance()
            .Lookup(backend.Family())
            .Load({IndexFiles{context_,
                              paths_,
                              LegacyIndexStorageConfig{
                                  storage::V1SourceLayout::MemoryEntries}},
                   options});
    const auto* pattern =
        dynamic_cast<const IPatternMatchReader*>(reader.get());
    ASSERT_NE(pattern, nullptr);
    ExpectHits(
        pattern->PatternMatch("beta", PatternOp::PrefixMatch), 4, {1, 2});
    for (const auto& [path, file] : fs_->files)
        EXPECT_TRUE(file->DirectReadCalls().empty());
    reader.reset();
    EXPECT_TRUE(std::filesystem::is_empty(staging_));
}

TEST_F(LegacyIndexLoadTest,
       LegacyBitmapMmapEstimateAccountsForResidentBitsets) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("BitmapVarchar");
    ScalarTestData<std::string_view> data({"alpha", "beta", "beta", "null"});
    data.validity.reset(3);
    const ScalarTestInput<std::string_view> input(data);
    Persist(*backend.Build(input.View(), {.row_count = 4}));
    const std::map<std::string, std::string> params{
        {INDEX_TYPE, BITMAP_INDEX_TYPE}, {SCALAR_INDEX_ENGINE_VERSION, "2"}};
    auto source = Run(storage::V1RemoteSource::OpenAsync(context_, paths_, {}));
    const auto heap = Run(LegacyScalarIndexLoadResourceAsync(
        DataType::VARCHAR, PhysicalBytes(), params, false, 4, *source));
    const auto mmap = Run(LegacyScalarIndexLoadResourceAsync(
        DataType::VARCHAR, PhysicalBytes(), params, true, 4, *source));
    // Two resident value bitsets plus validity and per-value bookkeeping.
    EXPECT_GE(mmap.final_memory_cost,
              3 * sizeof(uint64_t) + 2 * 128 + kScalarIndexFixedResidentBytes);
    EXPECT_EQ(mmap.final_memory_cost, heap.final_memory_cost);
    EXPECT_EQ(mmap.final_disk_cost, 0);
    EXPECT_EQ(mmap.max_memory_cost, heap.max_memory_cost);
    EXPECT_GT(mmap.max_memory_cost, mmap.final_memory_cost);
}

TEST_F(LegacyIndexLoadTest, NullableTantivyMmapEstimateRetainsNullSidecar) {
    for (const auto* profile :
         {"InvertedVarchar", "NgramVarcharMin2Max4Heap"}) {
        SCOPED_TRACE(profile);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(profile);
        ScalarTestData<std::string_view> data(
            {"alpha", "beta", "beta", "null"});
        data.validity.reset(3);
        const ScalarTestInput<std::string_view> input(data);
        Persist(*backend.Build(input.View(), {.row_count = 4}));
        const bool ngram = backend.Family() == families::kNgram;
        const std::map<std::string, std::string> params{
            {INDEX_TYPE, ngram ? NGRAM_INDEX_TYPE : INVERTED_INDEX_TYPE},
            {SCALAR_INDEX_ENGINE_VERSION, "2"}};
        auto source = Run(storage::V1RemoteSource::OpenAsync(
            context_,
            paths_,
            {},
            storage::ArtifactStorageNamespace::Index,
            storage::V1SourceLayout::DiskFiles));
        const auto estimate = Run(LegacyScalarIndexLoadResourceAsync(
            DataType::VARCHAR, PhysicalBytes(), params, true, 4, *source));
        // V1/V2 ngram uses its built-in average-row-size default; only V3
        // persists that POD. Both legacy families retain one validity word
        // and the single null row's native-size_t offset.
        if (ngram) {
            EXPECT_EQ(Object("ngram_avg_row_size"), nullptr);
        }
        EXPECT_EQ(
            estimate.final_memory_cost,
            sizeof(uint64_t) + sizeof(size_t) + kScalarIndexFixedResidentBytes);
        EXPECT_GT(estimate.max_memory_cost, estimate.final_memory_cost);
    }
}

TEST_F(LegacyIndexLoadTest,
       DiskEstimateSelectsNativeBackendBeforeReadingEngineObjects) {
    PersistSorted();
    auto adapted = AdaptIndexType({
        .index_type = "DISKANN",
        .field_type = DataType::VECTOR_FLOAT,
        .element_type = DataType::NONE,
        .index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber(),
        .params = {{METRIC_TYPE, "L2"}, {DIM_KEY, 4}},
    });
    ASSERT_EQ(adapted.family, families::kVectorDisk);
    auto options = Options(adapted.params, false);
    auto source = Run(storage::V1RemoteSource::OpenAsync(
        context_,
        paths_,
        options,
        storage::ArtifactStorageNamespace::Index,
        storage::V1SourceLayout::DiskFiles));
    const auto names = source->EntryNames();
    auto handle = source->OpenDiskEngineFiles(
        storage::DiskEngineFileMode::LocalFiles, names);
    auto pack = knowhere::Pack(handle->Manager());
    KnowhereEngine engine(
        DataType::VECTOR_FLOAT,
        DataType::NONE,
        "DISKANN",
        "L2",
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        handle,
        pack,
        true);
    const bool streams = engine.native_index.LoadIndexWithStream();
    const auto eager = VectorDiskLoader::AsyncEntryNames(*source, options);
    EXPECT_EQ(eager, streams ? std::vector<std::string>{} : names);
    for (const auto& [path, file] : fs_->files) file->ResetCounters();
    const auto estimate =
        Run(LegacyVectorFileLoadResourceAsync({}, true, options, *source));
    EXPECT_GT(estimate.max_memory_cost, 0);
    for (const auto& [path, file] : fs_->files) {
        if (std::filesystem::path(path).filename() == INDEX_FILE_SLICE_META)
            continue;
        if (streams) {
            EXPECT_TRUE(file->DirectReadCalls().empty());
            EXPECT_FALSE(file->WaitForSizeCall(std::chrono::milliseconds(30)));
        } else {
            EXPECT_FALSE(file->DirectReadCalls().empty());
        }
    }
}

TEST_F(LegacyIndexLoadTest, SliceMetadataCorruptionStopsBeforeEngineReads) {
    FILE_SLICE_SIZE.store(16);
    PersistSorted();
    auto metadata = Object(INDEX_FILE_SLICE_META);
    ASSERT_NE(metadata, nullptr);
    metadata->CorruptRemoteByte(0);
    ExpectError(DataFormatBroken, [&] {
        Run(storage::V1RemoteSource::OpenAsync(
            context_,
            paths_,
            {},
            storage::ArtifactStorageNamespace::Index,
            storage::V1SourceLayout::MemoryEntries));
    });
    for (const auto& [path, file] : fs_->files) {
        if (file != metadata)
            EXPECT_TRUE(file->DirectReadCalls().empty());
    }
}

TEST_F(LegacyIndexLoadTest,
       SliceMetadataCancellationDrainsBeforeSourceConstruction) {
    FILE_SLICE_SIZE.store(16);
    PersistSorted();
    auto metadata = Object(INDEX_FILE_SLICE_META);
    ASSERT_NE(metadata, nullptr);
    metadata->SetAutoComplete(false);
    folly::CancellationSource cancel;
    OpContext op(cancel.getToken());
    storage::LoadOptions options;
    options.op_ctx = &op;
    auto open = std::async(std::launch::async, [&] {
        return Run(storage::V1RemoteSource::OpenAsync(
            context_,
            paths_,
            options,
            storage::ArtifactStorageNamespace::Index,
            storage::V1SourceLayout::MemoryEntries));
    });
    auto drain = folly::makeGuard([&] {
        cancel.requestCancellation();
        if (metadata->WaitForCallCount(1))
            metadata->Complete(0);
        open.wait();
    });
    ASSERT_TRUE(metadata->WaitForCallCount(1));
    cancel.requestCancellation();
    EXPECT_EQ(open.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    metadata->Complete(0);
    ExpectError(FollyCancel, [&] { open.get(); });
    drain.dismiss();
    for (const auto& [path, file] : fs_->files) {
        if (file != metadata)
            EXPECT_TRUE(file->DirectReadCalls().empty());
    }
}

TEST_F(LegacyIndexLoadTest,
       SecondPublicationFailureRestoresBothExistingDestinations) {
    PersistSorted();
    auto source = Run(storage::V1RemoteSource::OpenAsync(
        context_,
        paths_,
        {},
        storage::ArtifactStorageNamespace::Index,
        storage::V1SourceLayout::MemoryEntries));
    const std::vector<std::string> names{"index_data", "valid_bitset"};
    Run(source->InspectLoadBytesAsync(names));
    auto second = Object(names[1]);
    ASSERT_NE(second, nullptr);
    const auto second_bytes = second->GetSize().ValueOrDie();
    second->ResetCounters();
    second->SetAutoComplete(false);
    for (const auto& name : names)
        std::ofstream(staging_ + "/" + name) << "old " << name;
    auto materialize = std::async(std::launch::async, [&] {
        return Run(source->ReadEntriesToLocalDirAsync(names, staging_));
    });
    auto drain = folly::makeGuard([&] {
        second->SetAutoComplete(true);
        if (second->WaitForCallCount(1))
            second->Complete(0);
        materialize.wait();
    });
    ASSERT_TRUE(second->WaitForCallCount(1));
    // Cached inspection means this reads the complete encoded payload, after
    // Prepare opened the second target. The writer truncates to zero initially;
    // it sets the final size in Finish, after this blocked read/write.
    ASSERT_EQ(second->DirectReadCalls().front().nbytes, second_bytes);
    // The first staging file is already complete and nonempty. Remove only
    // the second, still-empty staging path to fail its publication rename.
    size_t removed = 0;
    for (const auto& entry : std::filesystem::directory_iterator(staging_)) {
        if (entry.path().filename().string().starts_with(".milvus-artifact-") &&
            entry.file_size() == 0) {
            removed += std::filesystem::remove(entry.path());
        }
    }
    ASSERT_EQ(removed, 1);
    second->Complete(0);
    ExpectError(FileWriteFailed, [&] { materialize.get(); });
    drain.dismiss();
    for (const auto& name : names) {
        std::ifstream input(staging_ + "/" + name);
        const std::string contents((std::istreambuf_iterator<char>(input)), {});
        EXPECT_EQ(contents, "old " + name);
    }
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(staging_),
                            std::filesystem::directory_iterator()),
              2);
}

TEST_F(LegacyIndexLoadTest, CorruptOrFailedReadKeepsCodeAndRemovesMmapStaging) {
    for (bool corrupt : {false, true}) {
        PersistSorted();
        auto file = DataFile();
        ASSERT_NE(file, nullptr);
        if (corrupt) {
            file->CorruptRemoteByte(0);
        } else {
            file->SetNextCompletion(
                arrow::Status::IOError("injected legacy I/O failure"));
        }
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
        const auto options =
            Options(backend.LoadParams({.row_count = 4}), true);
        // An unclassified Arrow IOError maps to StorageError; FileReadFailed
        // is reserved for storage errors carrying that specific category.
        ExpectError(corrupt ? DataFormatBroken : StorageError, [&] {
            LoaderRegistry::Instance()
                .Lookup(backend.Family())
                .Load({IndexFiles{context_,
                                  paths_,
                                  LegacyIndexStorageConfig{
                                      storage::V1SourceLayout::MemoryEntries}},
                       options});
        });
        EXPECT_TRUE(std::filesystem::is_empty(staging_));
    }
}

TEST_F(LegacyIndexLoadTest, CancellationDrainsRemoteReadBeforeRemovingStaging) {
    PersistSorted();
    auto file = DataFile();
    ASSERT_NE(file, nullptr);
    file->SetAutoComplete(false);
    folly::CancellationSource cancel;
    OpContext op;
    op.cancellation_token = cancel.getToken();
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
    auto options = Options(backend.LoadParams({.row_count = 4}), true);
    options.op_ctx = &op;
    auto load = std::async(std::launch::async, [&] {
        return LoaderRegistry::Instance()
            .Lookup(backend.Family())
            .Load({IndexFiles{context_,
                              paths_,
                              LegacyIndexStorageConfig{
                                  storage::V1SourceLayout::MemoryEntries}},
                   options});
    });
    auto drain = folly::makeGuard([&] {
        cancel.requestCancellation();
        if (file->WaitForCallCount(1))
            file->Complete(0);
        load.wait();
    });
    ASSERT_TRUE(file->WaitForCallCount(1));
    cancel.requestCancellation();
    EXPECT_EQ(load.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    file->Complete(0);
    ExpectError(FollyCancel, [&] { load.get(); });
    drain.dismiss();
    EXPECT_TRUE(std::filesystem::is_empty(staging_));
}
}  // namespace
}  // namespace milvus::index::test
