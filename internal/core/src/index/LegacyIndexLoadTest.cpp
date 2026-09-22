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
#include <future>
#include <fstream>
#include <map>
#include "arrow/filesystem/localfs.h"
#include "common/Geometry.h"
#include "common/OpContext.h"
#include "common/Slice.h"
#include "folly/ScopeGuard.h"
#include "index/IndexTypeAdapter.h"
#include "index/scalar/bitmap/BitmapIndexLoader.h"
#include "index/LegacyIndexLoad.h"
#include "index/IndexLoaderFactory.h"
#include "index/LoadResource.h"
#include "index/vector/KnowhereEngine.h"
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
#include "index/contracts/query/IVectorReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"
#include "knowhere/version.h"
#include "storage/LocalFileIOPool.h"
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

// Counts actual sync transport bytes; size inspection must not fetch payloads.
class CountingLegacyChunkManager final : public storage::LocalChunkManager {
 public:
    using LocalChunkManager::LocalChunkManager;
    uint64_t
    Read(const std::string& path, void* data, uint64_t bytes) override {
        ++whole_reads;
        return LocalChunkManager::Read(path, data, bytes);
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
};

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

TEST_F(LegacyIndexLoadTest, OpenKeepsMetadataForFreshLoadContexts) {
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
        const auto entry = LoaderRegistry::Instance().Lookup(backend.Family());
        auto loader = Run(entry.open(
            {IndexFiles{
                 context_,
                 paths_,
                 LegacyIndexFiles{storage::V1SourceLayout::MemoryEntries}},
             options}));
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

TEST_F(LegacyIndexLoadTest, FailedOpenDetachesBorrowedSourceContext) {
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
            storage::ArtifactStoragePath::Index,
            storage::V1SourceLayout::MemoryEntries)));
    folly::CancellationSource opening;
    OpContext context(opening.getToken());
    options.op_ctx = &context;
    options.params["field_type"] = DataType::NONE;
    options.params["value_type"] = DataType::NONE;
    IndexOpenRequest request{OpenedIndexInput{LegacyIndexSource{source, true}},
                             options};
    ExpectError(DataTypeInvalid, [&] {
        static_cast<void>(Run(BitmapIndexLoader::Open(std::move(request))));
    });
    opening.requestCancellation();
    // No new loader/context bind is allowed to hide a leaked opening token.
    EXPECT_FALSE(Run(source->ReadEntryAsync(BITMAP_INDEX_META)).empty());
    EXPECT_TRUE(std::filesystem::is_empty(staging_));
}

TEST_F(LegacyIndexLoadTest, CancelledLoadDoesNotPoisonOpenedMetadata) {
    PersistSorted();
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
    auto options = Options(backend.LoadParams({.row_count = 4}), true);
    const auto entry = LoaderRegistry::Instance().Lookup(backend.Family());
    auto loader = Run(entry.open(
        {IndexFiles{context_,
                    paths_,
                    LegacyIndexFiles{storage::V1SourceLayout::MemoryEntries}},
         options}));
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
            const auto loader =
                LoaderRegistry::Instance().Lookup(backend.Family());
            auto reader = LoadIndex(
                loader,
                {IndexFiles{context_,
                            paths_,
                            LegacyIndexFiles{
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
            auto reader = LoadIndex(
                LoaderRegistry::Instance().Lookup(backend.Family()),
                {IndexFiles{
                     context_,
                     paths_,
                     LegacyIndexFiles{storage::V1SourceLayout::DiskFiles}},
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
                storage::ArtifactStoragePath::Index,
                storage::V1SourceLayout::MemoryEntries));
        // Inspection is serial; gate only the concurrently dispatched payloads.
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
            return LoadIndex(
                LoaderRegistry::Instance().Lookup(backend.Family()),
                {OpenedIndexInput{LegacyIndexSource{source, true}}, options});
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
            storage::ArtifactStoragePath::Index,
            storage::V1SourceLayout::MemoryEntries));
        Run(source->InspectLoadBytesAsync(source->EntryNames()));
        auto loader = Run(LoaderRegistry::Instance()
                              .Lookup(backend.Family())
                              .open({OpenedIndexInput{LegacyIndexSource{
                                         std::shared_ptr<storage::FileSource>(
                                             std::move(source)),
                                         true}},
                                     options}));
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
            const auto estimate = LegacyVectorFileLoadResource(
                base, false, options, paths_, context_);
            EXPECT_EQ(estimate.final_memory_cost, base.final_memory_cost);
            EXPECT_GT(estimate.max_memory_cost,
                      base.max_memory_cost + values.size() * sizeof(float));

            auto reader = LoadIndex(
                LoaderRegistry::Instance().Lookup(adapted.family),
                {IndexFiles{
                     context_,
                     paths_,
                     LegacyIndexFiles{storage::V1SourceLayout::MemoryEntries}},
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
        .params = {{METRIC_TYPE, "L2"}, {DIM_KEY, 4}},
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
        const auto estimate = LegacyVectorFileLoadResource(
            base, false, options, paths_, context_);
        EXPECT_EQ(estimate.final_memory_cost, 64);
        EXPECT_GT(estimate.max_memory_cost,
                  64 + offsets.size() * sizeof(size_t));
        auto reader = LoadIndex(
            LoaderRegistry::Instance().Lookup(adapted.family),
            {IndexFiles{
                 context_,
                 paths_,
                 LegacyIndexFiles{storage::V1SourceLayout::MemoryEntries}},
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
            storage::ArtifactStoragePath::Index,
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
        return LoadIndex(
            LoaderRegistry::Instance().Lookup(backend.Family()),
            {OpenedIndexInput{LegacyIndexSource{source, true}}, options});
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
    auto reader = LoadIndex(
        LoaderRegistry::Instance().Lookup(backend.Family()),
        {IndexFiles{context_,
                    paths_,
                    LegacyIndexFiles{storage::V1SourceLayout::MemoryEntries}},
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
    const auto heap = ScalarIndexFileLoadResource(DataType::VARCHAR,
                                                  PhysicalBytes(),
                                                  params,
                                                  false,
                                                  4,
                                                  paths_,
                                                  context_)
                          .request;
    const auto mmap = ScalarIndexFileLoadResource(DataType::VARCHAR,
                                                  PhysicalBytes(),
                                                  params,
                                                  true,
                                                  4,
                                                  paths_,
                                                  context_)
                          .request;
    // Two resident value bitsets plus validity and per-value bookkeeping.
    EXPECT_GE(mmap.final_memory_cost, 3 * sizeof(uint64_t) + 2 * 128);
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
        const auto estimate = ScalarIndexFileLoadResource(DataType::VARCHAR,
                                                          PhysicalBytes(),
                                                          params,
                                                          true,
                                                          4,
                                                          paths_,
                                                          context_)
                                  .request;
        // V1/V2 ngram uses its built-in average-row-size default; only V3
        // persists that POD. Both legacy families retain one validity word
        // and the single null row's native-size_t offset.
        if (ngram) {
            EXPECT_EQ(Object("ngram_avg_row_size"), nullptr);
        }
        EXPECT_EQ(estimate.final_memory_cost,
                  sizeof(uint64_t) + sizeof(size_t));
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
    auto source = Run(
        storage::V1RemoteSource::OpenAsync(context_,
                                           paths_,
                                           options,
                                           storage::ArtifactStoragePath::Index,
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
        LegacyVectorFileLoadResource({}, true, options, paths_, context_);
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
            storage::ArtifactStoragePath::Index,
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
            storage::ArtifactStoragePath::Index,
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
        storage::ArtifactStoragePath::Index,
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
            LoadIndex(LoaderRegistry::Instance().Lookup(backend.Family()),
                      {IndexFiles{context_,
                                  paths_,
                                  LegacyIndexFiles{
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
        return LoadIndex(
            LoaderRegistry::Instance().Lookup(backend.Family()),
            {IndexFiles{
                 context_,
                 paths_,
                 LegacyIndexFiles{storage::V1SourceLayout::MemoryEntries}},
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
