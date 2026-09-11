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
#include <mutex>
#include <numeric>
#include <type_traits>
#include <utility>
#include <future>
#include "folly/synchronization/Baton.h"
#include "common/Slice.h"
#include "folly/ScopeGuard.h"
#include "folly/system/ThreadName.h"
#include "index/BitmapIndex.h"
#include "index/HybridScalarIndex.h"
#include "index/IndexFactory.h"
#include "index/JsonScalarIndexWrapper.h"
#include "index/JsonHybridScalarIndex.h"
#include "common/Geometry.h"
#include "common/GeometryCache.h"
#include "common/Json.h"
#include "index/InvertedIndexTantivy.h"
#include "index/NgramInvertedIndex.h"
#include "index/RTreeIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/StringIndexSort.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/LegacyIndexLoader.h"
#include "storage/LocalFileIOPool.h"
#include "storage/LocalChunkManager.h"
#include "storage/Util.h"
#include "test_utils/AsyncLoadTestUtils.h"
#include "test_utils/TmpPath.h"

namespace milvus::index {
namespace {

class ObservedLegacySource : public storage::LocalChunkManager {
 public:
    using LocalChunkManager::LocalChunkManager;
    uint64_t
    Read(const std::string& path, void* data, uint64_t bytes) override {
        Observe(path, 0);
        return LocalChunkManager::Read(path, data, bytes);
    }
    uint64_t
    Read(const std::string& path,
         uint64_t offset,
         void* data,
         uint64_t bytes) override {
        Observe(path, offset);
        return LocalChunkManager::Read(path, offset, data, bytes);
    }
    void
    Observe(const std::string& path, uint64_t offset) {
        if (on_read) {
            on_read(path, offset);
        }
        if (expect_async) {
            EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
                "MILVUS_ASYNC"));
        }
    }
    bool expect_async{false};
    std::function<void(const std::string&, uint64_t)> on_read;
};

template <typename Index>
class ObservedLegacyIndex : public Index {
 public:
    using Index::Index;
    void
    ComputeByteSize() override {
        finalizer_thread = folly::getCurrentThreadName().value_or("");
        if (on_finalize) {
            on_finalize();
        }
        Index::ComputeByteSize();
    }
    std::function<void()> on_finalize;
    void
    FinishForTest() {
        this->finish();
    }
    const std::string&
    Path() const {
        return this->path_;
    }
    std::string finalizer_thread;
};

class LegacyScalarAsyncLoadTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        old_enabled_ =
            segcore::storagev2translator::StorageV2AsyncLoadEnabled();
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(true);
        old_workers_ = storage::GetAsyncLoadThreadPoolSize();
        storage::SetAsyncLoadThreadPoolSize(1);
        old_slots_ =
            storage::LoadAdmissionController::GetInstance().CapacitySlots();
        storage::LoadAdmissionController::GetInstance().SetCapacitySlots(1);
        storage::LocalFileIOPool::GetInstance().Configure(1);
        source_ =
            std::make_shared<ObservedLegacySource>(temporary_.get().string());
        old_remote_root_ =
            std::exchange(kOverrideRootPathForUT, temporary_.get().string());
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
    Context(proto::schema::DataType type, bool loading = false) {
        proto::schema::FieldSchema schema;
        schema.set_data_type(type);
        schema.set_nullable(true);
        storage::FileManagerContext result(
            storage::FieldDataMeta{1, 2, 3, 101, schema},
            storage::IndexMeta{3, 101, 91001, 2},
            source_,
            nullptr);
        result.set_for_loading_index(loading);
        return result;
    }
    std::string
    PersistBytes(const std::string& name, const uint8_t* data, size_t bytes) {
        const uint8_t empty = 0;
        storage::IndexData codec(bytes == 0 ? &empty : data, bytes);
        codec.SetFieldDataMeta(
            Context(proto::schema::DataType::VarChar).fieldDataMeta);
        codec.set_index_meta(
            Context(proto::schema::DataType::VarChar).indexMeta);
        auto encoded = codec.Serialize(storage::StorageType::Remote);
        const auto path = (temporary_.get() / "remote" / name).string();
        source_->Write(path, encoded.data(), encoded.size());
        return path;
    }
    std::vector<std::string>
    Persist(const BinarySet& binary, bool sliced = true) {
        std::vector<std::string> files;
        Config metadata;
        metadata[META] = Config::array();
        for (const auto& [name, bytes] : binary.binary_map_) {
            if (sliced && bytes->size > 1 && name != INDEX_FILE_SLICE_META) {
                const auto half = bytes->size / 2;
                files.push_back(PersistBytes(
                    GenSlicedFileName(name, 0), bytes->data.get(), half));
                files.push_back(PersistBytes(GenSlicedFileName(name, 1),
                                             bytes->data.get() + half,
                                             bytes->size - half));
                metadata[META].push_back(
                    {{NAME, name}, {SLICE_NUM, 2}, {TOTAL_LEN, bytes->size}});
            } else {
                files.push_back(
                    PersistBytes(name, bytes->data.get(), bytes->size));
            }
        }
        if (!metadata[META].empty()) {
            const auto text = metadata.dump();
            files.push_back(
                PersistBytes(INDEX_FILE_SLICE_META,
                             reinterpret_cast<const uint8_t*>(text.data()),
                             text.size()));
        }
        return files;
    }
    std::vector<std::string>
    PersistDirectory(const std::string& directory, const BinarySet& metadata) {
        auto files = Persist(metadata);
        for (const auto& entry :
             std::filesystem::directory_iterator(directory)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            std::ifstream input(entry.path(), std::ios::binary);
            std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)),
                                       {});
            const auto half = bytes.size() / 2;
            const auto name = entry.path().filename().string();
            files.push_back(PersistBytes(name + "_0", bytes.data(), half));
            files.push_back(
                PersistBytes(name + "_1",
                             bytes.empty() ? nullptr : bytes.data() + half,
                             bytes.size() - half));
        }
        return files;
    }
    Config
    LoadConfig(const std::vector<std::string>& files, bool mmap) {
        Config config{{INDEX_FILES, files},
                      {ENABLE_MMAP, mmap},
                      {SCALAR_INDEX_ENGINE_VERSION, 2},
                      {LOAD_PRIORITY, proto::common::LoadPriority::LOW}};
        if (mmap) {
            config[MMAP_FILE_PATH] =
                (temporary_.get() / "mapped" / "index").string();
        }
        return config;
    }
    template <typename T>
    void
    CheckNullable(ScalarIndex<T>& index, const T& needle) {
        EXPECT_EQ(index.Count(), 5);
        EXPECT_EQ(index.IsNull().count(), 2);
        EXPECT_EQ(index.IsNotNull().count(), 3);
        const auto hits = index.In(1, &needle);
        ASSERT_EQ(hits.size(), 5);
        EXPECT_EQ(hits.count(), 2);
        EXPECT_TRUE(hits[0]);
        EXPECT_TRUE(hits[3]);
        const auto misses = index.NotIn(1, &needle);
        EXPECT_EQ(misses.count(), 1);
        EXPECT_TRUE(misses[2]);
    }
    template <typename T, typename Index>
    void
    RoundTripBinary(const std::vector<T>& values,
                    proto::schema::DataType type) {
        const bool valid[]{true, false, true, true, false};
        Index build(Context(type));
        build.Build(values.size(), values.data(), valid);
        for (const bool sliced : {false, true}) {
            const auto files = Persist(build.Serialize({}), sliced);
            for (const bool mmap : {false, true}) {
                for (const bool enabled : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << type << '/' << sliced << '/' << mmap << '/'
                                 << enabled);
                    segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(
                        enabled);
                    source_->expect_async = enabled;
                    using Loaded = ObservedLegacyIndex<Index>;
                    Loaded loaded(Context(type, true));
                    IndexBase& base = loaded;
                    OpContext context;
                    base.Load(tracer::TraceContext{},
                              LoadConfig(files, mmap),
                              &context);
                    CheckNullable(loaded, values[0]);
                    if (enabled) {
                        EXPECT_TRUE(loaded.finalizer_thread.starts_with(
                            "MILVUS_ASYNC"));
                    }
                }
            }
        }
    }
    // Block real query-state restoration after engine opening. A single local
    // worker must remain available, and admission must already be released.
    template <typename Index>
    void
    CheckBlockedFinalizer(ObservedLegacyIndex<Index>& loaded,
                          Config config,
                          bool packed,
                          int outcome) {
        folly::Baton<> release;
        std::promise<void> entered;
        auto entered_future = entered.get_future();
        std::atomic<bool> first{true};
        loaded.on_finalize = [&] {
            EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
                "MILVUS_ASYNC"));
            if (first.exchange(false)) {
                entered.set_value();
                release.wait();
                if (outcome == 2) {
                    ThrowInfo(FileReadFailed,
                              "injected scalar state restoration failure");
                }
            }
        };
        folly::CancellationSource cancellation;
        OpContext context;
        context.cancellation_token = cancellation.getToken();
        auto pending = std::async(std::launch::async, [&] {
            if (packed) {
                loaded.LoadUnified(config, &context);
            } else {
                IndexBase& base = loaded;
                base.Load(tracer::TraceContext{}, config, &context);
            }
        });
        auto drain = folly::makeGuard([&] { release.post(); });
        ASSERT_EQ(entered_future.wait_for(std::chrono::seconds(10)),
                  std::future_status::ready);
        std::promise<void> local_probe;
        auto local_future = local_probe.get_future();
        storage::LocalFileIOPool::GetInstance().GetExecutor()->add(
            [&] { local_probe.set_value(); });
        EXPECT_EQ(local_future.wait_for(std::chrono::seconds(10)),
                  std::future_status::ready);
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const bool available =
            admission.TryAcquire({1, 1}, storage::LoadAdmissionPriority::High);
        EXPECT_TRUE(available);
        if (available) {
            admission.Release({1, 1});
        }
        if (outcome == 1) {
            cancellation.requestCancellation();
        }
        EXPECT_EQ(pending.wait_for(std::chrono::milliseconds(0)),
                  std::future_status::timeout);
        release.post();
        if (outcome == 0) {
            EXPECT_NO_THROW(pending.get());
        } else {
            try {
                pending.get();
                FAIL() << "expected load failure";
            } catch (const SegcoreError& error) {
                EXPECT_EQ(error.get_error_code(),
                          outcome == 1 ? FollyCancel : FileReadFailed);
            }
        }
        // Join the queued probe even if its deadline exposed the old routing.
        local_future.wait();
        loaded.on_finalize = {};
    }

    template <typename T, typename Index>
    void
    CheckBinaryFinalizers(const std::vector<T>& values,
                          proto::schema::DataType type) {
        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = temporary_.get().string();
        const auto fs = storage::InitArrowFileSystem(storage_config);
        auto build_context = Context(type);
        build_context.fs = fs;
        Index build(build_context);
        build.Build(values.size(), values.data());
        const auto legacy_files = Persist(build.Serialize({}));
        const auto packed_files = build.UploadUnified({})->GetIndexFiles();
        for (const bool packed : {false, true}) {
            for (const bool mmap : {false, true}) {
                for (const int outcome : {0, 1, 2}) {
                    SCOPED_TRACE(::testing::Message()
                                 << type << '/' << packed << '/' << mmap << '/'
                                 << outcome);
                    auto load_context = Context(type, true);
                    if (packed) {
                        load_context.fs = fs;
                    }
                    ObservedLegacyIndex<Index> loaded(load_context);
                    auto config =
                        LoadConfig(packed ? packed_files : legacy_files, mmap);
                    config[SCALAR_INDEX_ENGINE_VERSION] = packed ? 3 : 2;
                    CheckBlockedFinalizer(loaded, config, packed, outcome);
                    if (outcome == 0) {
                        EXPECT_EQ(loaded.Count(), values.size());
                        const T needles[]{values.front(), values.back()};
                        const auto matches = loaded.In(2, needles);
                        EXPECT_TRUE(matches[0]);
                        EXPECT_TRUE(matches[values.size() - 1]);
                    }
                    if constexpr (std::is_same_v<Index, BitmapIndex<T>>) {
                        if (mmap && outcome != 0) {
                            EXPECT_FALSE(std::filesystem::exists(
                                config[MMAP_FILE_PATH]
                                    .template get<std::string>()));
                        }
                    }
                }
            }
        }
    }

    test::TmpPath temporary_;
    std::shared_ptr<ObservedLegacySource> source_;
    test::ScopedLoadTransientBudget budget_{1024 * 1024};
    std::string old_remote_root_;
    bool old_enabled_{};
    int old_workers_{};
    size_t old_slots_{};
};

TEST_F(LegacyScalarAsyncLoadTest,
       ScalarFinalizersDoNotHoldLocalWorkerOrAdmission) {
    CheckBinaryFinalizers<int64_t, ScalarIndexSort<int64_t>>(
        {10, 20, 30, 10, 50}, proto::schema::DataType::Int64);
    CheckBinaryFinalizers<std::string, StringIndexSort>(
        {"cat", "dog", "owl", "cat", "ant"}, proto::schema::DataType::VarChar);
    CheckBinaryFinalizers<std::string, StringIndexMarisa>(
        {"cat", "dog", "owl", "cat", "ant"}, proto::schema::DataType::VarChar);
    // Enough keys for mmap mode and multiple frozen conversion batches.
    std::vector<int64_t> values(5000);
    std::iota(values.begin(), values.end(), int64_t{0});
    CheckBinaryFinalizers<int64_t, BitmapIndex<int64_t>>(
        values, proto::schema::DataType::Int64);
}

TEST_F(LegacyScalarAsyncLoadTest, BitmapMemoryAndMmap) {
    RoundTripBinary<int64_t, BitmapIndex<int64_t>>(
        {10, 20, 30, 10, 50}, proto::schema::DataType::Int64);
}

TEST_F(LegacyScalarAsyncLoadTest, HighCardinalityBitmapUsesMmapFinalizer) {
    std::vector<int64_t> values(600);
    std::iota(values.begin(), values.end(), int64_t{0});
    BitmapIndex<int64_t> build(Context(proto::schema::DataType::Int64));
    build.Build(values.size(), values.data());
    const auto files = Persist(build.Serialize({}));
    source_->expect_async = true;
    ObservedLegacyIndex<BitmapIndex<int64_t>> loaded(
        Context(proto::schema::DataType::Int64, true));
    loaded.Load(tracer::TraceContext{}, LoadConfig(files, true), nullptr);
    EXPECT_EQ(loaded.Count(), values.size());
    EXPECT_EQ(loaded.IsNull().count(), 0);
    const int64_t needles[]{0, 599};
    const auto hits = loaded.In(2, needles);
    EXPECT_EQ(hits.count(), 2);
    EXPECT_TRUE(hits[0]);
    EXPECT_TRUE(hits[599]);
    EXPECT_TRUE(loaded.finalizer_thread.starts_with("MILVUS_ASYNC"));
}

TEST_F(LegacyScalarAsyncLoadTest, StringSortMemoryAndMmap) {
    RoundTripBinary<std::string, StringIndexSort>(
        {"cat", "dog", "owl", "cat", "ant"}, proto::schema::DataType::VarChar);
}

TEST_F(LegacyScalarAsyncLoadTest, MarisaMemoryAndMmap) {
    RoundTripBinary<std::string, StringIndexMarisa>(
        {"cat", "dog", "owl", "cat", "ant"}, proto::schema::DataType::VarChar);
}

TEST_F(LegacyScalarAsyncLoadTest, NumericSortMmapRestoresOnAsyncWorker) {
    RoundTripBinary<int64_t, ScalarIndexSort<int64_t>>(
        {10, 20, 30, 10, 50}, proto::schema::DataType::Int64);
}

TEST_F(LegacyScalarAsyncLoadTest,
       HybridChildAndTypeInspectionUseAsyncExecutor) {
    const auto context = Context(proto::schema::DataType::Int64);
    HybridScalarIndex<int64_t> build(TANTIVY_INDEX_LATEST_VERSION, context);
    const int64_t values[]{10, 20, 30, 10, 50};
    const bool valid[]{true, false, true, true, false};
    build.Build(5, values, valid);
    const auto files = Persist(build.Serialize({}));
    source_->expect_async = true;
    HybridScalarIndex<int64_t> loaded(TANTIVY_INDEX_LATEST_VERSION, context);
    loaded.Load(tracer::TraceContext{}, LoadConfig(files, false), nullptr);
    CheckNullable<int64_t>(loaded, 10);
    const auto request =
        IndexFactory::GetInstance().ScalarIndexFileLoadResource(
            DataType::INT64,
            1,
            {{INDEX_TYPE, HYBRID_INDEX_TYPE},
             {SCALAR_INDEX_ENGINE_VERSION, "2"}},
            false,
            5,
            files,
            context);
    EXPECT_GT(request.request.max_memory_cost,
              request.request.final_memory_cost);
    EXPECT_FALSE(request.overhead.has_value());
}

TEST_F(LegacyScalarAsyncLoadTest, TantivyMetadataAndFilesMemoryAndMmap) {
    const std::vector<std::string> values{"cat", "dog", "owl", "cat", "ant"};
    auto field =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, true);
    uint8_t valid = 0b01101;
    field->FillFieldData(values.data(), &valid, values.size(), 0);
    ObservedLegacyIndex<InvertedIndexTantivy<std::string>> build(
        TANTIVY_INDEX_LATEST_VERSION,
        Context(proto::schema::DataType::VarChar));
    build.BuildWithFieldData({field});
    build.FinishForTest();
    const auto files = PersistDirectory(build.Path(), build.Serialize({}));
    for (bool mmap : {false, true}) {
        source_->expect_async = true;
        ObservedLegacyIndex<InvertedIndexTantivy<std::string>> loaded(
            TANTIVY_INDEX_LATEST_VERSION,
            Context(proto::schema::DataType::VarChar, true));
        CheckBlockedFinalizer(loaded, LoadConfig(files, mmap), false, 0);
        EXPECT_TRUE(loaded.finalizer_thread.starts_with("MILVUS_ASYNC"));
        CheckNullable(loaded, values[0]);
    }
}

TEST_F(LegacyScalarAsyncLoadTest, NgramMetadataAndFiles) {
    const std::vector<std::string> values{"cat", "dog", "owl", "cat", "ant"};
    auto field =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, true);
    uint8_t valid = 0b01101;
    field->FillFieldData(values.data(), &valid, values.size(), 0);
    ObservedLegacyIndex<NgramInvertedIndex> build(
        Context(proto::schema::DataType::VarChar), NgramParams{false, 2, 3});
    build.BuildWithFieldData({field});
    build.FinishForTest();
    auto metadata = build.Serialize({});
    const size_t average = 3;
    auto average_bytes =
        std::shared_ptr<uint8_t[]>(new uint8_t[sizeof(average)]);
    std::memcpy(average_bytes.get(), &average, sizeof(average));
    auto files = PersistDirectory(build.Path(), metadata);
    // The compatibility Ngram format stores this scalar as an unsliced object.
    files.push_back(PersistBytes(
        NGRAM_AVG_ROW_SIZE_FILE_NAME, average_bytes.get(), sizeof(average)));
    for (const bool enabled : {false, true}) {
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(enabled);
        source_->expect_async = enabled;
        for (const bool mmap : {false, true}) {
            ObservedLegacyIndex<NgramInvertedIndex> loaded(
                Context(proto::schema::DataType::VarChar, true),
                NgramParams{true, 2, 3});
            loaded.Load(
                tracer::TraceContext{}, LoadConfig(files, mmap), nullptr);
            EXPECT_EQ(loaded.Count(), 5);
            EXPECT_EQ(loaded.IsNull().count(), 2);
            EXPECT_EQ(std::filesystem::exists(loaded.Path()), mmap);
        }
    }
}

TEST_F(LegacyScalarAsyncLoadTest, RTreeFilesAndNullSidecar) {
    auto point = [](double x, double y) {
        std::string bytes(21, '\0');
        bytes[0] = 1;
        const uint32_t type = 1;
        std::memcpy(bytes.data() + 1, &type, sizeof(type));
        std::memcpy(bytes.data() + 5, &x, sizeof(x));
        std::memcpy(bytes.data() + 13, &y, sizeof(y));
        return bytes;
    };
    const std::vector<std::string> values{
        point(1, 1), point(2, 2), point(3, 3)};
    auto field =
        storage::CreateFieldData(DataType::GEOMETRY, DataType::NONE, true);
    const uint8_t valid = 0b101;
    field->FillFieldData(values.data(), &valid, values.size(), 0);
    ObservedLegacyIndex<RTreeIndex<std::string>> build(
        Context(proto::schema::DataType::Geometry));
    build.InitForBuildIndex(false);
    build.BuildWithFieldData({field});
    build.FinishForTest();
    const auto files = PersistDirectory(build.Path(), build.Serialize({}));
    const auto context = Context(proto::schema::DataType::Geometry, true);
    storage::MemFileManagerImpl manager(context);
    const auto prefix = manager.GetRemoteIndexObjectPrefix();
    std::filesystem::create_directories(prefix);
    std::vector<std::string> names;
    for (const auto& file : files) {
        auto name = std::filesystem::path(file).filename().string();
        std::filesystem::copy_file(file, std::filesystem::path(prefix) / name);
        names.push_back(std::move(name));
    }
    source_->expect_async = true;
    for (const auto& paths : {files, names}) {
        ObservedLegacyIndex<RTreeIndex<std::string>> loaded(context);
        CheckBlockedFinalizer(loaded, LoadConfig(paths, false), false, 0);
        EXPECT_TRUE(loaded.finalizer_thread.starts_with("MILVUS_ASYNC"));
        EXPECT_EQ(loaded.Count(), 3);
        EXPECT_EQ(loaded.IsNull().count(), 1);
        EXPECT_TRUE(loaded.IsNull()[1]);
        Geometry geometry(GetThreadLocalGEOSContext(), "POINT (1 1)");
        std::vector<int64_t> hits;
        loaded.QueryCandidates(
            proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            geometry,
            hits);
        EXPECT_EQ(hits, std::vector<int64_t>{0});
        auto resource = IndexFactory::GetInstance().ScalarIndexFileLoadResource(
            DataType::GEOMETRY,
            1,
            {{INDEX_TYPE, RTREE_INDEX_TYPE},
             {SCALAR_INDEX_ENGINE_VERSION, "2"}},
            false,
            3,
            paths,
            context);
        EXPECT_GT(resource.request.final_memory_cost, 0);
    }
}

TEST_F(LegacyScalarAsyncLoadTest, JsonInvertedSlicedMetadataAndOldFallback) {
    auto context = Context(proto::schema::DataType::JSON);
    context.fieldDataMeta.field_schema.set_nullable(false);
    std::vector<Json> jsons;
    for (const std::string value : {R"({"a": 1})",
                                    R"({"b": 2})",
                                    R"({"a": "bad"})",
                                    R"({"a": 3})",
                                    R"(42)"}) {
        jsons.emplace_back(simdjson::padded_string(value));
    }
    auto field = std::make_shared<FieldData<Json>>(DataType::JSON, false);
    field->add_json_data(jsons);
    using JsonIndex =
        JsonScalarIndexWrapper<double, InvertedIndexTantivy<double>>;
    ObservedLegacyIndex<JsonIndex> build(
        JsonCastType::FromString("DOUBLE"),
        "/a",
        JsonCastFunction::FromString("unknown"),
        context.fieldDataMeta.field_schema,
        context,
        TANTIVY_INDEX_LATEST_VERSION);
    build.BuildWithFieldData({field});
    build.FinishForTest();
    for (const bool old_fallback : {false, true}) {
        auto metadata = build.Serialize({});
        if (old_fallback) {
            metadata.Erase(INDEX_NON_EXIST_OFFSET_FILE_NAME);
        }
        const auto files = PersistDirectory(build.Path(), metadata);
        context.set_for_loading_index(true);
        source_->expect_async = true;
        JsonIndex loaded(JsonCastType::FromString("DOUBLE"),
                         "/a",
                         JsonCastFunction::FromString("unknown"),
                         context.fieldDataMeta.field_schema,
                         context,
                         TANTIVY_INDEX_LATEST_VERSION);
        static_cast<IndexBase&>(loaded).Load(
            tracer::TraceContext{}, LoadConfig(files, true), nullptr);
        EXPECT_EQ(loaded.Count(), 5);
        EXPECT_EQ(loaded.IsNull().count(), 3);
        const auto exists = loaded.Exists();
        ASSERT_EQ(exists.size(), 5);
        EXPECT_EQ(exists.count(), old_fallback ? 2 : 3);
        EXPECT_EQ(exists[2], !old_fallback);
        EXPECT_FALSE(exists[1]);
        EXPECT_FALSE(exists[4]);
        const double value = 1;
        EXPECT_EQ(loaded.NotIn(1, &value).count(), 1);
    }
}

TEST_F(LegacyScalarAsyncLoadTest, JsonHybridRestoresExistsAfterChildLoad) {
    auto context = Context(proto::schema::DataType::JSON);
    context.fieldDataMeta.field_schema.set_nullable(false);
    std::vector<Json> jsons;
    // Legacy Hybrid did not persist missing-path offsets. Exercise its
    // supported all-paths-present format, including a failed numeric cast.
    for (const std::string value :
         {R"({"a": 1})", R"({"a": "bad"})", R"({"a": 3})"}) {
        jsons.emplace_back(simdjson::padded_string(value));
    }
    auto field = std::make_shared<FieldData<Json>>(DataType::JSON, false);
    field->add_json_data(jsons);
    JsonHybridScalarIndex<double> build(JsonCastType::FromString("DOUBLE"),
                                        "/a",
                                        JsonCastFunction::FromString("unknown"),
                                        context.fieldDataMeta.field_schema,
                                        TANTIVY_INDEX_LATEST_VERSION,
                                        context);
    build.BuildWithFieldData({field});
    const auto files = Persist(build.Serialize({}));
    source_->expect_async = true;
    context.set_for_loading_index(true);
    JsonHybridScalarIndex<double> loaded(
        JsonCastType::FromString("DOUBLE"),
        "/a",
        JsonCastFunction::FromString("unknown"),
        context.fieldDataMeta.field_schema,
        TANTIVY_INDEX_LATEST_VERSION,
        context);
    static_cast<IndexBase&>(loaded).Load(
        tracer::TraceContext{}, LoadConfig(files, false), nullptr);
    EXPECT_EQ(loaded.Count(), 3);
    EXPECT_EQ(loaded.Exists().count(), 3);
    EXPECT_EQ(loaded.IsNull().count(), 1);
    const double value = 1;
    EXPECT_EQ(loaded.NotIn(1, &value).count(), 1);
}

TEST_F(LegacyScalarAsyncLoadTest, HybridSortChildMakesProgressWithOneWorker) {
    const auto context = Context(proto::schema::DataType::Int64);
    HybridScalarIndex<int64_t> build(TANTIVY_INDEX_LATEST_VERSION, context);
    build.bitmap_index_cardinality_limit_ = 1;
    const int64_t values[]{10, 20, 30, 10, 50};
    const bool valid[]{true, false, true, true, false};
    build.Build(5, values, valid);
    ASSERT_EQ(build.internal_index_type_, ScalarIndexType::STLSORT);
    const auto files = Persist(build.Serialize({}));
    source_->expect_async = true;
    HybridScalarIndex<int64_t> loaded(TANTIVY_INDEX_LATEST_VERSION, context);
    loaded.Load(tracer::TraceContext{}, LoadConfig(files, true), nullptr);
    CheckNullable<int64_t>(loaded, 10);
}

TEST_F(LegacyScalarAsyncLoadTest, DisabledLocalPoolStillLoadsMmap) {
    ScalarIndexSort<int64_t> build(Context(proto::schema::DataType::Int64));
    const int64_t values[]{10, 20, 30, 10, 50};
    const bool valid[]{true, false, true, true, false};
    build.Build(5, values, valid);
    const auto files = Persist(build.Serialize({}));
    storage::LocalFileIOPool::GetInstance().Configure(0);
    source_->expect_async = true;
    ScalarIndexSort<int64_t> loaded(
        Context(proto::schema::DataType::Int64, true));
    loaded.Load(tracer::TraceContext{}, LoadConfig(files, true), nullptr);
    CheckNullable<int64_t>(loaded, 10);
}

TEST_F(LegacyScalarAsyncLoadTest,
       BitmapEstimateReadsMetadataWithoutMaterializingPostings) {
    constexpr size_t rows = 10000;
    std::vector<int64_t> values(rows);
    for (size_t i = 0; i < rows; ++i) {
        values[i] = i % 2;
    }
    const auto context = Context(proto::schema::DataType::Int64);
    BitmapIndex<int64_t> build(context);
    build.Build(rows, values.data());
    const auto files = Persist(build.Serialize({}), false);
    const auto data_file =
        std::find_if(files.begin(), files.end(), [](const auto& file) {
            return std::filesystem::path(file).filename() == BITMAP_INDEX_DATA;
        });
    ASSERT_NE(data_file, files.end());
    auto input = storage::OpenLegacyIndexInput(source_, nullptr, *data_file);
    const auto info =
        folly::coro::blockingWait(storage::InspectLegacyIndexFileAsync(
            *input, proto::common::LoadPriority::HIGH));
    source_->on_read = [&](const auto& path, uint64_t offset) {
        if (path == *data_file) {
            EXPECT_LT(offset, info.payload_offset)
                << "planning must not materialize postings";
        }
    };
    source_->expect_async = true;
    const auto resource =
        IndexFactory::GetInstance().ScalarIndexFileLoadResource(
            DataType::INT64,
            1,
            {{INDEX_TYPE, BITMAP_INDEX_TYPE},
             {SCALAR_INDEX_ENGINE_VERSION, "2"}},
            true,
            rows,
            files,
            context);
    const auto bitmap_bytes = TargetBitmap(rows).size_in_bytes();
    EXPECT_GE(resource.request.final_memory_cost, bitmap_bytes * 3);
    EXPECT_GT(resource.request.max_memory_cost,
              resource.request.final_memory_cost);
}

TEST_F(LegacyScalarAsyncLoadTest,
       NestedStringSortAndBitmapRestoreFlattenedOffsets) {
    auto context = Context(proto::schema::DataType::Array);
    context.fieldDataMeta.field_schema.set_element_type(
        proto::schema::DataType::String);
    proto::schema::ScalarField first;
    for (const auto* value : {"cat", "dog", "owl"}) {
        first.mutable_string_data()->add_data(value);
    }
    proto::schema::ScalarField second;
    for (const auto* value : {"cat", "ant"}) {
        second.mutable_string_data()->add_data(value);
    }
    std::vector<Array> arrays{Array(first), Array(second)};
    auto field =
        storage::CreateFieldData(DataType::ARRAY, DataType::NONE, true);
    const uint8_t valid = 0b11;
    field->FillFieldData(arrays.data(), &valid, arrays.size(), 0);
    for (bool bitmap : {false, true}) {
        std::unique_ptr<ScalarIndex<std::string>> build;
        if (bitmap) {
            build = std::make_unique<BitmapIndex<std::string>>(context, true);
        } else {
            build = std::make_unique<StringIndexSort>(context, true);
        }
        build->BuildWithFieldData({field});
        const auto files = Persist(build->Serialize({}));
        std::unique_ptr<ScalarIndex<std::string>> loaded;
        if (bitmap) {
            loaded = std::make_unique<BitmapIndex<std::string>>(context, true);
        } else {
            loaded = std::make_unique<StringIndexSort>(context, true);
        }
        source_->expect_async = true;
        loaded->Load(tracer::TraceContext{}, LoadConfig(files, true), nullptr);
        EXPECT_EQ(loaded->Count(), 5);
        const std::string needle = "cat";
        const auto hits = loaded->In(1, &needle);
        ASSERT_EQ(hits.size(), 5);
        EXPECT_EQ(hits.count(), 2);
        EXPECT_TRUE(hits[0]);
        EXPECT_TRUE(hits[3]);
        const auto resource =
            IndexFactory::GetInstance().ScalarIndexFileLoadResource(
                DataType::ARRAY,
                1,
                {{INDEX_TYPE, bitmap ? BITMAP_INDEX_TYPE : ASCENDING_SORT},
                 {SCALAR_INDEX_ENGINE_VERSION, "2"}},
                true,
                2,
                files,
                context);
        EXPECT_GE(resource.request.final_memory_cost,
                  TargetBitmap(5).size_in_bytes());
        EXPECT_FALSE(resource.request.has_raw_data);
    }
}

TEST_F(LegacyScalarAsyncLoadTest, CancellationBeforeOpeningAnyFile) {
    BitmapIndex<int64_t> loaded(Context(proto::schema::DataType::Int64, true));
    folly::CancellationSource cancellation;
    cancellation.requestCancellation();
    OpContext context;
    context.cancellation_token = cancellation.getToken();
    try {
        loaded.Load(tracer::TraceContext{},
                    LoadConfig({"must-not-open"}, false),
                    &context);
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FollyCancel);
    }
}

TEST_F(LegacyScalarAsyncLoadTest, CancellationDrainsQueuedLocalWrite) {
    const uint8_t bytes[]{1, 2, 3, 4};
    const std::vector<std::string> files{
        PersistBytes("queued_0", bytes, sizeof(bytes))};
    auto input = storage::OpenLegacyIndexInput(source_, nullptr, files.front());
    const auto info =
        folly::coro::blockingWait(storage::InspectLegacyIndexFileAsync(
            *input, proto::common::LoadPriority::HIGH));
    folly::Baton<> release;
    std::promise<void> blocked;
    auto blocked_future = blocked.get_future();
    auto executor = storage::LocalFileIOPool::GetInstance().GetExecutor();
    source_->on_read = [&](const auto&, uint64_t offset) {
        if (offset >= info.payload_offset) {
            executor->add([&] {
                blocked.set_value();
                release.wait();
            });
        }
    };
    storage::DiskFileManagerImpl manager(
        Context(proto::schema::DataType::VarChar, true));
    const auto prefix = manager.GetLocalIndexObjectPrefix();
    folly::CancellationSource cancellation;
    auto pending = std::async(std::launch::async, [&] {
        folly::coro::blockingWait(
            manager
                .CacheIndexToDiskAsync(files,
                                       prefix,
                                       proto::common::LoadPriority::HIGH,
                                       cancellation.getToken())
                .scheduleOn(storage::ResolveAsyncLoadExecutor(
                    {}, proto::common::LoadPriority::HIGH)));
    });
    auto drain = folly::makeGuard([&] { release.post(); });
    ASSERT_EQ(blocked_future.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    cancellation.requestCancellation();
    auto& admission = storage::LoadAdmissionController::GetInstance();
    const bool acquired =
        admission.TryAcquire({1, 1}, storage::LoadAdmissionPriority::High);
    if (acquired) {
        admission.Release({1, 1});
    }
    EXPECT_FALSE(acquired);
    release.post();
    try {
        pending.get();
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FollyCancel);
    }
    EXPECT_TRUE(manager.GetLocalFilePaths().empty());
    EXPECT_FALSE(std::filesystem::exists(prefix));
}

TEST_F(LegacyScalarAsyncLoadTest, LocalPoolShutdownDoesNotWaitForRemoteRead) {
    const uint8_t bytes[]{1, 2, 3, 4};
    const std::vector<std::string> files{
        PersistBytes("shutdown_0", bytes, sizeof(bytes))};
    auto input = storage::OpenLegacyIndexInput(source_, nullptr, files.front());
    const auto info =
        folly::coro::blockingWait(storage::InspectLegacyIndexFileAsync(
            *input, proto::common::LoadPriority::HIGH));
    folly::Baton<> release;
    std::promise<void> reading;
    auto reading_future = reading.get_future();
    source_->on_read = [&](const auto&, uint64_t offset) {
        if (offset >= info.payload_offset) {
            reading.set_value();
            release.wait();
        }
    };
    storage::DiskFileManagerImpl manager(
        Context(proto::schema::DataType::VarChar, true));
    const auto prefix = manager.GetLocalIndexObjectPrefix();
    auto pending = std::async(std::launch::async, [&] {
        folly::coro::blockingWait(
            manager
                .CacheIndexToDiskAsync(
                    files, prefix, proto::common::LoadPriority::HIGH)
                .scheduleOn(storage::ResolveAsyncLoadExecutor(
                    {}, proto::common::LoadPriority::HIGH)));
    });
    auto drain = folly::makeGuard([&] { release.post(); });
    ASSERT_EQ(reading_future.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    auto disabling = std::async(std::launch::async, [] {
        storage::LocalFileIOPool::GetInstance().Configure(0);
    });
    EXPECT_EQ(disabling.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    release.post();
    disabling.get();
    pending.get();
    ASSERT_EQ(manager.GetLocalFilePaths().size(), 1);
    EXPECT_EQ(std::filesystem::file_size(manager.GetLocalFilePaths().front()),
              sizeof(bytes));
}

TEST_F(LegacyScalarAsyncLoadTest, DiskFailurePreservesExistingDestination) {
    const uint8_t payload[]{1, 2, 3, 4};
    const std::vector<std::string> files{
        PersistBytes("kept_0", payload, sizeof(payload))};
    storage::DiskFileManagerImpl manager(
        Context(proto::schema::DataType::VarChar, true));
    const auto prefix = manager.GetLocalIndexObjectPrefix();
    std::filesystem::create_directories(prefix);
    const auto destination = std::filesystem::path(prefix) / "kept";
    {
        std::ofstream output(destination, std::ios::binary);
        output << "existing";
    }
    try {
        folly::coro::blockingWait(
            manager
                .CacheIndexToDiskAsync(
                    files, prefix, proto::common::LoadPriority::HIGH)
                .scheduleOn(storage::ResolveAsyncLoadExecutor(
                    {}, proto::common::LoadPriority::HIGH)));
        FAIL() << "expected existing destination failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FileCreateFailed);
    }
    EXPECT_TRUE(manager.GetLocalFilePaths().empty());
    std::ifstream input(destination, std::ios::binary);
    const std::string retained((std::istreambuf_iterator<char>(input)), {});
    EXPECT_EQ(retained, "existing");
}

TEST_F(LegacyScalarAsyncLoadTest, DiskFailureRemovesCompletedAndPartialFiles) {
    const uint8_t payload[]{1, 2, 3, 4};
    const std::vector<std::string> files{
        PersistBytes("a_0", payload, sizeof(payload)),
        PersistBytes("b_0", payload, sizeof(payload))};
    auto input = storage::OpenLegacyIndexInput(source_, nullptr, files.back());
    const auto info =
        folly::coro::blockingWait(storage::InspectLegacyIndexFileAsync(
            *input, proto::common::LoadPriority::HIGH));
    source_->on_read = [&](const auto& path, uint64_t offset) {
        if (path == files.back() && offset >= info.payload_offset) {
            ThrowInfo(FileReadFailed, "injected legacy disk read failure");
        }
    };
    storage::DiskFileManagerImpl manager(
        Context(proto::schema::DataType::VarChar, true));
    const auto prefix = manager.GetLocalIndexObjectPrefix();
    try {
        folly::coro::blockingWait(
            manager
                .CacheIndexToDiskAsync(
                    files, prefix, proto::common::LoadPriority::HIGH)
                .scheduleOn(storage::ResolveAsyncLoadExecutor(
                    {}, proto::common::LoadPriority::HIGH)));
        FAIL() << "expected disk load failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FileReadFailed);
    }
    EXPECT_TRUE(manager.GetLocalFilePaths().empty());
    EXPECT_FALSE(std::filesystem::exists(prefix));
}

}  // namespace
}  // namespace milvus::index
