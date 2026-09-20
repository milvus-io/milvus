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
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <arrow/filesystem/localfs.h>
#include "folly/ScopeGuard.h"
#include "index/LoadResource.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/scalar/spatial/RTreeIndexReader.h"
#include "storage/EntryStreamUtils.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryEncryptedLocalWriter.h"
#include "storage/LoadOverheadController.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/PluginLoader.h"
#include "storage/Util.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "segcore/storagev1translator/SealedIndexTranslator.h"
#include "segcore/Types.h"
#include "test_utils/PlannerCipherPlugin.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index {
namespace {
struct ResourceFixture {
    std::shared_ptr<storage::LocalDirectory> root =
        storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "resource-test-XXXXXX",
            "resource estimate test");
    storage::FileManagerContext context;
    ResourceFixture() {
        storage::StorageConfig config;
        config.storage_type = "local";
        config.root_path = root->Path();
        context =
            storage::FileManagerContext(storage::FieldDataMeta{1, 2, 3, 101},
                                        storage::IndexMeta{3, 101, 1000, 10000},
                                        storage::CreateChunkManager(config),
                                        storage::InitArrowFileSystem(config));
    }
    std::string
    WritePacked(const std::map<std::string, std::vector<uint8_t>>& entries,
                const Config& metadata = Config::object(),
                std::shared_ptr<storage::plugin::ICipherPlugin> cipher = {}) {
        storage::MemFileManagerImpl manager(context);
        const auto path = manager.GetRemoteIndexObjectPrefix() + "/resource.v3";
        std::unique_ptr<storage::IndexEntryWriter> writer;
        if (cipher) {
            auto status = context.fs->CreateDir(
                manager.GetRemoteIndexObjectPrefix(), true);
            EXPECT_TRUE(status.ok());
            writer = std::make_unique<storage::IndexEntryEncryptedLocalWriter>(
                path,
                context.fs,
                cipher,
                7,
                context.fieldDataMeta.collection_id,
                root->Path(),
                storage::kStreamSliceAlignment);
        } else {
            writer = manager.CreateIndexEntryWriterUnified(path);
        }
        for (const auto& [name, data] : entries) {
            writer->WriteEntry(name, data.data(), data.size());
        }
        for (auto it = metadata.begin(); it != metadata.end(); ++it) {
            writer->PutMeta(it.key(), it.value());
        }
        writer->Finish();
        return path;
    }
    uint64_t
    Size(const std::string& path) {
        return context.fs->GetFileInfo(path).ValueOrDie().size();
    }
};

std::map<std::string, std::string>
V3Params(const std::string& type) {
    return {{INDEX_TYPE, type}, {SCALAR_INDEX_ENGINE_VERSION, "3"}};
}

class RecordingOpenFileSystem : public arrow::fs::LocalFileSystem {
 public:
    explicit RecordingOpenFileSystem(std::shared_ptr<arrow::fs::FileSystem> fs)
        : fs_(std::move(fs)) {
    }
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        ++sync_opens;
        return fs_->OpenInputFile(path);
    }
    arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFileAsync(const std::string& path) override {
        ++async_opens;
        return arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>::
            MakeFinished(fs_->OpenInputFile(path));
    }
    int sync_opens{0};
    int async_opens{0};

 private:
    std::shared_ptr<arrow::fs::FileSystem> fs_;
};
}  // namespace

TEST(LegacyHybridResourceEstimate, ResolvesPersistedChildType) {
    using namespace milvus;
    using namespace milvus::index;
    ResourceFixture fixture;
    auto ctx = fixture.context;
    ctx.indexMeta.build_id = 5348301;
    storage::MemFileManagerImpl manager(ctx);
    auto cleanup = folly::makeGuard([&] {
        for (const auto& [path, size] : manager.GetRemotePathsToFileSize()) {
            ctx.chunkManagerPtr->Remove(path);
        }
    });
    constexpr uint64_t index_size = 1UL << 20;
    constexpr int64_t rows = 10001;
    for (const auto& [type, name] :
         {std::pair{ScalarIndexType::BITMAP, BITMAP_INDEX_TYPE},
          std::pair{ScalarIndexType::INVERTED, INVERTED_INDEX_TYPE},
          std::pair{ScalarIndexType::STLSORT, ASCENDING_SORT},
          std::pair{ScalarIndexType::MARISA, MARISA_TRIE}}) {
        // Use the same standalone binlog encoding as legacy HYBRID uploads.
        BinarySet binary_set;
        auto data = std::make_shared<uint8_t[]>(1);
        data[0] = static_cast<uint8_t>(type);
        binary_set.Append(INDEX_TYPE, data, 1);
        // Async estimation inspects real legacy envelopes. Bitmap also reads
        // its cardinality/row metadata; the estimator never decodes postings.
        const auto bitmap_meta = Config{
            {BITMAP_INDEX_LENGTH, 2},
            {BITMAP_INDEX_NUM_ROWS,
             rows}}.dump();
        auto meta_bytes = std::make_shared<uint8_t[]>(bitmap_meta.size());
        std::memcpy(meta_bytes.get(), bitmap_meta.data(), bitmap_meta.size());
        binary_set.Append(BITMAP_INDEX_META, meta_bytes, bitmap_meta.size());
        auto payload = std::make_shared<uint8_t[]>(17);
        std::fill_n(payload.get(), 17, uint8_t{42});
        binary_set.Append("index_data", payload, 17);
        ASSERT_TRUE(manager.AddFile(binary_set));
        ASSERT_EQ(manager.GetRemotePathsToFileSize().size(), 3);
        std::vector<std::string> async_files;
        std::string selector_path;
        for (const auto& [path, bytes] : manager.GetRemotePathsToFileSize()) {
            async_files.push_back(path);
            if (std::filesystem::path(path).filename() == INDEX_TYPE) {
                selector_path = path;
            }
        }
        ASSERT_FALSE(selector_path.empty());
        // Preserve the synchronous regression: resolving the child must not
        // open any engine payload while using the size-only estimate.
        const std::vector<std::string> sync_files{
            fixture.root->Path() + "/unread_index_payload", selector_path};
        for (const auto* version : {"1", "2", ""}) {
            std::map<std::string, std::string> params{
                {INDEX_TYPE, HYBRID_INDEX_TYPE}};
            if (*version != '\0') {
                params[SCALAR_INDEX_ENGINE_VERSION] = version;
            }
            auto child_params = params;
            child_params[INDEX_TYPE] = name;
            for (bool async : {false, true}) {
                ctx.use_async_load = async;
                for (bool mmap : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << name << " version=" << version
                                 << " async=" << async << " mmap=" << mmap);
                    const auto& files = async ? async_files : sync_files;
                    const auto expected =
                        async ? ScalarIndexFileLoadResource(DataType::INT64,
                                                            index_size,
                                                            child_params,
                                                            mmap,
                                                            rows,
                                                            files,
                                                            ctx)
                                    .request
                              : ScalarIndexLoadResource(DataType::INT64,
                                                        0,
                                                        index_size,
                                                        child_params,
                                                        mmap,
                                                        rows);
                    const auto resources =
                        ScalarIndexFileLoadResource(DataType::INT64,
                                                    index_size,
                                                    params,
                                                    mmap,
                                                    rows,
                                                    files,
                                                    ctx);
                    EXPECT_EQ(resources.request.final_memory_cost,
                              expected.final_memory_cost);
                    EXPECT_EQ(resources.request.final_disk_cost,
                              expected.final_disk_cost);
                    EXPECT_EQ(resources.request.max_memory_cost,
                              expected.max_memory_cost);
                    EXPECT_EQ(resources.request.max_disk_cost,
                              expected.max_disk_cost);
                    EXPECT_EQ(resources.request.has_raw_data,
                              expected.has_raw_data);
                    EXPECT_FALSE(resources.overhead.has_value());
                }
            }
        }
    }
}

TEST(LegacyHybridResourceEstimate, FallsBackWhenChildTypeIsUnavailable) {
    using namespace milvus;
    using namespace milvus::index;
    ResourceFixture fixture;
    auto ctx = fixture.context;
    ctx.indexMeta.build_id = 5348302;
    storage::MemFileManagerImpl manager(ctx);
    auto cleanup = folly::makeGuard([&] {
        for (const auto& [path, size] : manager.GetRemotePathsToFileSize()) {
            ctx.chunkManagerPtr->Remove(path);
        }
    });
    constexpr uint64_t index_size = 1UL << 20;
    const std::map<std::string, std::string> params{
        {INDEX_TYPE, HYBRID_INDEX_TYPE}};
    auto check_fallback = [&](const std::vector<std::string>& files,
                              const storage::FileManagerContext& context) {
        const auto resources = ScalarIndexFileLoadResource(
            DataType::INT64, index_size, params, false, 10001, files, context);
        EXPECT_EQ(resources.request.final_memory_cost, index_size);
        EXPECT_EQ(resources.request.final_disk_cost, index_size);
        EXPECT_EQ(resources.request.max_memory_cost, 2 * index_size);
        EXPECT_EQ(resources.request.max_disk_cost, index_size);
        EXPECT_FALSE(resources.request.has_raw_data);
        EXPECT_FALSE(resources.overhead.has_value());
    };
    check_fallback({}, ctx);
    check_fallback({fixture.root->Path() + "/missing_legacy_hybrid/index_type"},
                   ctx);
    // Unknown type and invalid payload length must not be decoded as a child.
    for (const size_t size : {1, 2}) {
        BinarySet binary_set;
        auto data = std::make_shared<uint8_t[]>(size);
        data[0] =
            size == 1 ? 255 : static_cast<uint8_t>(ScalarIndexType::BITMAP);
        binary_set.Append(INDEX_TYPE, data, size);
        ASSERT_TRUE(manager.AddFile(binary_set));
        const auto path = manager.GetRemotePathsToFileSize().begin()->first;
        check_fallback({path}, ctx);
        check_fallback({path}, storage::FileManagerContext{});
    }
}

TEST(ScalarIndexV3ResourceTest, ResourceInspectionSelectsStreamOpenMode) {
    ResourceFixture fixture;
    // Inspection needs real directory bytes, but must never initialize an engine.
    const auto path = fixture.WritePacked({{"index_data", {1, 2, 3}}});
    const auto size = fixture.Size(path);
    auto fs = std::make_shared<RecordingOpenFileSystem>(fixture.context.fs);
    fixture.context.fs = fs;
    for (const bool async : {false, true}) {
        fixture.context.use_async_load = async;
        fs->sync_opens = fs->async_opens = 0;
        static_cast<void>(ScalarIndexFileLoadResource(DataType::INT32,
                                                      size,
                                                      V3Params(ASCENDING_SORT),
                                                      false,
                                                      4,
                                                      {path},
                                                      fixture.context));
        EXPECT_EQ(fs->sync_opens, async ? 0 : 1);
        EXPECT_EQ(fs->async_opens, async ? 1 : 0);
    }
}

TEST(ScalarIndexV3ResourceTest,
     PinnedEstimatesRetainSharedGroupAcrossLiveLimits) {
    ResourceFixture fixture;
    auto& budget = storage::LoadAdmissionController::GetInstance();
    const auto old_bytes = budget.CapacityBytes();
    const auto old_slots = budget.CapacitySlots();
    const auto old_workers = storage::GetAsyncLoadThreadPoolSize();
    const auto old_enabled =
        segcore::storagev2translator::StorageV2AsyncLoadEnabled();
    auto restore = folly::makeGuard([&] {
        budget.SetCapacityBytes(old_bytes);
        budget.SetCapacitySlots(old_slots);
        storage::SetAsyncLoadThreadPoolSize(old_workers);
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(old_enabled);
    });
    constexpr size_t rows = 10001;
    for (const bool nullable : {false, true}) {
        std::map<std::string, std::vector<uint8_t>> entries{
            {"engine", std::vector<uint8_t>(8192)}};
        if (nullable)
            entries[INDEX_NULL_OFFSET] = std::vector<uint8_t>(sizeof(size_t));
        const auto path = fixture.WritePacked(
            entries,
            {{INDEX_TYPE, static_cast<uint8_t>(ScalarIndexType::INVERTED)}});
        const auto size = fixture.Size(path);
        for (const bool mmap : {false, true}) {
            for (const bool async : {false, true}) {
                fixture.context.use_async_load = async;
                auto estimate = [&] {
                    return ScalarIndexFileLoadResource(
                        DataType::VARCHAR,
                        size,
                        V3Params(HYBRID_INDEX_TYPE),
                        mmap,
                        rows,
                        {path},
                        fixture.context);
                };
                const auto baseline = estimate();
                ASSERT_EQ(baseline.overhead.has_value(), !nullable);
                if (baseline.overhead) {
                    ASSERT_TRUE(baseline.overhead->memory.has_value());
                    EXPECT_EQ(
                        baseline.overhead->memory->group,
                        storage::LoadMemoryOverheadController::GetInstance()
                            .GetOrCreate());
                    EXPECT_FALSE(baseline.overhead->file.has_value());
                    if (async) {
                        ASSERT_TRUE(baseline.overhead->memory->max_runtime_unit
                                        .has_value());
                        EXPECT_GT(*baseline.overhead->memory->max_runtime_unit,
                                  0);
                        EXPECT_LE(*baseline.overhead->memory->max_runtime_unit,
                                  2 * storage::DefaultStreamSliceSize() +
                                      2 * storage::FileWriter::ALIGNMENT_MASK);
                    }
                } else {
                    EXPECT_GE(
                        baseline.request.max_memory_cost,
                        baseline.request.final_memory_cost + sizeof(size_t));
                }
                for (const size_t limit :
                     {size_t{1}, size_t{0}, size_t{1024 * 1024}}) {
                    budget.SetCapacityBytes(limit);
                    budget.SetCapacitySlots(limit == 0 ? 0 : 1);
                    for (const bool global_enabled : {false, true}) {
                        segcore::storagev2translator::
                            SetStorageV2AsyncLoadEnabled(global_enabled);
                        for (const int workers : {1, 3}) {
                            storage::SetAsyncLoadThreadPoolSize(workers);
                            const auto changed = estimate();
                            EXPECT_EQ(changed.request.final_memory_cost,
                                      baseline.request.final_memory_cost);
                            EXPECT_EQ(changed.request.max_memory_cost,
                                      baseline.request.max_memory_cost);
                            EXPECT_EQ(changed.request.final_disk_cost,
                                      baseline.request.final_disk_cost);
                            EXPECT_EQ(changed.request.max_disk_cost,
                                      baseline.request.max_disk_cost);
                            EXPECT_EQ(changed.overhead.has_value(),
                                      baseline.overhead.has_value());
                            if (changed.overhead && baseline.overhead) {
                                EXPECT_EQ(changed.overhead->memory->group,
                                          baseline.overhead->memory->group);
                                EXPECT_EQ(
                                    changed.overhead->memory->max_runtime_unit,
                                    baseline.overhead->memory
                                        ->max_runtime_unit);
                            }
                        }
                    }
                }
            }
        }
    }
}

TEST(ScalarIndexV3ResourceTest, RTreeReservesHeapInsteadOfResidentFiles) {
    for (const int64_t rows : {0, 1, 16, 17, 64, 65, 257}) {
        std::vector<rtree_detail::Value> values;
        values.reserve(rows);
        for (int64_t i = 0; i < rows; ++i) {
            values.emplace_back(
                rtree_detail::Box(rtree_detail::Point(i, i),
                                  rtree_detail::Point(i + 1, i + 1)),
                i);
        }
        auto engine = RTreeQueryEngine::Create(std::move(values));
        auto state = RTreeIndexState::Create(
            engine, std::make_shared<const std::vector<size_t>>(), rows);
        ResourceFixture fixture;
        const auto path = fixture.WritePacked({{"tree.bgi", {0}}});
        const auto size = fixture.Size(path);
        for (const bool async : {false, true}) {
            fixture.context.use_async_load = async;
            for (const bool mmap : {false, true}) {
                const auto resources =
                    ScalarIndexFileLoadResource(DataType::GEOMETRY,
                                                size,
                                                V3Params(RTREE_INDEX_TYPE),
                                                mmap,
                                                rows,
                                                {path},
                                                fixture.context);
                EXPECT_GE(resources.request.final_memory_cost,
                          state->MemoryUsage());
                EXPECT_EQ(resources.request.final_disk_cost, 0);
                EXPECT_GE(resources.request.max_disk_cost, size);
                EXPECT_GT(resources.request.max_memory_cost,
                          resources.request.final_memory_cost);
                EXPECT_FALSE(resources.overhead.has_value());
            }
        }
    }
}

TEST(ScalarIndexV3ResourceTest, RTreeSaturationDoesNotEnableSharedScratch) {
    ResourceFixture fixture;
    fixture.context.use_async_load = true;
    const auto path = fixture.WritePacked({{"tree.bgi", {0}}});
    const auto resources =
        ScalarIndexFileLoadResource(DataType::GEOMETRY,
                                    fixture.Size(path),
                                    V3Params(RTREE_INDEX_TYPE),
                                    false,
                                    std::numeric_limits<int64_t>::max(),
                                    {path},
                                    fixture.context);
    EXPECT_EQ(resources.request.final_memory_cost,
              std::numeric_limits<uint64_t>::max());
    EXPECT_FALSE(resources.overhead.has_value());
}

TEST(ScalarIndexV3ResourceTest, BitmapFallbackReservesDenseState) {
    ResourceFixture fixture;
    constexpr size_t rows = 100003;
    constexpr size_t cardinality = DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND;
    const auto path = fixture.WritePacked(
        {{BITMAP_INDEX_DATA, std::vector<uint8_t>(128)}},
        {{INDEX_TYPE, static_cast<uint8_t>(ScalarIndexType::BITMAP)},
         {BITMAP_INDEX_LENGTH, cardinality},
         {BITMAP_INDEX_NUM_ROWS, rows}});
    const auto size = fixture.Size(path);
    const auto dense_bytes =
        (cardinality + 1) * TargetBitmap(rows).size_in_bytes();
    ASSERT_GT(dense_bytes, 16 * size);
    for (const bool async : {false, true}) {
        fixture.context.use_async_load = async;
        for (const bool mmap : {false, true}) {
            const auto resources =
                ScalarIndexFileLoadResource(DataType::INT32,
                                            size,
                                            V3Params(HYBRID_INDEX_TYPE),
                                            mmap,
                                            rows,
                                            {path},
                                            fixture.context);
            EXPECT_EQ(resources.request.final_memory_cost, size + dense_bytes);
            EXPECT_GE(resources.request.max_memory_cost,
                      resources.request.final_memory_cost + 128);
            EXPECT_EQ(resources.request.final_disk_cost, 0);
            EXPECT_FALSE(resources.request.has_raw_data);
            EXPECT_FALSE(resources.overhead.has_value());
        }
    }
}

TEST(ScalarIndexV3ResourceTest, EncryptedEstimatesRetainFullScratch) {
    ResourceFixture fixture;
    auto cipher =
        std::make_shared<milvus::test::CollectionBoundPlannerCipherPlugin>(1);
    auto& plugins = storage::PluginLoader::GetInstance();
    plugins.registerPluginForTest(cipher);
    auto& budget = storage::LoadAdmissionController::GetInstance();
    const auto old_bytes = budget.CapacityBytes();
    auto restore = folly::makeGuard([&] {
        budget.SetCapacityBytes(old_bytes);
        plugins.unregisterPluginForTest("CipherPlugin");
    });
    budget.SetCapacityBytes(1);
    const std::vector<uint8_t> data(4 * storage::kStreamSliceAlignment, 0x5a);
    for (const auto type :
         {ScalarIndexType::BITMAP, ScalarIndexType::INVERTED}) {
        const auto name =
            type == ScalarIndexType::BITMAP ? BITMAP_INDEX_DATA : "engine";
        const auto path =
            fixture.WritePacked({{name, data}},
                                {{INDEX_TYPE, static_cast<uint8_t>(type)},
                                 {BITMAP_INDEX_LENGTH, 1},
                                 {BITMAP_INDEX_NUM_ROWS, 10001}},
                                cipher);
        const auto size = fixture.Size(path);
        storage::MemFileManagerImpl manager(fixture.context);
        auto input = manager.OpenInputStream(path);
        auto reader = storage::IndexEntryReader::Open(input, input->Size(), 1);
        const auto meta_bytes = reader->Directory()
                                    .At(storage::MILVUS_V3_META_ENTRY_NAME)
                                    .plaintext_size;
        const auto full_scratch = 3 * (data.size() + meta_bytes);
        const auto max_slice = 3 * storage::kStreamSliceAlignment;
        ASSERT_GT(full_scratch, max_slice);
        ASSERT_LT(
            storage::EntryStreamMaxTransientBytes(full_scratch, max_slice),
            full_scratch);
        for (const bool async : {false, true}) {
            fixture.context.use_async_load = async;
            const auto resources =
                ScalarIndexFileLoadResource(DataType::VARCHAR,
                                            size,
                                            V3Params(HYBRID_INDEX_TYPE),
                                            false,
                                            10001,
                                            {path},
                                            fixture.context);
            EXPECT_GE(resources.request.max_memory_cost,
                      resources.request.final_memory_cost + full_scratch);
            EXPECT_EQ(resources.overhead.has_value(),
                      type == ScalarIndexType::INVERTED);
            if (resources.overhead) {
                EXPECT_GE(*resources.overhead->memory->max_runtime_unit,
                          max_slice);
            }
        }
    }
}

TEST(ScalarIndexV3ResourceTest, StringValidityStagingPreventsSyncSharing) {
    ResourceFixture fixture;
    constexpr size_t rows = 9;
    const auto path = fixture.WritePacked(
        {{"index_data", std::vector<uint8_t>(128)},
         {"valid_bitset", std::vector<uint8_t>((rows + 7) / 8)}},
        {{"version", 1}, {"num_rows", rows}});
    for (const bool async : {false, true}) {
        fixture.context.use_async_load = async;
        const auto resources =
            ScalarIndexFileLoadResource(DataType::VARCHAR,
                                        fixture.Size(path),
                                        V3Params(ASCENDING_SORT),
                                        false,
                                        rows,
                                        {path},
                                        fixture.context);
        EXPECT_EQ(resources.overhead.has_value(), async);
    }
}
TEST(ScalarIndexV3ResourceTest,
     TranslatorPropagatesSharedOverheadAndExplicitEstimate) {
    ResourceFixture fixture;
    fixture.context.use_async_load = true;
    fixture.context.set_for_loading_index(true);
    const auto path = fixture.WritePacked(
        {{"engine", std::vector<uint8_t>(8192)}},
        {{INDEX_TYPE, static_cast<uint8_t>(ScalarIndexType::INVERTED)}});
    segcore::LoadIndexInfo load_info{};
    load_info.collection_id = 1;
    load_info.partition_id = 2;
    load_info.segment_id = 3;
    load_info.field_id = 101;
    load_info.field_type = DataType::VARCHAR;
    load_info.element_type = DataType::NONE;
    load_info.enable_mmap = false;
    load_info.index_id = 1000;
    load_info.index_build_id = 1000;
    load_info.index_version = 10000;
    load_info.index_engine_version = 3;
    load_info.index_params = V3Params(HYBRID_INDEX_TYPE);
    load_info.index_files = {path};
    load_info.index_size = 1024;
    load_info.num_rows = 10001;
    load_info.dim = 0;
    load_info.warmup_policy = "disable";
    load_info.load_resource_request =
        LoadResourceRequest{/*max_memory_cost=*/2048,
                            /*max_disk_cost=*/512,
                            /*final_memory_cost=*/1024,
                            /*final_disk_cost=*/128,
                            /*has_raw_data=*/true};
    auto& budget = storage::LoadAdmissionController::GetInstance();
    const auto old_bytes = budget.CapacityBytes();
    auto restore =
        folly::makeGuard([&] { budget.SetCapacityBytes(old_bytes); });
    auto group =
        storage::LoadMemoryOverheadController::GetInstance().GetOrCreate();
    std::optional<int64_t> max_task;
    auto fs = std::make_shared<RecordingOpenFileSystem>(fixture.context.fs);
    fixture.context.fs = fs;
    auto check = [&] {
        fs->sync_opens = fs->async_opens = 0;
        // Construction inspects metadata and derives contracts, without engine initialization.
        segcore::storagev1translator::SealedIndexTranslator translator(
            &load_info,
            tracer::TraceContext{},
            fixture.context,
            Config(load_info.index_params));
        EXPECT_EQ(fs->sync_opens, 0);
        EXPECT_EQ(fs->async_opens, 1);
        EXPECT_EQ(translator.Family(), families::kInverted);
        const auto& overhead = translator.meta()->loading_overhead_config;
        ASSERT_TRUE(overhead.has_value());
        ASSERT_TRUE(overhead->memory.has_value());
        EXPECT_EQ(overhead->memory->group, group);
        ASSERT_TRUE(overhead->memory->max_runtime_unit.has_value());
        const auto current = *overhead->memory->max_runtime_unit;
        EXPECT_GT(current, 0);
        EXPECT_LE(current,
                  2 * storage::DefaultStreamSliceSize() +
                      2 * storage::FileWriter::ALIGNMENT_MASK);
        EXPECT_LT(current,
                  storage::MaxEntryStreamTaskBytes() *
                      storage::kFileStreamBufferMultiplier);
        if (max_task)
            EXPECT_EQ(current, *max_task);
        max_task = current;
        EXPECT_FALSE(overhead->file.has_value());
        const auto [loaded, transient] =
            translator.estimated_byte_size_of_cell(0);
        EXPECT_EQ(loaded, (cachinglayer::ResourceUsage{1024, 128}));
        EXPECT_EQ(transient, (cachinglayer::ResourceUsage{1024, 896}));
    };
    budget.SetCapacityBytes(0);
    check();
    budget.SetCapacityBytes(storage::kTailMergeGrace);
    check();
    budget.SetCapacityBytes(0);
    auto& plugins = storage::PluginLoader::GetInstance();
    plugins.registerPluginForTest(
        std::make_shared<milvus::test::PlannerCipherPlugin>());
    auto remove_plugin = folly::makeGuard(
        [&] { plugins.unregisterPluginForTest("CipherPlugin"); });
    check();
}

TEST(ScalarIndexV3ResourceTest, BitmapTranslatorKeepsRequestLocalOverhead) {
    ResourceFixture fixture;
    fixture.context.use_async_load = true;
    const auto path = fixture.WritePacked(
        {{BITMAP_INDEX_DATA, std::vector<uint8_t>(128)}},
        {{INDEX_TYPE, static_cast<uint8_t>(ScalarIndexType::BITMAP)},
         {BITMAP_INDEX_LENGTH, 1},
         {BITMAP_INDEX_NUM_ROWS, 10001}});
    segcore::LoadIndexInfo load_info{};
    load_info.collection_id = 1;
    load_info.partition_id = 2;
    load_info.segment_id = 3;
    load_info.field_id = 101;
    load_info.field_type = DataType::INT64;
    load_info.element_type = DataType::NONE;
    load_info.index_engine_version = 3;
    load_info.index_params = V3Params(HYBRID_INDEX_TYPE);
    load_info.index_files = {path};
    load_info.index_size = fixture.Size(path);
    load_info.num_rows = 10001;
    load_info.warmup_policy = "disable";
    auto fs = std::make_shared<RecordingOpenFileSystem>(fixture.context.fs);
    fixture.context.fs = fs;
    for (bool use_async : {false, true}) {
        SCOPED_TRACE(use_async);
        fixture.context.use_async_load = use_async;
        fs->sync_opens = fs->async_opens = 0;
        segcore::storagev1translator::SealedIndexTranslator translator(
            &load_info,
            tracer::TraceContext{},
            fixture.context,
            Config(load_info.index_params));
        EXPECT_EQ(translator.Family(), families::kBitmap);
        EXPECT_FALSE(translator.meta()->loading_overhead_config.has_value());
        const auto [loaded, transient] =
            translator.estimated_byte_size_of_cell(0);
        EXPECT_GT(loaded.memory_bytes, 0);
        EXPECT_GT(transient.memory_bytes, 0);
        EXPECT_EQ(fs->sync_opens, use_async ? 0 : 1);
        EXPECT_EQ(fs->async_opens, use_async ? 1 : 0);
    }
}
}  // namespace milvus::index
