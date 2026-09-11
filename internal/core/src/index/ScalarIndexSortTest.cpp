#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "bitset/bitset.h"
#include "common/Tracer.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "common/Slice.h"
#include "folly/coro/BlockingWait.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "index/IndexFactory.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "index/ScalarIndexSort.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/IndexMaterializer.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/AsyncLoadTestUtils.h"
#include "test_utils/Constants.h"
#include "test_utils/TmpPath.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::index;

namespace {

class ExposedScalarIndexSort : public ScalarIndexSort<int64_t> {
 public:
    using ScalarIndexSort<int64_t>::ScalarIndexSort;

    void
    LoadDirectForTest(storage::AsyncIndexEntryReader& reader,
                      const Config& config,
                      proto::common::LoadPriority priority) {
        auto plan = PlanLoad(reader.Catalog(), config);
        plan.priority = priority;
        auto artifact = folly::coro::blockingWait(
            storage::MaterializeIndexAsync(reader, std::move(plan)));
        folly::coro::blockingWait(FinalizeLoad(artifact, config));
        artifact.CommitTargets();
    }
};

struct ScalarSortAsyncLoadFixture {
    explicit ScalarSortAsyncLoadFixture(std::string test_name)
        : root_path(TestLocalPath + "/" + std::move(test_name)) {
        boost::filesystem::remove_all(root_path);
        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path;
        chunk_manager = storage::CreateChunkManager(storage_config);
        fs = storage::InitArrowFileSystem(storage_config);

        field_schema.set_data_type(proto::schema::DataType::Int64);
        field_meta = storage::FieldDataMeta{1, 2, 3, 101, field_schema};
        index_meta = storage::IndexMeta{3, 101, 1000, 10000};
        ctx = storage::FileManagerContext(
            field_meta, index_meta, chunk_manager, fs);
    }

    ~ScalarSortAsyncLoadFixture() {
        boost::filesystem::remove_all(root_path);
    }

    std::string root_path;
    proto::schema::FieldSchema field_schema;
    storage::FieldDataMeta field_meta;
    storage::IndexMeta index_meta;
    storage::ChunkManagerPtr chunk_manager;
    milvus_storage::ArrowFileSystemPtr fs;
    storage::FileManagerContext ctx;
};

}  // namespace

static storage::FileManagerContext
CreateScalarSortTestFileManagerContext() {
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FieldDataMeta field_meta{1, 2, 3, 101};
    field_meta.field_schema.set_data_type(proto::schema::DataType::Int64);
    storage::IndexMeta index_meta{3, 101, 1000, 10000};
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);
    return ctx;
}

TEST(ScalarIndexSortV3AsyncLoadTest, MemoryPathUsesNativeDirectEntryReads) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    ScalarSortAsyncLoadFixture fixture("scalar_sort_async_memory");
    std::vector<int64_t> data{50, 10, 30, 20, 40};

    ExposedScalarIndexSort build_index(fixture.ctx);
    build_index.Build(data.size(), data.data());
    auto stats = build_index.UploadUnified({});

    milvus::test::ControlledDirectReadFile* remote_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(
        milvus::test::ReadPackedIndexBytes(fixture.ctx, stats->GetIndexFiles()),
        &remote_file);
    auto read_at_calls_after_open = remote_file->ReadAtCalls();

    ExposedScalarIndexSort load_index(fixture.ctx);
    Config config;
    config[milvus::index::ENABLE_MMAP] = false;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    load_index.LoadDirectForTest(
        *reader, config, milvus::proto::common::LoadPriority::HIGH);

    EXPECT_GE(remote_file->DirectReadCalls().size(), 3);
    EXPECT_EQ(remote_file->AsyncReadCalls(), 0);
    EXPECT_EQ(remote_file->ReadAtCalls(), read_at_calls_after_open);
    ASSERT_EQ(load_index.Count(), data.size());
    auto bitset = load_index.Range(
        static_cast<int64_t>(20), true, static_cast<int64_t>(40), true);
    EXPECT_FALSE(bitset[0]);
    EXPECT_FALSE(bitset[1]);
    EXPECT_TRUE(bitset[2]);
    EXPECT_TRUE(bitset[3]);
    EXPECT_TRUE(bitset[4]);
    EXPECT_EQ(load_index.Reverse_Lookup(0), data[0]);
}

TEST(ScalarIndexSortV3AsyncLoadTest, MmapPathUsesNativeDirectEntryReads) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    ScalarSortAsyncLoadFixture fixture("scalar_sort_async_mmap");
    std::vector<int64_t> data{5, 4, 3, 2, 1, 0};

    ExposedScalarIndexSort build_index(fixture.ctx);
    build_index.Build(data.size(), data.data());
    auto stats = build_index.UploadUnified({});

    milvus::test::ControlledDirectReadFile* remote_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(
        milvus::test::ReadPackedIndexBytes(fixture.ctx, stats->GetIndexFiles()),
        &remote_file);
    auto read_at_calls_after_open = remote_file->ReadAtCalls();

    ExposedScalarIndexSort load_index(fixture.ctx);
    Config config;
    config[milvus::index::ENABLE_MMAP] = true;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    load_index.LoadDirectForTest(
        *reader, config, milvus::proto::common::LoadPriority::HIGH);

    EXPECT_GE(remote_file->DirectReadCalls().size(), 3);
    EXPECT_EQ(remote_file->AsyncReadCalls(), 0);
    EXPECT_EQ(remote_file->ReadAtCalls(), read_at_calls_after_open);
    ASSERT_EQ(load_index.Count(), data.size());
    std::vector<int64_t> values{0, 5};
    auto bitset = load_index.In(values.size(), values.data());
    EXPECT_TRUE(bitset[0]);
    EXPECT_FALSE(bitset[1]);
    EXPECT_FALSE(bitset[2]);
    EXPECT_FALSE(bitset[3]);
    EXPECT_FALSE(bitset[4]);
    EXPECT_TRUE(bitset[5]);
    EXPECT_EQ(load_index.Reverse_Lookup(5), data[5]);
}

void
test_stlsort_for_range(
    const std::vector<int64_t>& data,
    DataType data_type,
    bool enable_mmap,
    std::function<TargetBitmap(
        const std::shared_ptr<ScalarIndexSort<int64_t>>&)> exec_expr,
    const std::vector<bool>& expected_result) {
    size_t nb = data.size();
    std::vector<std::string> index_files;
    {
        Config config;

        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>(
            CreateScalarSortTestFileManagerContext());
        index->Build(nb, data.data());

        auto create_index_result = index->UploadUnified({});
        index_files = create_index_result->GetIndexFiles();
    }
    {
        Config config;
        config[milvus::index::ENABLE_MMAP] = enable_mmap;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        config["index_files"] = index_files;

        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>(
            CreateScalarSortTestFileManagerContext());
        index->LoadUnified(config);

        auto cnt = index->Count();
        ASSERT_EQ(cnt, nb);
        auto bitset = exec_expr(index);
        for (size_t i = 0; i < nb; i++) {
            ASSERT_EQ(bitset[i], expected_result[i]);
        }
    }
}
TEST(StlSortIndexTest, TestRange) {
    std::vector<int64_t> data = {10, 2, 6, 5, 9, 3, 7, 8, 4, 1};
    {
        std::vector<bool> expected_result = {
            false, false, true, true, false, true, true, false, true, false};
        auto exec_expr =
            [](const std::shared_ptr<ScalarIndexSort<int64_t>>& index) {
                return index->Range(3, true, 7, true);
            };

        test_stlsort_for_range(
            data, DataType::INT64, false, exec_expr, expected_result);

        test_stlsort_for_range(
            data, DataType::INT64, true, exec_expr, expected_result);
    }

    {
        std::vector<bool> expected_result(data.size(), false);
        auto exec_expr =
            [](const std::shared_ptr<ScalarIndexSort<int64_t>>& index) {
                return index->Range(10, false, 70, true);
            };

        test_stlsort_for_range(
            data, DataType::INT64, false, exec_expr, expected_result);

        test_stlsort_for_range(
            data, DataType::INT64, true, exec_expr, expected_result);
    }
}

TEST(StlSortIndexTest, TestIn) {
    std::vector<int64_t> data = {10, 2, 6, 5, 9, 3, 7, 8, 4, 1};
    std::vector<bool> expected_result = {
        false, false, false, true, false, true, true, false, false, false};

    std::vector<int64_t> values = {3, 5, 7};

    auto exec_expr =
        [&values](const std::shared_ptr<ScalarIndexSort<int64_t>>& index) {
            return index->In(values.size(), values.data());
        };
    test_stlsort_for_range(
        data, DataType::INT64, false, exec_expr, expected_result);

    test_stlsort_for_range(
        data, DataType::INT64, true, exec_expr, expected_result);
}

TEST(StlSortIndexTest, MmapByteSizeCountsValidBitsetOnce) {
    constexpr size_t kAlignment = 32;
    constexpr uint64_t kMmapIndexPadding = 1;
    const std::vector<int64_t> data = {
        10, 2, 6, 5, 9, 3, 7, 8, 4, 1, 11, 12, 13};

    std::vector<std::string> index_files;
    {
        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>(
            CreateScalarSortTestFileManagerContext());
        index->Build(data.size(), data.data());

        auto create_index_result = index->UploadUnified({});
        index_files = create_index_result->GetIndexFiles();
    }

    auto index = std::make_shared<index::ScalarIndexSort<int64_t>>(
        CreateScalarSortTestFileManagerContext());
    Config config;
    config[milvus::index::ENABLE_MMAP] = true;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    config["index_files"] = index_files;
    index->LoadUnified(config);

    auto index_data_bytes = data.size() * sizeof(IndexStructure<int64_t>);
    auto aligned_data_bytes =
        ((index_data_bytes + kAlignment - 1) / kAlignment) * kAlignment;
    TargetBitmap valid_bitset(data.size(), true);
    auto expected_byte_size = aligned_data_bytes + kMmapIndexPadding +
                              data.size() * sizeof(int32_t) +
                              valid_bitset.size_in_bytes();

    ASSERT_EQ(index->ByteSize(), static_cast<int64_t>(expected_byte_size));
}

// V2 compat test removed: kScalarIndexUseV3 flag deleted,
// Upload()/Load() now always route to V3 paths.

namespace {

class ScalarIndexSortLegacyAsyncLoadTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        old_enabled_ =
            segcore::storagev2translator::StorageV2AsyncLoadEnabled();
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(true);
        old_threads_ = storage::GetAsyncLoadThreadPoolSize();
        storage::SetAsyncLoadThreadPoolSize(1);
        old_slots_ =
            storage::LoadAdmissionController::GetInstance().CapacitySlots();
        storage::LoadAdmissionController::GetInstance().SetCapacitySlots(1);
    }
    void
    TearDown() override {
        storage::LoadAdmissionController::GetInstance().SetCapacitySlots(
            old_slots_);
        storage::SetAsyncLoadThreadPoolSize(old_threads_);
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(
            old_enabled_);
    }
    std::vector<std::string>
    Persist(const BinarySet& binary, bool sliced) {
        std::vector<std::string> paths;
        auto write = [&](const std::string& name,
                         const uint8_t* data,
                         size_t size) {
            storage::IndexData codec(data, size);
            codec.SetFieldDataMeta(fixture_.field_meta);
            codec.set_index_meta(fixture_.index_meta);
            auto encoded = codec.Serialize(storage::StorageType::Remote);
            // Deliberately use an old arbitrary prefix, not a newly derived path.
            const auto path = fixture_.root_path + "/old-prefix/" + name;
            fixture_.chunk_manager->Write(path, encoded.data(), encoded.size());
            paths.push_back(path);
        };
        for (const auto& [name, data] : binary.binary_map_) {
            if (sliced && name == "index_data" && data->size > 1) {
                const auto first = data->size / 2;
                write(GenSlicedFileName(name, 0), data->data.get(), first);
                write(GenSlicedFileName(name, 1),
                      data->data.get() + first,
                      data->size - first);
                Config meta;
                meta[META] = Config::array(
                    {{{NAME, name}, {SLICE_NUM, 2}, {TOTAL_LEN, data->size}}});
                const auto text = meta.dump();
                write(INDEX_FILE_SLICE_META,
                      reinterpret_cast<const uint8_t*>(text.data()),
                      text.size());
            } else {
                write(name, data->data.get(), data->size);
            }
        }
        return paths;
    }
    Config
    ConfigFor(const std::vector<std::string>& files) {
        return {{INDEX_FILES, files},
                {ENABLE_MMAP, false},
                {SCALAR_INDEX_ENGINE_VERSION, 2},
                {LOAD_PRIORITY, proto::common::LoadPriority::LOW}};
    }
    ScalarSortAsyncLoadFixture fixture_{"scalar_sort_legacy_stream"};
    bool old_enabled_{};
    int old_threads_{};
    size_t old_slots_{};
};

TEST_F(ScalarIndexSortLegacyAsyncLoadTest,
       SlicedAndUnslicedNullableQueryParity) {
    const std::vector<int64_t> values{30, 10, 20, 10, 50};
    const bool valid[]{true, false, true, true, false};
    ScalarIndexSort<int64_t> build(fixture_.ctx);
    build.Build(values.size(), values.data(), valid);
    for (bool sliced : {false, true}) {
        auto files = Persist(build.Serialize({}), sliced);
        for (bool enabled : {false, true}) {
            segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(enabled);
            for (bool arrow : {false, true}) {
                auto context =
                    arrow ? fixture_.ctx
                          : storage::FileManagerContext(fixture_.field_meta,
                                                        fixture_.index_meta,
                                                        fixture_.chunk_manager,
                                                        nullptr);
                ScalarIndexSort<int64_t> loaded(context);
                IndexBase& base = loaded;
                OpContext op_ctx;
                base.Load(tracer::TraceContext{}, ConfigFor(files), &op_ctx);
                EXPECT_EQ(loaded.Count(), values.size());
                EXPECT_EQ(loaded.IsNull().count(), 2);
                const int64_t needle = 10;
                const auto hits = loaded.In(1, &needle);
                EXPECT_EQ(hits.count(), 1);
                EXPECT_TRUE(hits[3]);
                const auto misses = loaded.NotIn(1, &needle);
                EXPECT_EQ(misses.count(), 2);
                EXPECT_FALSE(misses[1]);
                EXPECT_FALSE(misses[4]);
                EXPECT_EQ(loaded.Reverse_Lookup(0), 30);
            }
        }
    }
}

TEST_F(ScalarIndexSortLegacyAsyncLoadTest, AllNullPayloadAndOldMetadata) {
    const int64_t values[]{30, 10};
    const bool valid[]{false, false};
    ScalarIndexSort<int64_t> build(fixture_.ctx);
    build.Build(2, values, valid);
    auto binary = build.Serialize({});
    binary.Erase("is_nested_index");
    ScalarIndexSort<int64_t> loaded(fixture_.ctx);
    loaded.Load(
        tracer::TraceContext{}, ConfigFor(Persist(binary, false)), nullptr);
    EXPECT_EQ(loaded.Count(), 2);
    EXPECT_EQ(loaded.IsNull().count(), 2);
}

TEST_F(ScalarIndexSortLegacyAsyncLoadTest, MissingSliceAndInvalidSortSizeFail) {
    const int64_t values[]{30, 10, 20};
    ScalarIndexSort<int64_t> build(fixture_.ctx);
    build.Build(3, values);
    auto binary = build.Serialize({});
    auto files = Persist(binary, true);
    files.erase(std::remove_if(files.begin(),
                               files.end(),
                               [](const auto& path) {
                                   return path.ends_with(
                                       GenSlicedFileName("index_data", 1));
                               }),
                files.end());
    ScalarIndexSort<int64_t> missing(fixture_.ctx);
    EXPECT_THROW(
        missing.Load(tracer::TraceContext{}, ConfigFor(files), nullptr),
        SegcoreError);
    auto length = binary.GetByName("index_length");
    length->size = 1;
    ScalarIndexSort<int64_t> invalid(fixture_.ctx);
    EXPECT_THROW(
        invalid.Load(
            tracer::TraceContext{}, ConfigFor(Persist(binary, false)), nullptr),
        SegcoreError);
}

TEST_F(ScalarIndexSortLegacyAsyncLoadTest,
       CancellationReachesLegacyLoadThroughIndexBase) {
    ScalarIndexSort<int64_t> index(fixture_.ctx);
    IndexBase& base = index;
    folly::CancellationSource source;
    source.requestCancellation();
    OpContext ctx;
    ctx.cancellation_token = source.getToken();
    try {
        base.Load(tracer::TraceContext{}, ConfigFor({"not-opened"}), &ctx);
        FAIL() << "expected cancellation before open";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FollyCancel);
    }
}

TEST_F(ScalarIndexSortLegacyAsyncLoadTest,
       EstimateIncludesRetainedInputAndSingleFileScratch) {
    const int64_t values[]{30, 10, 20};
    ScalarIndexSort<int64_t> build(fixture_.ctx);
    build.Build(3, values);
    auto files = Persist(build.Serialize({}), true);
    size_t payload_bytes = 0;
    size_t max_transient_bytes = 0;
    for (const auto& file : files) {
        auto input = storage::OpenLegacyIndexInput(
            fixture_.ctx.chunkManagerPtr, fixture_.ctx.fs, file);
        const auto info = folly::coro::blockingWait(
            storage::InspectLegacyIndexFileAsync(
                *input, proto::common::LoadPriority::HIGH)
                .scheduleOn(storage::ResolveAsyncLoadExecutor(
                    {}, proto::common::LoadPriority::HIGH)));
        payload_bytes += info.payload_bytes;
        max_transient_bytes =
            std::max(max_transient_bytes, info.max_transient_bytes);
    }
    const auto resource =
        IndexFactory::GetInstance().ScalarIndexFileLoadResource(
            DataType::INT64,
            1,
            {{"index_type", ASCENDING_SORT},
             {SCALAR_INDEX_ENGINE_VERSION, "2"}},
            false,
            3,
            files,
            fixture_.ctx);
    const auto legacy = IndexFactory::GetInstance().ScalarIndexLoadResource(
        DataType::INT64,
        0,
        1,
        {{"index_type", ASCENDING_SORT}, {SCALAR_INDEX_ENGINE_VERSION, "2"}},
        false,
        3,
        files,
        fixture_.ctx);
    EXPECT_EQ(legacy.max_memory_cost, resource.request.max_memory_cost);
    EXPECT_EQ(legacy.final_memory_cost, resource.request.final_memory_cost);
    EXPECT_FALSE(resource.overhead.has_value());
    EXPECT_GE(resource.request.final_memory_cost, payload_bytes);
    EXPECT_GE(resource.request.max_memory_cost,
              resource.request.final_memory_cost + payload_bytes +
                  max_transient_bytes);
}
}  // namespace
