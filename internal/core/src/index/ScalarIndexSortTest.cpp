#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <random>
#include <unordered_set>
#include <utility>
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
#include "index/SortedInt64Lookup.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
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
        auto plan = PlanLoad(reader.Directory(), reader.IndexMeta(), config);
        folly::coro::blockingWait(
            reader.ReadEntriesAsync(plan.entries, priority));
        folly::coro::blockingWait(FinishLoadAsync(plan, config));
        plan.Commit();
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

namespace {

std::vector<std::vector<int64_t>>
MembershipQueries() {
    constexpr auto min = std::numeric_limits<int64_t>::min();
    constexpr auto max = std::numeric_limits<int64_t>::max();
    constexpr auto threshold =
        milvus::index::detail::kSortedInt64BatchThreshold;
    std::vector<std::vector<int64_t>> queries{
        {}, {min}, {max}, {0}, {17, -17}, {min, max, 0, 0}};
    std::mt19937_64 rng(53853);
    for (size_t n : {threshold - 1, threshold, threshold + 1, size_t{4096}}) {
        // Duplicate hits, all misses on either side, and mixed sparse probes.
        queries.emplace_back(n, 0);
        queries.emplace_back(n, -1000000);
        queries.emplace_back(n, 1000000);
        std::vector<int64_t> mixed(n);
        for (size_t i = 0; i < n; ++i) {
            mixed[i] = static_cast<int64_t>(rng() % 8193) - 4096;
        }
        mixed[0] = min;
        mixed[1] = max;
        mixed[2] = 0;
        mixed[3] = 0;
        queries.push_back(mixed);
        std::sort(mixed.begin(), mixed.end());
        queries.push_back(mixed);
        std::reverse(mixed.begin(), mixed.end());
        queries.push_back(std::move(mixed));
    }
    return queries;
}

void
CheckMembership(ScalarIndexSort<int64_t>& index,
                const std::vector<int64_t>& rows,
                const bool* valid) {
    ASSERT_EQ(index.Count(), rows.size());
    auto queries = MembershipQueries();
    queries.push_back(rows);  // Full coverage, including repeated index values.
    for (const auto& query : queries) {
        SCOPED_TRACE("query size=" + std::to_string(query.size()));
        const auto original = query;
        const auto* values = query.empty() ? nullptr : query.data();
        const auto in = index.In(query.size(), values);
        const auto not_in = index.NotIn(query.size(), values);
        ASSERT_EQ(in.size(), rows.size());
        ASSERT_EQ(not_in.size(), rows.size());
        const std::unordered_set<int64_t> terms(query.begin(), query.end());
        for (size_t row = 0; row < rows.size(); ++row) {
            const bool hit = terms.count(rows[row]) != 0;
            const bool is_valid = valid == nullptr || valid[row];
            ASSERT_EQ(in[row], is_valid && hit) << "row=" << row;
            ASSERT_EQ(not_in[row], is_valid && !hit) << "row=" << row;
        }
        EXPECT_EQ(query, original);
    }
}

std::vector<int64_t>
MembershipRows(size_t n, size_t cardinality) {
    std::vector<int64_t> rows(n);
    for (size_t i = 0; i < n; ++i) {
        rows[i] = 2 * static_cast<int64_t>(i % cardinality) -
                  static_cast<int64_t>(cardinality);
    }
    rows.front() = std::numeric_limits<int64_t>::min();
    rows.back() = std::numeric_limits<int64_t>::max();
    std::mt19937_64 rng(53853);
    std::shuffle(rows.begin(), rows.end(), rng);
    return rows;
}

}  // namespace

TEST(ScalarIndexSortMembershipTest, MatchesScanAcrossCardinalityAndNulls) {
    for (size_t n : {1, 2, 63, 64, 65, 127, 128, 129, 2049}) {
        for (size_t cardinality : {size_t{2}, size_t{32}, n}) {
            const auto rows = MembershipRows(n, cardinality);
            for (int null_mode : {0, 1, 2}) {
                SCOPED_TRACE("rows=" + std::to_string(n) +
                             " cardinality=" + std::to_string(cardinality) +
                             " null_mode=" + std::to_string(null_mode));
                auto valid = std::make_unique<bool[]>(n);
                for (size_t i = 0; i < n; ++i) {
                    valid[i] = null_mode == 0 || (null_mode == 1 && i % 3 != 0);
                }
                const auto* validity = null_mode == 0 ? nullptr : valid.get();
                ScalarIndexSort<int64_t> index;
                index.Build(n, rows.data(), validity);
                CheckMembership(index, rows, validity);
            }
        }
    }
}

TEST(ScalarIndexSortMembershipTest, LargeListWithFewNonNullEntries) {
    const auto rows = MembershipRows(4096, 32);
    auto valid = std::make_unique<bool[]>(rows.size());
    valid[0] = valid[rows.size() / 2] = valid[rows.size() - 1] = true;
    ScalarIndexSort<int64_t> index;
    index.Build(rows.size(), rows.data(), valid.get());
    ASSERT_EQ(index.Size(), 3);
    CheckMembership(index, rows, valid.get());
}

TEST(ScalarIndexSortMembershipTest, MatchesScanAfterLegacyAndPackedReloads) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    ScalarSortAsyncLoadFixture fixture("scalar_sort_membership_reload");
    auto rows = MembershipRows(257, 32);
    auto valid = std::make_unique<bool[]>(rows.size());
    for (bool all_null : {false, true}) {
        SCOPED_TRACE("all_null=" + std::to_string(all_null));
        for (size_t i = 0; i < rows.size(); ++i) {
            valid[i] = !all_null && i % 3 != 0;
        }
        ScalarIndexSort<int64_t> built(fixture.ctx);
        built.Build(rows.size(), rows.data(), valid.get());
        CheckMembership(built, rows, valid.get());
        const auto stats = built.UploadUnified({});
        for (bool mmap : {false, true}) {
            SCOPED_TRACE("mmap=" + std::to_string(mmap));
            Config config;
            config[ENABLE_MMAP] = mmap;
            config[milvus::LOAD_PRIORITY] = proto::common::LoadPriority::HIGH;
            config[INDEX_FILES] = stats->GetIndexFiles();
            for (bool async : {false, true}) {
                SCOPED_TRACE("async=" + std::to_string(async));
                auto ctx = fixture.ctx;
                ctx.use_async_load = async;
                ScalarIndexSort<int64_t> loaded(ctx);
                loaded.LoadUnified(config);
                CheckMembership(loaded, rows, valid.get());
            }
            ScalarIndexSort<int64_t> legacy(fixture.ctx);
            legacy.Load(built.Serialize({}), config);
            CheckMembership(legacy, rows, valid.get());
        }
    }
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
