#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <random>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/Tracer.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "index/ScalarIndexSort.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/TmpPath.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::index;

namespace {

class RecordingIndexEntryWriter : public storage::IndexEntryWriter {
 public:
    void
    WriteEntry(const std::string& name, const void*, size_t size) override {
        names_.insert(name);
        total_bytes_ += size;
    }

    void
    WriteEntry(const std::string& name, int, size_t size) override {
        names_.insert(name);
        total_bytes_ += size;
    }

    void
    Finish() override {
    }

    size_t
    GetTotalBytesWritten() const override {
        return total_bytes_;
    }

    bool
    HasEntry(const std::string& name) const {
        return names_.count(name) != 0;
    }

 private:
    std::unordered_set<std::string> names_;
    size_t total_bytes_{0};
};

class MemoryOutputStream : public OutputStream {
 public:
    explicit MemoryOutputStream(std::shared_ptr<std::vector<uint8_t>> data)
        : data_(std::move(data)) {
    }

    size_t
    Tell() const override {
        return data_->size();
    }

    size_t
    Write(const void* data, size_t size) override {
        const auto* bytes = static_cast<const uint8_t*>(data);
        data_->insert(data_->end(), bytes, bytes + size);
        return size;
    }

    size_t
    Write(int, size_t) override {
        return 0;
    }

    void
    Close() override {
    }

 private:
    std::shared_ptr<std::vector<uint8_t>> data_;
};

class FailingMemoryInputStream : public InputStream {
 public:
    FailingMemoryInputStream(std::shared_ptr<const std::vector<uint8_t>> data,
                             size_t failing_offset)
        : data_(std::move(data)), failing_offset_(failing_offset) {
    }

    size_t
    Size() const override {
        return data_->size();
    }

    bool
    Seek(int64_t offset) override {
        if (offset < 0 || static_cast<size_t>(offset) > data_->size()) {
            return false;
        }
        position_ = static_cast<size_t>(offset);
        return true;
    }

    size_t
    Tell() const override {
        return position_;
    }

    bool
    Eof() const override {
        return position_ == data_->size();
    }

    size_t
    Read(void* data, size_t size) override {
        auto bytes_read = ReadAt(data, position_, size);
        position_ += bytes_read;
        return bytes_read;
    }

    size_t
    ReadAt(void* data, size_t offset, size_t size) override {
        if (offset == failing_offset_ || offset >= data_->size()) {
            return 0;
        }
        auto bytes_read = std::min(size, data_->size() - offset);
        std::memcpy(data, data_->data() + offset, bytes_read);
        return bytes_read;
    }

    size_t
    Read(int, size_t) override {
        return 0;
    }

 private:
    std::shared_ptr<const std::vector<uint8_t>> data_;
    size_t failing_offset_;
    size_t position_{0};
};

std::shared_ptr<std::vector<uint8_t>>
BuildPackedScalarSortIndex(const std::vector<int64_t>& values) {
    auto packed_data = std::make_shared<std::vector<uint8_t>>();
    ScalarIndexSort<int64_t> source;
    source.Build(values.size(), values.data());
    auto output = std::make_shared<MemoryOutputStream>(packed_data);
    storage::IndexEntryDirectStreamWriter writer(output);
    source.WriteEntries(&writer);
    writer.Finish();
    return packed_data;
}

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

namespace {

struct ScalarSortMembershipLoadFixture {
    explicit ScalarSortMembershipLoadFixture(
        std::string test_name,
        proto::schema::DataType type = proto::schema::DataType::Int64)
        : root_path(TestLocalPath + "/" + std::move(test_name)) {
        std::filesystem::remove_all(root_path);
        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path;
        chunk_manager = storage::CreateChunkManager(storage_config);
        fs = storage::InitArrowFileSystem(storage_config);

        field_schema.set_data_type(type);
        field_meta = storage::FieldDataMeta{1, 2, 3, 101, field_schema};
        index_meta = storage::IndexMeta{3, 101, 1000, 10000};
        ctx = storage::FileManagerContext(
            field_meta, index_meta, chunk_manager, fs);
    }

    ~ScalarSortMembershipLoadFixture() {
        std::filesystem::remove_all(root_path);
    }

    std::string root_path;
    proto::schema::FieldSchema field_schema;
    storage::FieldDataMeta field_meta;
    storage::IndexMeta index_meta;
    storage::ChunkManagerPtr chunk_manager;
    milvus_storage::ArrowFileSystemPtr fs;
    storage::FileManagerContext ctx;
};

std::vector<std::vector<int64_t>>
MembershipQueries() {
    constexpr auto min = std::numeric_limits<int64_t>::min();
    constexpr auto max = std::numeric_limits<int64_t>::max();
    constexpr size_t threshold = 128;
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
    ScalarSortMembershipLoadFixture fixture("scalar_sort_membership_reload");
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
            ScalarIndexSort<int64_t> loaded(fixture.ctx);
            loaded.LoadUnified(config);
            CheckMembership(loaded, rows, valid.get());
            ScalarIndexSort<int64_t> legacy(fixture.ctx);
            legacy.Load(built.Serialize({}), config);
            CheckMembership(legacy, rows, valid.get());
        }
    }
}

namespace {

template <typename T>
proto::schema::DataType
MembershipDataType() {
    if constexpr (std::is_same_v<T, bool>)
        return proto::schema::Bool;
    if constexpr (std::is_same_v<T, int8_t>)
        return proto::schema::Int8;
    if constexpr (std::is_same_v<T, int16_t>)
        return proto::schema::Int16;
    if constexpr (std::is_same_v<T, int32_t>)
        return proto::schema::Int32;
    if constexpr (std::is_same_v<T, int64_t>)
        return proto::schema::Int64;
    if constexpr (std::is_same_v<T, float>)
        return proto::schema::Float;
    return proto::schema::Double;
}

template <typename T>
std::vector<T>
TypedMembershipRows(size_t n) {
    std::vector<T> rows(n);
    for (size_t i = 0; i < n; ++i) {
        if constexpr (std::is_same_v<T, bool>) {
            rows[i] = i % 2 != 0;
        } else {
            rows[i] = static_cast<T>(2 * static_cast<int>(i % 31) - 30);
        }
    }
    if (n >= 8) {
        rows[0] = std::numeric_limits<T>::lowest();
        rows[1] = std::numeric_limits<T>::max();
        if constexpr (std::is_floating_point_v<T>) {
            rows[2] = -std::numeric_limits<T>::infinity();
            rows[3] = std::numeric_limits<T>::infinity();
            rows[4] = T(-0.0);
            rows[5] = T(0.0);
            rows[6] = std::numeric_limits<T>::denorm_min();
            rows[7] = -std::numeric_limits<T>::denorm_min();
            if (n >= 10) {
                rows[8] = T(1.25);
                rows[9] = std::nextafter(T(1.25), T(2));
            }
        }
    }
    return rows;
}

// A real bool[] also exercises ScalarIndexSort<bool>'s pointer API, without
// relying on the packed vector<bool> specialization. Used for every type.
template <typename T>
std::unique_ptr<T[]>
MembershipBuffer(const std::vector<T>& values) {
    auto buffer = std::make_unique<T[]>(values.size());
    std::copy(values.begin(), values.end(), buffer.get());
    return buffer;
}

template <typename T>
void
CheckTypedMembership(ScalarIndexSort<T>& index,
                     const std::vector<T>& rows,
                     const bool* valid) {
    std::vector<std::vector<T>> queries{{},
                                        {T(0)},
                                        {T(1)},
                                        {T(1), T(0), T(1)},
                                        rows,
                                        std::vector<T>(129, T(0)),
                                        std::vector<T>(4096, T(1))};
    auto reverse = rows;
    std::reverse(reverse.begin(), reverse.end());
    queries.push_back(reverse);
    if constexpr (!std::is_same_v<T, bool>) {
        queries.push_back({std::numeric_limits<T>::lowest(),
                           std::numeric_limits<T>::max(),
                           T(-31),
                           T(31)});
    }
    if constexpr (std::is_floating_point_v<T>) {
        queries.push_back({T(-0.0), T(0.0), T(-0.0)});
        queries.push_back({-std::numeric_limits<T>::infinity(),
                           std::numeric_limits<T>::infinity()});
        queries.push_back({std::numeric_limits<T>::denorm_min(),
                           -std::numeric_limits<T>::denorm_min()});
        queries.push_back({T(1.25),
                           std::nextafter(T(1.25), T(2)),
                           std::nextafter(T(1.25), T(0))});
        queries.push_back({std::numeric_limits<T>::quiet_NaN()});
        queries.push_back({T(31),
                           std::numeric_limits<T>::quiet_NaN(),
                           T(-0.0),
                           std::numeric_limits<T>::quiet_NaN()});
    }
    ASSERT_EQ(index.Count(), rows.size());
    for (const auto& query : queries) {
        SCOPED_TRACE("query size=" + std::to_string(query.size()));
        auto input = MembershipBuffer(query);
        auto original = MembershipBuffer(query);
        auto* values = query.empty() ? nullptr : input.get();
        const auto in = index.In(query.size(), values);
        const auto not_in = index.NotIn(query.size(), values);
        ASSERT_EQ(in.size(), rows.size());
        ASSERT_EQ(not_in.size(), rows.size());
        bool legacy_nan_query = false;
        if constexpr (std::is_floating_point_v<T>) {
            legacy_nan_query = std::any_of(
                query.begin(), query.end(), [](T v) { return std::isnan(v); });
        }
        for (size_t row = 0; row < rows.size(); ++row) {
            // Keep the legacy NaN-query contract distinct from the equality
            // scan oracle: lower/upper_bound(NaN) spans every non-NaN entry.
            const bool hit =
                legacy_nan_query ||
                std::find(query.begin(), query.end(), rows[row]) != query.end();
            const bool is_valid = !valid || valid[row];
            ASSERT_EQ(in[row], is_valid && hit) << "row=" << row;
            ASSERT_EQ(not_in[row], is_valid && !hit) << "row=" << row;
        }
        if (!query.empty()) {
            EXPECT_EQ(
                std::memcmp(
                    input.get(), original.get(), query.size() * sizeof(T)),
                0);
        }
    }
}

template <typename T>
class ScalarIndexSortTypedMembershipTest : public testing::Test {};
using MembershipTypes =
    testing::Types<int8_t, int16_t, int32_t, int64_t, bool, float, double>;
TYPED_TEST_SUITE(ScalarIndexSortTypedMembershipTest, MembershipTypes);

TYPED_TEST(ScalarIndexSortTypedMembershipTest, ScanOracleAndInputImmutability) {
    for (size_t n : {1, 2, 63, 64, 65, 127, 128, 129, 257}) {
        const auto rows = TypedMembershipRows<TypeParam>(n);
        const auto data = MembershipBuffer(rows);
        for (int null_mode : {0, 1, 2}) {
            SCOPED_TRACE("rows=" + std::to_string(n) +
                         " null_mode=" + std::to_string(null_mode));
            auto valid = std::make_unique<bool[]>(n);
            for (size_t i = 0; i < n; ++i) {
                valid[i] = null_mode == 0 || (null_mode == 1 && i % 3 != 0);
            }
            const bool* validity = null_mode == 0 ? nullptr : valid.get();
            ScalarIndexSort<TypeParam> index;
            index.Build(n, data.get(), validity);
            CheckTypedMembership(index, rows, validity);
        }
    }
}

TYPED_TEST(ScalarIndexSortTypedMembershipTest, LegacyAndPackedReloads) {
    ScalarSortMembershipLoadFixture fixture(
        "scalar_sort_typed_membership_reload", MembershipDataType<TypeParam>());
    const auto rows = TypedMembershipRows<TypeParam>(65);
    const auto data = MembershipBuffer(rows);
    auto valid = std::make_unique<bool[]>(rows.size());
    for (bool all_null : {false, true}) {
        SCOPED_TRACE("all_null=" + std::to_string(all_null));
        for (size_t i = 0; i < rows.size(); ++i) {
            valid[i] = !all_null && i % 3 != 1;
        }
        ScalarIndexSort<TypeParam> built(fixture.ctx);
        built.Build(rows.size(), data.get(), valid.get());
        const auto stats = built.UploadUnified({});
        for (bool mmap : {false, true}) {
            SCOPED_TRACE("mmap=" + std::to_string(mmap));
            Config config;
            config[ENABLE_MMAP] = mmap;
            config[milvus::LOAD_PRIORITY] = proto::common::LoadPriority::HIGH;
            config[INDEX_FILES] = stats->GetIndexFiles();
            ScalarIndexSort<TypeParam> loaded(fixture.ctx);
            loaded.LoadUnified(config);
            CheckTypedMembership(loaded, rows, valid.get());
            ScalarIndexSort<TypeParam> legacy(fixture.ctx);
            legacy.Load(built.Serialize({}), config);
            CheckTypedMembership(legacy, rows, valid.get());
        }
    }
}

}  // namespace

void
test_stlsort_for_range(
    const std::vector<int64_t>& data,
    DataType data_type,
    bool enable_mmap,
    std::function<TargetBitmap(
        const std::shared_ptr<ScalarIndexSort<int64_t>>&)> exec_expr,
    const std::vector<bool>& expected_result) {
    size_t nb = data.size();
    BinarySet binary_set;
    {
        Config config;

        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>();
        index->Build(nb, data.data());

        binary_set = index->Serialize(config);
    }
    {
        Config config;
        config[milvus::index::ENABLE_MMAP] = enable_mmap;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;

        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>();
        index->Load(binary_set, config);

        auto cnt = index->Count();
        ASSERT_EQ(cnt, nb);
        auto bitset = exec_expr(index);
        for (size_t i = 0; i < nb; i++) {
            ASSERT_EQ(bitset[i], expected_result[i]);
        }
    }

    std::vector<std::string> index_files;
    {
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

TEST(StlSortIndexTest, V3PersistsDirectLoadMetadata) {
    const std::vector<int64_t> data = {10, 2, 6, 5, 9, 3, 7, 8, 4, 1};
    auto index = std::make_shared<index::ScalarIndexSort<int64_t>>();
    index->Build(data.size(), data.data());
    RecordingIndexEntryWriter writer;

    index->WriteEntries(&writer);

    EXPECT_TRUE(writer.HasEntry("index_data"));
    EXPECT_TRUE(writer.HasEntry("idx_to_offsets"));
    EXPECT_TRUE(writer.HasEntry("valid_bitset"));
}

TEST(StlSortIndexTest, MmapFileOnlyPadsEmptyIndex) {
    constexpr size_t kAlignment = 32;
    constexpr const char* kMmapPath = "/tmp/milvus/mmap_test";
    Config config;
    config[milvus::index::ENABLE_MMAP] = true;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;

    const std::vector<int64_t> values = {10, 2, 6, 5};
    BinarySet non_empty_binary;
    {
        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>();
        index->Build(values.size(), values.data());
        non_empty_binary = index->Serialize({});
    }
    {
        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>();
        index->Load(non_empty_binary, config);
        const auto data_size = non_empty_binary.GetByName("index_data")->size;
        const auto aligned_size =
            ((data_size + kAlignment - 1) / kAlignment) * kAlignment;
        EXPECT_EQ(std::filesystem::file_size(kMmapPath), aligned_size);
    }

    const bool all_null[] = {false, false, false, false};
    BinarySet empty_binary;
    {
        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>();
        index->Build(values.size(), values.data(), all_null);
        empty_binary = index->Serialize({});
    }
    {
        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>();
        index->Load(empty_binary, config);
        EXPECT_EQ(std::filesystem::file_size(kMmapPath), 1);
    }
}

TEST(StlSortIndexTest, MmapLoadRemovesBackingFileOnStreamFailure) {
    constexpr const char* kMmapPath = "/tmp/milvus/mmap_test";
    std::filesystem::remove(kMmapPath);

    const std::vector<int64_t> values = {10, 2, 6, 5};
    auto packed_data = BuildPackedScalarSortIndex(values);

    auto input = std::make_shared<FailingMemoryInputStream>(
        packed_data, storage::MILVUS_V3_MAGIC_SIZE);
    auto reader = storage::IndexEntryReader::Open(input, packed_data->size());

    Config config;
    config[milvus::index::ENABLE_MMAP] = true;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    {
        ScalarIndexSort<int64_t> index;
        EXPECT_THROW(index.LoadEntries(*reader, config), SegcoreError);
    }

    EXPECT_FALSE(std::filesystem::exists(kMmapPath));
    std::filesystem::remove(kMmapPath);
}

TEST(StlSortIndexTest, MmapLoadRemovesMetaFileOnStreamFailure) {
    constexpr const char* kMmapPath = "/tmp/milvus/mmap_test";
    const auto mmap_meta_path = std::string(kMmapPath) + "-meta";
    std::filesystem::remove(kMmapPath);
    std::filesystem::remove(mmap_meta_path);

    const std::vector<int64_t> values = {10, 2, 6, 5};
    auto packed_data = BuildPackedScalarSortIndex(values);
    const auto offsets_entry_offset =
        storage::MILVUS_V3_MAGIC_SIZE +
        values.size() * sizeof(IndexStructure<int64_t>);
    auto input = std::make_shared<FailingMemoryInputStream>(
        packed_data, offsets_entry_offset);
    auto reader = storage::IndexEntryReader::Open(input, packed_data->size());

    Config config;
    config[milvus::index::ENABLE_MMAP] = true;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    {
        ScalarIndexSort<int64_t> index;
        EXPECT_THROW(index.LoadEntries(*reader, config), SegcoreError);
    }

    EXPECT_FALSE(std::filesystem::exists(kMmapPath));
    EXPECT_FALSE(std::filesystem::exists(mmap_meta_path));
    std::filesystem::remove(kMmapPath);
    std::filesystem::remove(mmap_meta_path);
}
