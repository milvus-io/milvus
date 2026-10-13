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

#ifdef BUILD_DISK_ANN

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Utils.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/vector/KnowhereEngine.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/LoadOptions.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index::test {
namespace {

constexpr int64_t kRows = 1000;
constexpr int64_t kDim = 4;

template <typename T>
void
WriteRaw(const std::string& path,
         const std::vector<T>& values,
         uint32_t rows,
         uint32_t dim) {
    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(stream.good());
    stream.write(reinterpret_cast<const char*>(&rows), sizeof(rows));
    stream.write(reinterpret_cast<const char*>(&dim), sizeof(dim));
    stream.write(reinterpret_cast<const char*>(values.data()),
                 static_cast<std::streamsize>(values.size() * sizeof(T)));
    ASSERT_TRUE(stream.good());
}

void
WriteOffsets(const std::string& path, const std::vector<size_t>& offsets) {
    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(stream.good());
    const size_t count = offsets.size();
    stream.write(reinterpret_cast<const char*>(&count), sizeof(count));
    stream.write(reinterpret_cast<const char*>(offsets.data()),
                 static_cast<std::streamsize>(count * sizeof(size_t)));
    ASSERT_TRUE(stream.good());
}

template <typename T>
DataType
PhysicalType() {
    if constexpr (std::is_same_v<T, float>) {
        return DataType::VECTOR_FLOAT;
    } else if constexpr (std::is_same_v<T, float16>) {
        return DataType::VECTOR_FLOAT16;
    } else {
        return DataType::VECTOR_BFLOAT16;
    }
}

template <typename T>
std::vector<T>
MakeRows() {
    std::vector<T> values(kRows * kDim);
    for (int64_t row = 0; row < kRows; ++row) {
        for (int64_t col = 0; col < kDim; ++col) {
            const float value = static_cast<float>(
                ((row * (col * 73 + 41) + col * 11) % 997) / 997.0);
            values[row * kDim + col] = static_cast<T>(value);
        }
    }
    return values;
}

template <typename T>
struct OpenedDiskIndex {
    IIndexReaderBasePtr base;
};

template <typename T>
void
BuildDisk(const std::string& raw_path,
          const std::string& staging_dir,
          OpenedDiskIndex<T>& opened,
          const std::string& metric = "L2",
          int64_t dim = kDim,
          int64_t logical_rows = kRows,
          const std::string& offsets_path = {},
          bool mmap = false) {
    auto adapted = AdaptIndexType({
        .index_type = knowhere::IndexEnum::INDEX_DISKANN,
        .field_type =
            offsets_path.empty() ? PhysicalType<T>() : DataType::VECTOR_ARRAY,
        .element_type =
            offsets_path.empty() ? DataType::NONE : PhysicalType<T>(),
        .index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber(),
        .params = {{METRIC_TYPE, metric},
                   {DIM_KEY, dim},
                   {DISK_ANN_MAX_DEGREE, "24"},
                   {DISK_ANN_SEARCH_LIST_SIZE, "56"},
                   {DISK_ANN_PQ_CODE_BUDGET, "0.001"},
                   {DISK_ANN_BUILD_DRAM_BUDGET, "2"},
                   {DISK_ANN_BUILD_THREAD_NUM, "2"}},
    });
    adapted.params["local_dir"] = staging_dir;
    auto builder =
        BuilderRegistry<PreparedVectorBuildFiles<T>>::Instance().Create(
            adapted.family, adapted.params);
    ASSERT_NE(builder, nullptr);
    PreparedVectorBuildFiles<T> input{.raw_path = raw_path};
    if (!offsets_path.empty()) {
        input.embedding_offsets_path = offsets_path;
    }
    auto artifact = std::move(*builder).Build(input);
    ASSERT_NE(artifact, nullptr);

    // DiskANN needs the V1/V2 directory source's native file-manager handle.
    // A buffer-backed test source can read the entries but cannot open that
    // handle, so persist through the same sink/source pair as production.
    storage::StorageConfig config;
    config.storage_type = "local";
    config.root_path = staging_dir + "/objects/";
    std::filesystem::create_directories(config.root_path);
    const auto field_type =
        offsets_path.empty() ? PhysicalType<T>() : DataType::VECTOR_ARRAY;
    storage::FileManagerContext context({1, 2, 3, 100},
                                        {3,
                                         100,
                                         1000,
                                         1,
                                         "vector_disk_family",
                                         "field",
                                         field_type,
                                         dim,
                                         false},
                                        storage::CreateChunkManager(config),
                                        storage::InitArrowFileSystem(config));
    context.use_async_load = false;
    storage::V1DiskSink sink(context);
    artifact->Serialize(sink);
    const auto stats = sink.Finish();
    ASSERT_FALSE(stats.Files().empty());
    std::vector<std::string> paths;
    paths.reserve(stats.Files().size());
    for (const auto& file : stats.Files()) {
        paths.push_back(file.file_name);
    }
    sink.ReleaseLocalStaging();
    storage::LoadOptions options;
    options.params = adapted.params;
    options.params[INDEX_NUM_ROWS_KEY] = logical_rows;
    options.params[DISK_ANN_LOAD_THREAD_NUM] = 2;
    options.enable_mmap = mmap;
    options.mmap_dir_path = staging_dir;
    opened.base =
        LoaderRegistry::Instance()
            .Lookup(adapted.family)
            .Load({IndexFiles{context,
                              std::move(paths),
                              LegacyIndexStorageConfig{
                                  storage::V1SourceLayout::DiskFiles}},
                   options});
}

template <typename T>
void
RunOrdinaryDiskSearch(bool mmap) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_family_XXXXXX", "vector disk family test");
    const auto raw_path = files->Path() + "/raw";
    auto values = MakeRows<T>();
    WriteRaw(raw_path, values, kRows, kDim);
    OpenedDiskIndex<T> opened;
    BuildDisk(raw_path, files->Path(), opened, "L2", kDim, kRows, {}, mmap);
    ASSERT_NE(opened.base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(opened.base->Count(), kRows);
    EXPECT_EQ(reader->Dim(), kDim);
    EXPECT_EQ(reader->KnowhereIndexType(), knowhere::IndexEnum::INDEX_DISKANN);
    VectorSearchParams params{
        .search_params_ = {{DISK_ANN_QUERY_LIST, 8}},
        .metric_type_ = "L2",
        .topk_ = 4,
    };
    auto query = GenDataset(2, kDim, values.data() + 100 * kDim);
    SearchResult result;
    reader->Search(query, params, BitsetView{}, nullptr, result);
    EXPECT_EQ(result.total_nq_, 2);
    ASSERT_EQ(result.seg_offsets_.size(), 8);
    EXPECT_EQ(result.unity_topK_, 4);
    for (int64_t nq = 0; nq < 2; ++nq) {
        EXPECT_GE(result.seg_offsets_[nq * 4], 0);
        EXPECT_LT(result.seg_offsets_[nq * 4], kRows);
    }
    params.search_params_[knowhere::meta::RADIUS] = 0.2;
    params.search_params_[knowhere::meta::RANGE_FILTER] = 0.1;
    SearchResult range_result;
    reader->Search(query, params, BitsetView{}, nullptr, range_result);
    EXPECT_EQ(range_result.total_nq_, 2);
    EXPECT_EQ(range_result.seg_offsets_.size(), 8);
    if constexpr (std::is_same_v<T, float>) {
        const std::array<int64_t, 2> ids{511, 100};
        if (reader->HasRawData()) {
            auto got = reader->GetVector(GenIdsDataset(ids.size(), ids.data()));
            ASSERT_EQ(got.size(), ids.size() * kDim * sizeof(T));
            for (size_t i = 0; i < ids.size(); ++i) {
                EXPECT_EQ(std::memcmp(got.data() + i * kDim * sizeof(T),
                                      values.data() + ids[i] * kDim,
                                      kDim * sizeof(T)),
                          0);
            }
        } else {
            EXPECT_ANY_THROW(static_cast<void>(
                reader->GetVector(GenIdsDataset(ids.size(), ids.data()))));
        }
    }
}

TEST(VectorDiskFamilyTest, FloatBuildLoadSearchAndRawCapability) {
    RunOrdinaryDiskSearch<float>(false);
}

TEST(VectorDiskFamilyTest, Float16BuildLoadSearch) {
    RunOrdinaryDiskSearch<float16>(false);
}

TEST(VectorDiskFamilyTest, BFloat16BuildLoadSearch) {
    RunOrdinaryDiskSearch<bfloat16>(false);
}

TEST(VectorDiskFamilyTest, MmapCapabilityAndSearch) {
#ifdef __APPLE__
    GTEST_SKIP() << "faiss mapped I/O is unavailable on macOS";
#endif
    const bool native_mmap =
        KnowhereMmapSupported(knowhere::IndexEnum::INDEX_DISKANN);
    RunOrdinaryDiskSearch<float>(native_mmap);
}

TEST(VectorDiskFamilyTest, RejectsQueryListSmallerThanTopK) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_invalid_XXXXXX", "vector disk test");
    const auto raw_path = files->Path() + "/raw";
    auto values = MakeRows<float>();
    WriteRaw(raw_path, values, kRows, kDim);
    OpenedDiskIndex<float> opened;
    BuildDisk(raw_path, files->Path(), opened);
    ASSERT_NE(opened.base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(reader, nullptr);
    VectorSearchParams params{
        .search_params_ = {{DISK_ANN_QUERY_LIST, 2}},
        .metric_type_ = "L2",
        .topk_ = 4,
    };
    SearchResult result;
    EXPECT_ANY_THROW(reader->Search(GenDataset(1, kDim, values.data()),
                                    params,
                                    BitsetView{},
                                    nullptr,
                                    result));
}

TEST(VectorDiskFamilyTest, EmbeddingListsPreserveTrailingEmptyParents) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_array_XXXXXX", "vector disk array test");
    constexpr int64_t parents = 100;
    std::vector<size_t> offsets{0};
    for (int64_t row = 0; row < parents; ++row) {
        offsets.push_back(offsets.back() +
                          (row >= parents - 2 ? 0 : (row % 5) + 1));
    }
    std::vector<float> values(offsets.back() * kDim);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<float>((i * 47) % 997) / 997.0F;
    }
    const auto raw_path = files->Path() + "/raw";
    const auto offsets_path = files->Path() + "/offsets";
    WriteRaw(raw_path, values, offsets.back(), kDim);
    WriteOffsets(offsets_path, offsets);
    OpenedDiskIndex<float> opened;
    BuildDisk(raw_path,
              files->Path(),
              opened,
              knowhere::metric::MAX_SIM_L2,
              kDim,
              parents,
              offsets_path);
    ASSERT_NE(opened.base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(opened.base->Count(), offsets.back());
    const std::array<int64_t, 2> empty_ids{parents - 2, parents - 1};
    const auto [bytes, got_offsets] = reader->GetEmbListByIds(
        GenIdsDataset(empty_ids.size(), empty_ids.data()), reader->Metric());
    EXPECT_TRUE(bytes.empty());
    EXPECT_EQ(got_offsets, (std::vector<size_t>{0, 0, 0}));

    std::array<size_t, 3> query_offsets{0, 3, 5};
    auto query = GenDataset(5, kDim, values.data());
    query->Set(knowhere::meta::EMB_LIST_OFFSET,
               static_cast<const size_t*>(query_offsets.data()));
    query->Set(knowhere::meta::EMB_LIST_COUNT, int64_t{2});
    query->Set(knowhere::meta::NQ, int64_t{2});
    VectorSearchParams params{
        .search_params_ = {{DISK_ANN_QUERY_LIST, 8}},
        .metric_type_ = knowhere::metric::MAX_SIM_L2,
        .topk_ = 4,
    };
    SearchResult result;
    reader->Search(query, params, BitsetView{}, nullptr, result);
    EXPECT_EQ(result.total_nq_, 2);
    EXPECT_EQ(result.unity_topK_, 4);
    EXPECT_EQ(result.seg_offsets_.size(), 8);
    EXPECT_EQ(result.distances_.size(), 8);
}

}  // namespace
}  // namespace milvus::index::test

#endif  // BUILD_DISK_ANN
