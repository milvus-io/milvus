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

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <array>
#include <set>

#include "common/FieldData.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "index/Meta.h"
#include "index/VectorMemIndex.h"
#include "index/VectorDiskIndex.h"
#include "knowhere/comp/index_param.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/packed/writer.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::indexbuilder::test {
namespace {
constexpr int64_t kDim = 128;

Config
DiskParams() {
    return {{index::METRIC_TYPE, "L2"},
            {index::DISK_ANN_MAX_DEGREE, "24"},
            {index::DISK_ANN_SEARCH_LIST_SIZE, "56"},
            {index::DISK_ANN_PQ_CODE_BUDGET, "0.001"},
            {index::DISK_ANN_BUILD_DRAM_BUDGET, "2"},
            {index::DISK_ANN_BUILD_THREAD_NUM, "1"},
            {index::DISK_ANN_LOAD_THREAD_NUM, "1"},
            {index::DISK_ANN_SEARCH_CACHE_BUDGET, "0.0"}};
}

void
ExpectEmptySearch(const index::VectorIndex& reader,
                  const DatasetPtr& queries,
                  const MetricType& metric,
                  int64_t nq);

class VectorArrayBuildSessionTest : public testing::Test {
 protected:
    struct PreparedBuild {
        storage::FileManagerContext file_manager_context;
        Config params;
        bool disk;
        int64_t dim;
    };
    struct ElementNullableData {
        FieldDataPtr data;
        std::vector<bool> element_valid;
        std::vector<std::vector<float>> element_vectors;
        int64_t physical_elements{0};
    };
    std::string root_;
    storage::ChunkManagerPtr cm_;
    milvus_storage::ArrowFileSystemPtr fs_;

    void
    SetUp() override {
        root_ =
            "/tmp/cp30-vector-array-" + std::to_string(getpid()) + "-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
        std::filesystem::create_directories(root_ + "/100");
        auto config = get_default_local_storage_config();
        config.root_path = root_;
        cm_ = storage::CreateChunkManager(config);
        fs_ = storage::InitArrowFileSystem(config);
    }
    void
    TearDown() override {
        std::filesystem::remove_all(root_);
    }
    PreparedBuild
    Prepare(DataType type,
            DataType element,
            bool nullable,
            const IndexType& family,
            int64_t rows,
            int64_t dim,
            Config params) {
        auto field =
            segcore::gen_field_meta(1, 2, 3001, 100, type, element, nullable);
        storage::IndexMeta index = {
            3001, 100, 1001, 1, "test", "100", type, dim};
        params[STORAGE_VERSION_KEY] = 2;
        params[DATA_TYPE_KEY] = type;
        params[ELEMENT_TYPE_KEY] = element;
        params[DIM_KEY] = dim;
        params[SEGMENT_INSERT_FILES_KEY] =
            std::vector<std::vector<std::string>>{{root_ + "/100/0"}};
        return {storage::FileManagerContext(field, index, cm_, fs_),
                params,
                family == knowhere::IndexEnum::INDEX_DISKANN,
                dim};
    }
    PreparedBuild
    PrepareElementNullable(int64_t rows, bool disk = false) {
        auto params = disk
                          ? DiskParams()
                          : Config{{index::METRIC_TYPE, "COSINE"},
                                   {knowhere::indexparam::HNSW_M, 16},
                                   {knowhere::indexparam::EFCONSTRUCTION, 200}};
        auto result = Prepare(DataType::VECTOR_ARRAY,
                              DataType::VECTOR_FLOAT,
                              true,
                              disk ? knowhere::IndexEnum::INDEX_DISKANN
                                   : knowhere::IndexEnum::INDEX_HNSW,
                              rows,
                              kDim,
                              params);
        result.file_manager_context.fieldDataMeta.field_schema
            .set_element_nullable(true);
        return result;
    }
    // V1 binlogs cannot carry element validity. Use the existing StorageV2
    // production reader, preserving null parent rows and null child slots.
    void
    WriteInsert(const PreparedBuild& prepared, const FieldDataPtr& data) {
        const bool element_nullable =
            prepared.file_manager_context.fieldDataMeta.field_schema
                .element_nullable();
        // Match the production Arrow schema: native nullable elements use
        // Binary; non-nullable vectors use FixedSizeBinary at their full width.
        std::shared_ptr<arrow::ArrayBuilder> child;
        if (element_nullable) {
            child = std::make_shared<arrow::BinaryBuilder>();
        } else {
            child = std::make_shared<arrow::FixedSizeBinaryBuilder>(
                arrow::fixed_size_binary(prepared.dim * sizeof(float)));
        }
        auto list_type =
            arrow::list(arrow::field("item", child->type(), element_nullable));
        arrow::ListBuilder builder(
            arrow::default_memory_pool(), child, list_type);
        auto arrays = std::dynamic_pointer_cast<FieldData<VectorArray>>(data);
        ASSERT_NE(arrays, nullptr);
        int64_t physical_parent = 0;
        for (int64_t row = 0; row < data->get_num_rows(); ++row) {
            if (data->IsNullable() && !data->is_valid(row)) {
                ASSERT_TRUE(builder.AppendNull().ok());
                continue;
            }
            ASSERT_TRUE(builder.Append().ok());
            const auto& value = *arrays->value_at(physical_parent++);
            int64_t physical_element = 0;
            for (int64_t i = 0; i < value.length(); ++i) {
                if (value.is_element_nullable() &&
                    !value.element_validity_view()[i]) {
                    ASSERT_TRUE(element_nullable);
                    ASSERT_TRUE(child->AppendNull().ok());
                    continue;
                }
                const auto* bytes =
                    reinterpret_cast<const uint8_t*>(value.data()) +
                    physical_element++ * prepared.dim * sizeof(float);
                if (element_nullable) {
                    ASSERT_TRUE(
                        std::static_pointer_cast<arrow::BinaryBuilder>(child)
                            ->Append(bytes, prepared.dim * sizeof(float))
                            .ok());
                } else {
                    ASSERT_TRUE(
                        std::static_pointer_cast<arrow::FixedSizeBinaryBuilder>(
                            child)
                            ->Append(bytes)
                            .ok());
                }
            }
        }
        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto field = arrow::field("100", array->type(), data->IsNullable())
                         ->WithMetadata(arrow::key_value_metadata(
                             {milvus_storage::ARROW_FIELD_ID_KEY}, {"100"}));
        auto schema = arrow::schema({field});
        std::vector<std::string> paths{root_ + "/100/0"};
        std::vector<std::vector<int>> column_groups{{0}};
        milvus_storage::StorageConfig storage_config;
        auto output = milvus_storage::PackedRecordBatchWriter::Make(
            fs_, paths, schema, storage_config, column_groups);
        ASSERT_TRUE(output.ok()) << output.status().ToString();
        auto writer = output.ValueOrDie();
        ASSERT_TRUE(writer
                        ->Write(arrow::RecordBatch::Make(
                            schema, data->get_num_rows(), {array}))
                        .ok());
        ASSERT_TRUE(writer->Close().ok());
    }
    std::unique_ptr<index::VectorIndex>
    NewIndex(const PreparedBuild& prepared) {
        const auto metric =
            prepared.params.at(index::METRIC_TYPE).get<std::string>();
        auto version = knowhere::Version::GetCurrentVersion().VersionNumber();
#ifdef BUILD_DISK_ANN
        if (prepared.disk) {
            return std::make_unique<index::VectorDiskAnnIndex<float>>(
                DataType::VECTOR_FLOAT,
                knowhere::IndexEnum::INDEX_DISKANN,
                metric,
                version,
                prepared.file_manager_context);
        }
#endif
        return std::make_unique<index::VectorMemIndex<float>>(
            DataType::VECTOR_FLOAT,
            knowhere::IndexEnum::INDEX_HNSW,
            metric,
            version,
            true,
            prepared.file_manager_context);
    }
    index::IndexStatsPtr
    Publish(const PreparedBuild& prepared) {
        auto owner = NewIndex(prepared);
        owner->Build(prepared.params);
        return owner->Upload(prepared.params);
    }
    index::IndexStatsPtr
    PublishElementNullable(const PreparedBuild& prepared,
                           const FieldDataPtr& data,
                           int64_t rows) {
        WriteInsert(prepared, data);
        return Publish(prepared);
    }
    index::IndexStatsPtr
    PublishElementNullableDisk(const PreparedBuild& prepared,
                               const FieldDataPtr& data,
                               int64_t rows) {
        return PublishElementNullable(prepared, data, rows);
    }
    std::unique_ptr<index::VectorIndex>
    Open(const PreparedBuild& prepared,
         const index::IndexStatsPtr& stats,
         bool mmap = false) {
        auto result = NewIndex(prepared);
        auto config = prepared.params;
        // The 3.0 metadata-only loader reads dim from string index_params.
        config[DIM_KEY] = std::to_string(prepared.dim);
        const auto& schema =
            prepared.file_manager_context.fieldDataMeta.field_schema;
        if (schema.data_type() == proto::schema::DataType::ArrayOfVector) {
            config[index::ELEMENT_NULLABLE] = schema.element_nullable();
        }
        config["index_files"] = stats->GetIndexFiles();
        config[index::ENABLE_MMAP_I2O_MAP] = mmap;
        config[index::ENABLE_MMAP_O2I_MAP] = mmap;
        if (mmap && !prepared.disk)
            config[index::MMAP_FILE_PATH] = root_ + "/index.mmap";
        result->Load(milvus::tracer::TraceContext{}, config);
        return result;
    }
    static ElementNullableData
    GenElementNullable(int64_t rows, bool all_null_elements) {
        std::mt19937 rng(7);
        std::uniform_real_distribution<float> distribution(-1.0F, 1.0F);
        ElementNullableData result;
        std::vector<VectorArray> arrays;
        std::vector<uint8_t> validity((rows + 7) / 8, 0);
        for (int64_t row = 0; row < rows; ++row) {
            if (row % 7 == 3) {
                continue;
            }
            validity[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
            const auto count = static_cast<int>(row % 4);
            TargetBitmap bits(count, false);
            std::vector<float> values;
            for (int j = 0; j < count; ++j) {
                const bool valid = !all_null_elements && (row + j) % 3 != 0;
                result.element_valid.push_back(valid);
                result.element_vectors.emplace_back();
                if (!valid) {
                    continue;
                }
                bits.set(j);
                auto& vector = result.element_vectors.back();
                for (int64_t d = 0; d < kDim; ++d) {
                    vector.push_back(distribution(rng));
                }
                values.insert(values.end(), vector.begin(), vector.end());
                ++result.physical_elements;
            }
            arrays.emplace_back(values.data(),
                                count,
                                kDim,
                                DataType::VECTOR_FLOAT,
                                TargetBitmapView(bits.data(), count),
                                true);
        }
        result.data = storage::CreateFieldData(
            DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, true, kDim);
        result.data->FillFieldData(arrays.data(), validity.data(), rows, 0);
        return result;
    }
    void
    CheckZeroLogicalElements(bool disk, bool null_parents) {
        constexpr int64_t rows = 4;
        for (const bool element_nullable : {false, true}) {
            SCOPED_TRACE(element_nullable);
            SCOPED_TRACE(null_parents);
            auto prepared = PrepareElementNullable(rows, disk);
            prepared.file_manager_context.fieldDataMeta.field_schema
                .set_element_nullable(element_nullable);
            std::vector<VectorArray> arrays;
            for (int64_t row = 0; row < rows; ++row) {
                arrays.emplace_back(nullptr,
                                    0,
                                    kDim,
                                    DataType::VECTOR_FLOAT,
                                    TargetBitmapView(),
                                    element_nullable);
            }
            auto data = storage::CreateFieldData(
                DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, true, kDim);
            const std::vector<uint8_t> validity{
                static_cast<uint8_t>(null_parents ? 0 : 0x0F)};
            data->FillFieldData(arrays.data(), validity.data(), rows, 0);
            WriteInsert(prepared, data);
            auto builder = NewIndex(prepared);
            builder->Build(prepared.params);
            EXPECT_EQ(builder->Count(), 0);
            const auto serialized = builder->Serialize(prepared.params);
            EXPECT_FALSE(serialized.binary_map_.empty());
            const auto stats = builder->Upload(prepared.params);
            ASSERT_FALSE(stats->GetIndexFiles().empty());
            builder.reset();
            for (const bool mmap : {false, true}) {
                SCOPED_TRACE(mmap);
                auto owner = Open(prepared, stats, mmap);
                EXPECT_EQ(owner->GetDim(), kDim);
                EXPECT_EQ(owner->Count(), 0);
                EXPECT_EQ(owner->GetLogicalCount(), 0);
                EXPECT_FALSE(owner->HasValidData());
                const std::vector<float> values(2 * kDim, 0.1F);
                ExpectEmptySearch(
                    *owner,
                    knowhere::GenDataSet(2, kDim, values.data()),
                    prepared.params.at(index::METRIC_TYPE).get<std::string>(),
                    2);
            }
        }
    }
};

void
ExpectEmptySearch(const index::VectorIndex& reader,
                  const DatasetPtr& queries,
                  const MetricType& metric,
                  int64_t nq) {
    SearchInfo search;
    search.topk_ = 4;
    search.metric_type_ = metric;
    search.search_params_ =
        Config{{index::METRIC_TYPE, metric}, {knowhere::indexparam::EF, 64}};
    SearchResult result;
    reader.Query(queries, search, nullptr, nullptr, result);
    EXPECT_EQ(result.total_nq_, nq);
    EXPECT_TRUE(std::all_of(result.seg_offsets_.begin(),
                            result.seg_offsets_.end(),
                            [](auto id) { return id == INVALID_SEG_OFFSET; }));
}

TEST_F(VectorArrayBuildSessionTest, HnswElementNullableElementLevel) {
    constexpr int64_t rows = 60;
    constexpr int64_t topk = 4;
    auto prepared = PrepareElementNullable(rows);
    const auto generated = GenElementNullable(rows, false);
    const auto logical = static_cast<int64_t>(generated.element_valid.size());
    ASSERT_GT(generated.physical_elements, topk);
    ASSERT_LT(generated.physical_elements, logical);
    const auto stats = PublishElementNullable(prepared, generated.data, rows);
    ASSERT_FALSE(stats->GetIndexFiles().empty());

    std::vector<int64_t> targets;
    for (int64_t id = 0; id < logical && targets.size() < 2; ++id) {
        if (generated.element_valid[id]) {
            targets.push_back(id);
        }
    }
    ASSERT_EQ(targets.size(), 2);
    std::vector<float> query_values;
    for (const auto id : targets) {
        const auto& vector = generated.element_vectors[id];
        query_values.insert(query_values.end(), vector.begin(), vector.end());
    }
    const auto queries = knowhere::GenDataSet(2, kDim, query_values.data());
    SearchInfo search;
    search.topk_ = topk;
    search.metric_type_ = knowhere::metric::COSINE;
    search.search_params_ =
        Config{{knowhere::meta::METRIC_TYPE, search.metric_type_},
               {knowhere::indexparam::EF, 64}};
    const auto expect_valid_elements = [&](const SearchResult& result) {
        for (const auto offset : result.seg_offsets_) {
            if (offset == INVALID_SEG_OFFSET) {
                continue;
            }
            ASSERT_GE(offset, 0);
            ASSERT_LT(offset, logical);
            EXPECT_TRUE(generated.element_valid[offset]) << offset;
        }
    };

    for (const bool mmap : {false, true}) {
        SCOPED_TRACE(mmap ? "mmap" : "heap");
        auto owner = Open(prepared, stats, mmap);
        EXPECT_EQ(owner->Count(), generated.physical_elements);
        const auto* reader =
            dynamic_cast<const index::VectorIndex*>(owner.get());
        ASSERT_NE(reader, nullptr);
        EXPECT_TRUE(reader->HasValidData());
        EXPECT_EQ(reader->GetValidCount(), generated.physical_elements);
        for (int64_t id = 0; id < logical; ++id) {
            EXPECT_EQ(reader->IsRowValid(id), generated.element_valid[id])
                << id;
        }

        SearchResult result;
        reader->Query(queries, search, nullptr, nullptr, result);
        EXPECT_EQ(result.total_nq_, 2);
        ASSERT_EQ(result.seg_offsets_.size(), 2 * topk);
        expect_valid_elements(result);
        // An exact copy of an element finds that element's logical id.
        EXPECT_EQ(result.seg_offsets_[0], targets[0]);
        EXPECT_EQ(result.seg_offsets_[topk], targets[1]);

        // The filter bitset is addressed by logical element id.
        TargetBitmap filter(logical, false);
        filter.set(targets[0]);
        SearchResult filtered;
        reader->Query(queries, search, BitsetView(filter), nullptr, filtered);
        ASSERT_EQ(filtered.seg_offsets_.size(), 2 * topk);
        expect_valid_elements(filtered);
        EXPECT_TRUE(
            std::none_of(filtered.seg_offsets_.begin(),
                         filtered.seg_offsets_.end(),
                         [&](int64_t offset) { return offset == targets[0]; }));
        EXPECT_EQ(filtered.seg_offsets_[topk], targets[1]);
    }
}

TEST_F(VectorArrayBuildSessionTest, HnswElementNullableAllNullElements) {
    constexpr int64_t rows = 20;
    auto prepared = PrepareElementNullable(rows);
    const auto generated = GenElementNullable(rows, true);
    const auto logical = static_cast<int64_t>(generated.element_valid.size());
    ASSERT_GT(logical, 0);
    ASSERT_EQ(generated.physical_elements, 0);
    const auto stats = PublishElementNullable(prepared, generated.data, rows);
    ASSERT_FALSE(stats->GetIndexFiles().empty());

    auto owner = Open(prepared, stats);
    EXPECT_EQ(owner->Count(), 0);
    const auto* reader = dynamic_cast<const index::VectorIndex*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->HasValidData());
    EXPECT_EQ(reader->GetLogicalCount(), logical);
    EXPECT_EQ(reader->GetValidCount(), 0);
    for (int64_t id = 0; id < logical; ++id) {
        EXPECT_FALSE(reader->IsRowValid(id)) << id;
    }
    const std::vector<float> values(2 * kDim, 0.1F);
    ExpectEmptySearch(*reader,
                      knowhere::GenDataSet(2, kDim, values.data()),
                      knowhere::metric::COSINE,
                      2);
}

TEST_F(VectorArrayBuildSessionTest, HnswElementNullableWithoutElements) {
    CheckZeroLogicalElements(false, false);
}

TEST_F(VectorArrayBuildSessionTest, HnswElementNullableAllNullParents) {
    CheckZeroLogicalElements(false, true);
}

#ifdef BUILD_DISK_ANN

TEST_F(VectorArrayBuildSessionTest, DiskAnnElementNullableWithoutElements) {
    CheckZeroLogicalElements(true, false);
}

TEST_F(VectorArrayBuildSessionTest, DiskAnnElementNullableAllNullParents) {
    CheckZeroLogicalElements(true, true);
}

TEST_F(VectorArrayBuildSessionTest, DiskAnnElementNullableAllNullElements) {
    constexpr int64_t rows = 20;
    auto prepared = PrepareElementNullable(rows, true);
    const auto generated = GenElementNullable(rows, true);
    const auto logical = static_cast<int64_t>(generated.element_valid.size());
    ASSERT_GT(logical, 0);
    ASSERT_EQ(generated.physical_elements, 0);
    const auto stats =
        PublishElementNullableDisk(prepared, generated.data, rows);
    ASSERT_FALSE(stats->GetIndexFiles().empty());
    for (const bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        auto owner = Open(prepared, stats, mmap);
        EXPECT_EQ(owner->Count(), 0);
        EXPECT_EQ(owner->GetLogicalCount(), logical);
        EXPECT_TRUE(owner->HasValidData());
        EXPECT_EQ(owner->GetValidCount(), 0);
        for (int64_t id = 0; id < logical; ++id) {
            EXPECT_FALSE(owner->IsRowValid(id)) << id;
        }
        const std::vector<float> values(2 * kDim, 0.1F);
        ExpectEmptySearch(*owner,
                          knowhere::GenDataSet(2, kDim, values.data()),
                          knowhere::metric::L2,
                          2);
    }
}

TEST_F(VectorArrayBuildSessionTest,
       DiskAnnNullableElementLevelListsFromBinlog) {
    constexpr int64_t rows = 60;
    constexpr int64_t topk = 4;
    auto params = DiskParams();
    params[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
    auto prepared = Prepare(DataType::VECTOR_ARRAY,
                            DataType::VECTOR_FLOAT,
                            true,
                            knowhere::IndexEnum::INDEX_DISKANN,
                            rows,
                            kDim,
                            params);
    std::mt19937 rng(42);
    std::uniform_real_distribution<float> distribution(-1.0F, 1.0F);
    std::vector<VectorArray> arrays;
    std::vector<std::vector<float>> elements;
    std::vector<uint8_t> validity((rows + 7) / 8, 0);
    for (int64_t row = 0; row < rows; ++row) {
        if (row % 5 == 0) {
            continue;
        }
        validity[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
        const auto count = row % 3 + 1;
        std::vector<float> values(count * kDim);
        for (auto& value : values) {
            value = distribution(rng);
        }
        for (int64_t j = 0; j < count; ++j) {
            elements.emplace_back(values.begin() + j * kDim,
                                  values.begin() + (j + 1) * kDim);
        }
        arrays.emplace_back(values.data(), count, kDim, DataType::VECTOR_FLOAT);
    }
    const auto physical = static_cast<int64_t>(elements.size());
    auto data = storage::CreateFieldData(
        DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, true, kDim);
    data->FillFieldData(arrays.data(), validity.data(), rows, 0);
    WriteInsert(prepared, data);
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats->GetIndexFiles().empty());

    auto owner = Open(prepared, stats);
    ASSERT_EQ(owner->Count(), physical);
    const auto* reader = dynamic_cast<const index::VectorIndex*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_FALSE(reader->HasValidData());

    // An exact copy of an element finds that element's id.
    const std::vector<int64_t> targets{0, physical - 1};
    std::vector<float> query_values;
    for (const auto id : targets) {
        query_values.insert(
            query_values.end(), elements[id].begin(), elements[id].end());
    }
    const auto queries = knowhere::GenDataSet(2, kDim, query_values.data());
    SearchInfo search;
    search.topk_ = topk;
    search.metric_type_ = knowhere::metric::L2;
    search.search_params_ =
        Config{{knowhere::meta::METRIC_TYPE, search.metric_type_},
               {index::DISK_ANN_QUERY_LIST, topk * 4}};
    SearchResult result;
    reader->Query(queries, search, nullptr, nullptr, result);
    EXPECT_EQ(result.total_nq_, 2);
    ASSERT_EQ(result.seg_offsets_.size(), 2 * topk);
    EXPECT_TRUE(std::all_of(
        result.seg_offsets_.begin(),
        result.seg_offsets_.end(),
        [&](int64_t offset) { return offset >= 0 && offset < physical; }));
    EXPECT_EQ(result.seg_offsets_[0], targets[0]);
    EXPECT_EQ(result.seg_offsets_[topk], targets[1]);
}

TEST_F(VectorArrayBuildSessionTest, DiskAnnElementNullableElementLevel) {
    constexpr int64_t rows = 60;
    constexpr int64_t topk = 4;
    auto prepared = PrepareElementNullable(rows, true);
    const auto generated = GenElementNullable(rows, false);
    const auto logical = static_cast<int64_t>(generated.element_valid.size());
    ASSERT_LT(generated.physical_elements, logical);
    const auto stats =
        PublishElementNullableDisk(prepared, generated.data, rows);
    ASSERT_FALSE(stats->GetIndexFiles().empty());

    std::vector<int64_t> targets;
    for (int64_t id = logical - 1; id >= 0 && targets.size() < 2; --id) {
        if (generated.element_valid[id]) {
            targets.push_back(id);
        }
    }
    ASSERT_EQ(targets.size(), 2);
    std::vector<float> query_values;
    for (const auto id : targets) {
        const auto& vector = generated.element_vectors[id];
        query_values.insert(query_values.end(), vector.begin(), vector.end());
    }
    const auto queries = knowhere::GenDataSet(2, kDim, query_values.data());
    SearchInfo search;
    search.topk_ = topk;
    search.metric_type_ = knowhere::metric::L2;
    search.search_params_ =
        Config{{knowhere::meta::METRIC_TYPE, search.metric_type_},
               {index::DISK_ANN_QUERY_LIST, topk * 4}};

    auto owner = Open(prepared, stats);
    EXPECT_EQ(owner->Count(), generated.physical_elements);
    const auto* reader = dynamic_cast<const index::VectorIndex*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->HasValidData());
    EXPECT_EQ(reader->GetValidCount(), generated.physical_elements);
    for (int64_t id = 0; id < logical; ++id) {
        EXPECT_EQ(reader->IsRowValid(id), generated.element_valid[id]) << id;
    }
    const auto expect_valid_elements = [&](const SearchResult& result) {
        for (const auto offset : result.seg_offsets_) {
            if (offset == INVALID_SEG_OFFSET) {
                continue;
            }
            ASSERT_GE(offset, 0);
            ASSERT_LT(offset, logical);
            EXPECT_TRUE(generated.element_valid[offset]) << offset;
        }
    };
    SearchResult result;
    reader->Query(queries, search, nullptr, nullptr, result);
    ASSERT_EQ(result.seg_offsets_.size(), 2 * topk);
    expect_valid_elements(result);
    EXPECT_EQ(result.seg_offsets_[0], targets[0]);
    EXPECT_EQ(result.seg_offsets_[topk], targets[1]);

    TargetBitmap filter(logical, false);
    filter.set(targets[0]);
    SearchResult filtered;
    reader->Query(queries, search, BitsetView(filter), nullptr, filtered);
    ASSERT_EQ(filtered.seg_offsets_.size(), 2 * topk);
    expect_valid_elements(filtered);
    EXPECT_TRUE(
        std::none_of(filtered.seg_offsets_.begin(),
                     filtered.seg_offsets_.end(),
                     [&](int64_t offset) { return offset == targets[0]; }));
    EXPECT_EQ(filtered.seg_offsets_[topk], targets[1]);
}

#endif

TEST_F(VectorArrayBuildSessionTest,
       ResidentElementNullableRejectsEmbeddingList) {
    auto prepared = PrepareElementNullable(4);
    prepared.params[index::METRIC_TYPE] = "MAX_SIM_L2";
    const auto generated = GenElementNullable(4, false);
    WriteInsert(prepared, generated.data);
    EXPECT_THROW(Publish(prepared), SegcoreError);
}

TEST_F(VectorArrayBuildSessionTest, DiskElementLevelWritesElementValidity) {
    auto prepared = PrepareElementNullable(2);
    prepared.dim = 2;
    prepared.params[DIM_KEY] = 2;
    prepared.params[index::METRIC_TYPE] = "L2";
    prepared.params[index::EMB_LIST] = false;
    uint64_t selected = 5, none = 0;
    float values[] = {1, 2, 3, 4};
    VectorArray rows[] = {VectorArray(values,
                                      3,
                                      2,
                                      DataType::VECTOR_FLOAT,
                                      TargetBitmapView(&selected, 3),
                                      true),
                          VectorArray(nullptr,
                                      2,
                                      2,
                                      DataType::VECTOR_FLOAT,
                                      TargetBitmapView(&none, 2),
                                      true)};
    auto data =
        std::make_shared<FieldData<VectorArray>>(2, DataType::VECTOR_FLOAT);
    data->FillFieldData(rows, 2);
    WriteInsert(prepared, data);
    storage::DiskFileManagerImpl manager(prepared.file_manager_context);
    auto lease = manager.AcquireLocalDirWriteLease(
        manager.GetLocalRawDataObjectPrefix());
    auto validity_path = root_ + "/validity";
    auto offsets_path = root_ + "/offsets";
    prepared.params[index::VALID_DATA_PATH_KEY] = validity_path;
    prepared.params[index::EMB_LIST_OFFSETS_PATH] = offsets_path;
    auto raw_path = manager.CacheRawDataToDisk<float>(prepared.params);
    std::ifstream raw(raw_path, std::ios::binary);
    uint32_t header[2]{};
    raw.read(reinterpret_cast<char*>(header), sizeof(header));
    EXPECT_EQ(header[0], 2);
    EXPECT_EQ(header[1], 2);
    float payload[4]{};
    raw.read(reinterpret_cast<char*>(payload), sizeof(payload));
    EXPECT_TRUE(raw.good());
    EXPECT_EQ((std::vector<float>(payload, payload + 4)),
              (std::vector<float>{1, 2, 3, 4}));
    EXPECT_FALSE(std::filesystem::exists(offsets_path));
    std::ifstream valid(validity_path, std::ios::binary);
    uint64_t count = 0;
    uint8_t bits = 0;
    valid.read(reinterpret_cast<char*>(&count), sizeof(count));
    valid.read(reinterpret_cast<char*>(&bits), sizeof(bits));
    ASSERT_TRUE(valid.good());
    EXPECT_EQ(count, 5);
    EXPECT_EQ(bits, 0b00101);
}

TEST_F(VectorArrayBuildSessionTest, ResidentElementLevelUsesElementValidity) {
    auto prepared = PrepareElementNullable(2);
    prepared.dim = 2;
    prepared.params[DIM_KEY] = 2;
    prepared.params[index::METRIC_TYPE] = "L2";
    uint64_t selected = 5, none = 0;
    float values[] = {1, 2, 3, 4};
    VectorArray rows[] = {VectorArray(values,
                                      3,
                                      2,
                                      DataType::VECTOR_FLOAT,
                                      TargetBitmapView(&selected, 3),
                                      true),
                          VectorArray(nullptr,
                                      2,
                                      2,
                                      DataType::VECTOR_FLOAT,
                                      TargetBitmapView(&none, 2),
                                      true)};
    auto data =
        std::make_shared<FieldData<VectorArray>>(2, DataType::VECTOR_FLOAT);
    data->FillFieldData(rows, 2);
    auto stats = PublishElementNullable(prepared, data, 2);
    auto reader = Open(prepared, stats);
    EXPECT_EQ(reader->Count(), 2);
    EXPECT_EQ(reader->GetLogicalCount(), 5);
    EXPECT_EQ(reader->GetValidCount(), 2);
    for (int64_t i = 0; i < 5; ++i)
        EXPECT_EQ(reader->IsRowValid(i), i == 0 || i == 2);
    SearchInfo search;
    search.metric_type_ = "L2";
    search.topk_ = 2;
    search.search_params_ = Config{{knowhere::indexparam::EF, 16}};
    SearchResult result;
    reader->Query(
        knowhere::GenDataSet(2, 2, values), search, nullptr, nullptr, result);
    ASSERT_EQ(result.seg_offsets_.size(), 4);
    EXPECT_EQ(result.seg_offsets_[0], 0);
    EXPECT_EQ(result.seg_offsets_[2], 2);
}

// Reproduce the old VECTOR_ARRAY build mode with an embedding-list engine
// and plain metric parameters, then load it through the 3.0 production loader.
TEST_F(VectorArrayBuildSessionTest,
       LegacyElementLevelEmbeddingListIndexLoadsAndSearches) {
    for (const std::string metric : {"L2", "IP", "COSINE"}) {
        for (bool nullable : {false, true}) {
            for (bool empty : {false, true}) {
                for (bool mmap : {false, true}) {
                    SCOPED_TRACE(metric);
                    SCOPED_TRACE(nullable);
                    SCOPED_TRACE(empty);
                    SCOPED_TRACE(mmap);
                    auto prepared = Prepare(
                        DataType::VECTOR_ARRAY,
                        DataType::VECTOR_FLOAT,
                        nullable,
                        knowhere::IndexEnum::INDEX_HNSW,
                        4,
                        4,
                        Config{{index::METRIC_TYPE, metric},
                               {knowhere::indexparam::HNSW_M, 8},
                               {knowhere::indexparam::EFCONSTRUCTION, 16}});
                    const std::array<bool, 4> valid{
                        true, true, !nullable, true};
                    const std::vector<float> values =
                        empty ? std::vector<float>{}
                              : std::vector<float>{
                                    1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0};
                    const std::vector<size_t> offsets =
                        empty ? std::vector<size_t>(nullable ? 4 : 5, 0)
                              : (nullable ? std::vector<size_t>{0, 2, 2, 3}
                                          : std::vector<size_t>{0, 2, 2, 2, 3});
                    auto dataset =
                        knowhere::GenDataSet(empty ? 0 : 3, 4, values.data());
                    dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                                 offsets.data());
                    dataset->Set(knowhere::meta::EMB_LIST_COUNT,
                                 static_cast<int64_t>(offsets.size() - 1));
                    // The 3.0 BuildWithDataset empty-list path requires an
                    // IdMap even for non-nullable parents (all valid here).
                    if (nullable || empty)
                        dataset->SetIdMapData(
                            knowhere::IdMapData::FromValidData(valid.data(),
                                                               4));
                    index::VectorMemIndex<float> builder(
                        DataType::VECTOR_FLOAT,
                        knowhere::IndexEnum::INDEX_HNSW,
                        knowhere::metric::MAX_SIM_L2,
                        knowhere::Version::GetCurrentVersion().VersionNumber(),
                        true,
                        prepared.file_manager_context);
                    builder.BuildWithDataset(dataset, prepared.params);
                    auto stats = builder.Upload(prepared.params);
                    auto reader = Open(prepared, stats, mmap);
                    EXPECT_EQ(reader->GetDim(), 4);
                    EXPECT_EQ(reader->Count(), empty ? 0 : 3);
                    const std::vector<float> query{0, 1, 0, 0};
                    SearchInfo search;
                    search.metric_type_ = metric;
                    search.topk_ = 3;
                    search.search_params_ =
                        Config{{knowhere::indexparam::EF, 16}};
                    SearchResult result;
                    reader->Query(knowhere::GenDataSet(1, 4, query.data()),
                                  search,
                                  nullptr,
                                  nullptr,
                                  result);
                    ASSERT_EQ(result.seg_offsets_.size(), 3);
                    if (empty) {
                        for (auto id : result.seg_offsets_)
                            EXPECT_EQ(id, INVALID_SEG_OFFSET);
                    } else {
                        EXPECT_EQ(result.seg_offsets_[0], 1);
                        EXPECT_NEAR(
                            result.distances_[0], metric == "L2" ? 0 : 1, 1e-5);
                        EXPECT_EQ(
                            (std::set<int64_t>(result.seg_offsets_.begin(),
                                               result.seg_offsets_.end())),
                            (std::set<int64_t>{0, 1, 2}));
                    }
                }
            }
        }
    }
}
}  // namespace
}  // namespace milvus::indexbuilder::test
