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
#include <arrow/array/array_base.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <parquet/properties.h>
#include <stddef.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
#include "segcore/default_fs.h"

#include "NamedType/named_type_impl.hpp"
#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/LoadInfo.h"
#include "common/OpContext.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/query/IVectorReader.h"
#include "indexbuilder/BuildSession.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/config.h"
#include "knowhere/dataset.h"
#include "knowhere/index/emb_list_strategy.h"
#include "knowhere/version.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/packed/writer.h"
#include "pb/common.pb.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "segcore/TimestampIndex.h"
#include "storage/FileManager.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/artifact/ArtifactStats.h"
#include "storage/artifact/LoadOptions.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::storage;

namespace {
constexpr int64_t DIM = 4;
}

SchemaPtr
GenVectorArrayTestSchema() {
    auto schema = std::make_shared<Schema>();
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->AddDebugVectorArrayField(
        "vector_array", DataType::VECTOR_FLOAT, DIM, knowhere::metric::L2);
    schema->AddField(FieldName("ts"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);
    schema->set_primary_field_id(int64_fid);
    return schema;
}

std::vector<float>
GenerateDistinctFloatVectors(int64_t row_id, int64_t vec_num, int64_t dim) {
    std::vector<float> data(vec_num * dim);
    for (int64_t vec_idx = 0; vec_idx < vec_num; ++vec_idx) {
        std::default_random_engine engine(
            static_cast<unsigned int>(42 + row_id * vec_num + vec_idx));
        std::normal_distribution<float> distribution(0.0F, 1.0F);
        float sum = 0.0F;
        for (int64_t dim_idx = 0; dim_idx < dim; ++dim_idx) {
            auto& value = data[vec_idx * dim + dim_idx];
            value =
                distribution(engine) +
                static_cast<float>((row_id + 1) * 0.01 + (vec_idx + 1) * 0.001);
            sum += value * value;
        }
        sum = std::sqrt(sum);
        for (int64_t dim_idx = 0; dim_idx < dim; ++dim_idx) {
            data[vec_idx * dim + dim_idx] /= sum;
        }
    }
    return data;
}

class TestVectorArrayStorageV2 : public testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = GenVectorArrayTestSchema();
        segment_ = segcore::CreateSealedSegment(
            schema_,
            nullptr,
            -1,
            segcore::SegcoreConfig::default_config(),
            true);

        auto fs = milvus::segcore::GetDefaultArrowFileSystem();

        // Prepare paths and column groups
        std::vector<std::string> paths = {
            TestLocalPath + "test_data/0/10000.parquet",
            TestLocalPath + "test_data/101/10001.parquet"};

        // Create directories for the parquet files
        for (const auto& path : paths) {
            auto dir_path = path.substr(0, path.find_last_of('/'));
            auto status = fs->CreateDir(dir_path);
            EXPECT_TRUE(status.ok())
                << "Failed to create directory: " << dir_path;
        }

        std::vector<std::vector<int>> column_groups = {
            {0, 2}, {1}};  // narrow columns and wide columns
        auto writer_memory = 16 * 1024 * 1024;
        auto storage_config = milvus_storage::StorageConfig();

        // Create writer
        auto result = milvus_storage::PackedRecordBatchWriter::Make(
            fs,
            paths,
            schema_->ConvertToArrowSchema(),
            storage_config,
            column_groups,
            writer_memory,
            ::parquet::default_writer_properties());
        EXPECT_TRUE(result.ok());
        auto writer = result.ValueOrDie();

        // Generate and write data
        int64_t row_count = 0;
        int start_id = 0;

        std::vector<std::string> str_data;
        for (int i = 0; i < test_data_count_ * chunk_num_; i++) {
            str_data.push_back("test" + std::to_string(i));
        }
        std::sort(str_data.begin(), str_data.end());

        fields_ = {
            {"int64", schema_->get_field_id(FieldName("int64"))},
            {"ts", TimestampFieldID},
            {"vector_array", schema_->get_field_id(FieldName("vector_array"))}};

        auto arrow_schema = schema_->ConvertToArrowSchema();
        for (int chunk_id = 0; chunk_id < chunk_num_;
             chunk_id++, start_id += test_data_count_) {
            std::vector<int64_t> test_data(test_data_count_);
            std::iota(test_data.begin(), test_data.end(), start_id);

            // Create arrow arrays for each field
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            for (int i = 0; i < arrow_schema->fields().size(); i++) {
                if (arrow_schema->fields()[i]->type()->id() ==
                    arrow::Type::INT64) {
                    arrow::Int64Builder builder;
                    auto status = builder.AppendValues(test_data.data(),
                                                       test_data_count_);
                    EXPECT_TRUE(status.ok());
                    std::shared_ptr<arrow::Array> array;
                    status = builder.Finish(&array);
                    EXPECT_TRUE(status.ok());
                    arrays.push_back(array);
                } else {
                    // vector array - using ListArray
                    // Get field meta to determine element type
                    auto vector_array_field_id = fields_["vector_array"];
                    auto& field_meta =
                        schema_->operator[](vector_array_field_id);
                    auto element_type = field_meta.get_element_type();

                    // Create appropriate value builder based on element type
                    std::shared_ptr<arrow::ArrayBuilder> value_builder;
                    int byte_width = 0;
                    if (element_type == DataType::VECTOR_FLOAT) {
                        byte_width = DIM * sizeof(float);
                        value_builder =
                            std::make_shared<arrow::FixedSizeBinaryBuilder>(
                                arrow::fixed_size_binary(byte_width));
                    } else {
                        FAIL() << "Unsupported element type for VECTOR_ARRAY "
                                  "in test";
                    }

                    auto list_builder = std::make_shared<arrow::ListBuilder>(
                        arrow::default_memory_pool(), value_builder);

                    for (int row = 0; row < test_data_count_; row++) {
                        // Each row contains 3 vectors of dimension DIM
                        auto status = list_builder->Append();
                        EXPECT_TRUE(status.ok());

                        // Generate 3 vectors for this row
                        auto data = GenerateDistinctFloatVectors(
                            chunk_id * test_data_count_ + row, 3, DIM);
                        auto binary_builder = std::static_pointer_cast<
                            arrow::FixedSizeBinaryBuilder>(value_builder);
                        // Append each vector as a fixed-size binary value
                        for (int vec_idx = 0; vec_idx < 3; vec_idx++) {
                            status = binary_builder->Append(
                                reinterpret_cast<const uint8_t*>(
                                    data.data() + vec_idx * DIM));
                            EXPECT_TRUE(status.ok());
                        }
                    }

                    std::shared_ptr<arrow::Array> array;
                    auto status = list_builder->Finish(&array);
                    EXPECT_TRUE(status.ok());
                    arrays.push_back(array);
                }
            }

            // Create record batch
            auto record_batch = arrow::RecordBatch::Make(
                schema_->ConvertToArrowSchema(), test_data_count_, arrays);
            row_count += test_data_count_;
            EXPECT_TRUE(writer->Write(record_batch).ok());
        }
        EXPECT_TRUE(writer->Close().ok());

        LoadFieldDataInfo load_info;
        load_info.field_infos.emplace(
            int64_t(0),
            FieldBinlogInfo{
                int64_t(0),
                static_cast<int64_t>(row_count),
                std::vector<int64_t>(chunk_num_ * test_data_count_),
                std::vector<int64_t>(chunk_num_ * test_data_count_ * 4),
                false,
                "",
                std::vector<std::string>({paths[0]})});
        load_info.field_infos.emplace(
            int64_t(101),
            FieldBinlogInfo{int64_t(101),
                            static_cast<int64_t>(row_count),
                            std::vector<int64_t>(chunk_num_ * test_data_count_),
                            std::vector<int64_t>(chunk_num_ * test_data_count_ *
                                                 10 * 4 * DIM),
                            false,
                            "",
                            std::vector<std::string>({paths[1]})});

        load_info.storage_version = 2;
        segment_->AddFieldDataInfoForSealed(load_info);
        for (auto& [id, info] : load_info.field_infos) {
            LoadFieldDataInfo load_field_info;
            load_field_info.storage_version = 2;
            load_field_info.field_infos.emplace(id, info);
            segment_->LoadFieldData(load_field_info);
        }
    }

    void
    TearDown() override {
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
        (void)fs->DeleteDir(TestLocalPath + "test_data");
    }

 protected:
    struct PublishedIndex {
        index::IndexFamily family;
        Config params;
        storage::FileManagerContext context;
        storage::ArtifactStats stats;
    };

    PublishedIndex
    PublishIndex(const std::vector<std::string>& paths,
                 FieldId field_id,
                 int64_t segment_id,
                 int64_t build_id,
                 int64_t index_version,
                 IndexVersion engine_version,
                 Config params) {
        auto field_meta = gen_field_meta(1,
                                        2,
                                        segment_id,
                                        field_id.get(),
                                        DataType::VECTOR_ARRAY,
                                        DataType::VECTOR_FLOAT,
                                        false);
        auto index_meta =
            gen_index_meta(segment_id, field_id.get(), build_id, index_version);
        index_meta.dim = DIM;
        auto cm = CreateChunkManager(gen_local_storage_config(TestLocalPath));
        storage::FileManagerContext context(
            field_meta, index_meta, cm, GetDefaultArrowFileSystem());
        auto adapted = index::AdaptIndexType(
            {.index_type = knowhere::IndexEnum::INDEX_HNSW,
             .field_type = DataType::VECTOR_ARRAY,
             .element_type = DataType::VECTOR_FLOAT,
             .index_engine_version = engine_version,
             .params = std::move(params)});
        adapted.params[DIM_KEY] = DIM;
        adapted.params["nullable"] = false;
        adapted.params["num_rows"] = test_data_count_ * chunk_num_;
        indexbuilder::BuildRequest request{
            .family = adapted.family,
            .params = adapted.params,
            .value_type = adapted.value_type,
            .field_id = field_id,
            .source = indexbuilder::StorageV2BuildSource{{paths}},
            .expected_rows = test_data_count_ * chunk_num_,
            .staging_parent = TestLocalPath};
        // Retain the actual parquet materialization/publication boundary.
        // No resident synthetic tensor replaces the StorageV2 source.
        indexbuilder::BuildSession session(std::move(request), context);
        session.BuildFromSource();
        auto stats = session.Publish();
        return {std::move(adapted.family),
                std::move(adapted.params),
                std::move(context),
                std::move(stats)};
    }

    index::IIndexReaderBasePtr
    OpenIndex(const PublishedIndex& published,
              bool mmap = false,
              const std::string& mmap_name = "") {
        std::vector<std::string> paths;
        for (const auto& file : published.stats.Files()) {
            paths.push_back(file.file_name);
        }
        storage::LoadOptions options;
        options.params = published.params;
        options.enable_mmap = mmap;
        options.mmap_dir_path = TestLocalPath + "mmap/" + mmap_name;
        auto context = published.context;
        context.set_for_loading_index(true);
        return index::LoaderRegistry::Instance()
            .Lookup(published.family)
            .Load({index::IndexFiles{
                       std::move(context),
                       std::move(paths),
                       index::LegacyIndexStorageConfig{
                           storage::V1SourceLayout::MemoryEntries}},
                   std::move(options)});
    }

    SchemaPtr schema_;
    segcore::SegmentSealedUPtr segment_;
    int chunk_num_ = 2;
    int test_data_count_ = 100;
    std::unordered_map<std::string, FieldId> fields_;
};

TEST_F(TestVectorArrayStorageV2, BuildEmbListHNSWIndex) {
    ASSERT_NE(segment_, nullptr);
    ASSERT_EQ(segment_->get_row_count(), test_data_count_ * chunk_num_);

    auto vector_array_field_id = fields_["vector_array"];
    ASSERT_TRUE(segment_->HasFieldData(vector_array_field_id));

    // Get the storage v2 parquet file paths that were already written in SetUp
    std::vector<std::string> paths = {TestLocalPath +
                                      "test_data/101/10001.parquet"};

    Config config;
    config[knowhere::meta::METRIC_TYPE] = knowhere::metric::MAX_SIM;
    config[knowhere::indexparam::M] = "16";
    config[knowhere::indexparam::EF] = "10";
    auto published = PublishIndex(
        paths,
        vector_array_field_id,
        3,
        4000,
        4000,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        std::move(config));
    auto owner = OpenIndex(published);
    auto vec_index = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(vec_index, nullptr);

    // Each row has 3 vectors, so total count should be rows * 3
    EXPECT_EQ(owner->Count(), test_data_count_ * chunk_num_ * 3);
    EXPECT_EQ(vec_index->Dim(), DIM);

    {
        auto vec_num = 10;
        std::vector<float> query_vec = generate_float_vector(vec_num, DIM);
        auto query_dataset =
            knowhere::GenDataSet(vec_num, DIM, query_vec.data());
        std::vector<size_t> query_vec_offsets;
        query_vec_offsets.push_back(0);
        query_vec_offsets.push_back(3);
        query_vec_offsets.push_back(10);
        query_dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                           const_cast<const size_t*>(query_vec_offsets.data()));
        query_dataset->Set(knowhere::meta::EMB_LIST_COUNT,
                           static_cast<int64_t>(query_vec_offsets.size() - 1));
        query_dataset->Set(knowhere::meta::NQ,
                           static_cast<int64_t>(query_vec_offsets.size() - 1));

        auto search_conf = knowhere::Json{{knowhere::indexparam::NPROBE, 10}};
        index::VectorSearchParams searchInfo;
        searchInfo.topk_ = 5;
        searchInfo.metric_type_ = knowhere::metric::MAX_SIM_IP;
        searchInfo.search_params_ = search_conf;
        SearchResult result;
        milvus::OpContext op_context;
        vec_index->Search(
            query_dataset, searchInfo, nullptr, &op_context, result);
        auto ref_result = SearchResultToJson(result);
        std::cout << ref_result.dump(1) << std::endl;
        EXPECT_EQ(result.total_nq_, 2);
        EXPECT_EQ(result.distances_.size(), 2 * searchInfo.topk_);
        EXPECT_EQ(op_context.storage_usage.scanned_cold_bytes, 0);
        EXPECT_EQ(op_context.storage_usage.scanned_total_bytes, 0);
    }
}

TEST_F(TestVectorArrayStorageV2, BuildEmbListHNSWIndexWithMmap) {
#ifdef __APPLE__
    // faiss MmappedFileMappingOwner is not implemented on macOS:
    // knowhere/thirdparty/faiss/impl/mapped_io.cpp only has Linux/FreeBSD
    // and Windows branches; the #else falls through to FAISS_THROW_MSG.
    // Skip until faiss adds __APPLE__ support upstream.
    GTEST_SKIP() << "faiss mmap not implemented on macOS (mapped_io.cpp)";
#endif
    ASSERT_NE(segment_, nullptr);
    ASSERT_EQ(segment_->get_row_count(), test_data_count_ * chunk_num_);

    auto vector_array_field_id = fields_["vector_array"];
    ASSERT_TRUE(segment_->HasFieldData(vector_array_field_id));

    // Get the storage v2 parquet file paths that were already written in SetUp
    std::vector<std::string> paths = {TestLocalPath +
                                      "test_data/101/10001.parquet"};

    Config config;
    config[knowhere::meta::METRIC_TYPE] = knowhere::metric::MAX_SIM_IP;
    config[knowhere::indexparam::M] = "16";
    config[knowhere::indexparam::EF] = "10";
    auto published = PublishIndex(
        paths,
        vector_array_field_id,
        3,
        4000,
        4000,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        std::move(config));
    ASSERT_GT(published.stats.MemSize(), 0);
    const auto serialized_size = std::accumulate(
        published.stats.Files().begin(),
        published.stats.Files().end(),
        int64_t{0},
        [](int64_t size, const auto& file) { return size + file.file_size; });
    ASSERT_GT(serialized_size, 0);

    auto owner = OpenIndex(published, true, "test_emb_list");
    auto vec_index = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(vec_index, nullptr);
    // search
    {
        // Each row has 3 vectors, so total count should be rows * 3
        EXPECT_EQ(owner->Count(), test_data_count_ * chunk_num_ * 3);
        EXPECT_EQ(vec_index->Dim(), DIM);
        auto vec_num = 10;
        std::vector<float> query_vec = generate_float_vector(vec_num, DIM);
        auto query_dataset =
            knowhere::GenDataSet(vec_num, DIM, query_vec.data());
        std::vector<size_t> query_vec_lims;
        query_vec_lims.push_back(0);
        query_vec_lims.push_back(3);
        query_vec_lims.push_back(10);
        query_dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                           const_cast<const size_t*>(query_vec_lims.data()));
        query_dataset->Set(knowhere::meta::EMB_LIST_COUNT,
                           static_cast<int64_t>(query_vec_lims.size() - 1));
        query_dataset->Set(knowhere::meta::NQ,
                           static_cast<int64_t>(query_vec_lims.size() - 1));

        auto search_conf = knowhere::Json{{knowhere::indexparam::NPROBE, 10}};
        index::VectorSearchParams searchInfo;
        searchInfo.topk_ = 5;
        searchInfo.metric_type_ = knowhere::metric::MAX_SIM_IP;
        searchInfo.search_params_ = search_conf;
        SearchResult result;
        milvus::OpContext op_context;
        vec_index->Search(
            query_dataset, searchInfo, nullptr, &op_context, result);
        auto ref_result = SearchResultToJson(result);
        std::cout << ref_result.dump(1) << std::endl;
        EXPECT_EQ(result.total_nq_, 2);
        EXPECT_EQ(result.distances_.size(), 2 * searchInfo.topk_);
        EXPECT_EQ(op_context.storage_usage.scanned_cold_bytes, 0);
        EXPECT_EQ(op_context.storage_usage.scanned_total_bytes, 0);
    }
}

TEST_F(TestVectorArrayStorageV2, BuildEncodedEmbListHNSWIndexWithMmap) {
#ifdef __APPLE__
    // faiss MmappedFileMappingOwner is not implemented on macOS.
    GTEST_SKIP() << "faiss mmap not implemented on macOS (mapped_io.cpp)";
#endif
    ASSERT_NE(segment_, nullptr);
    ASSERT_EQ(segment_->get_row_count(), test_data_count_ * chunk_num_);

    auto vector_array_field_id = fields_["vector_array"];
    ASSERT_TRUE(segment_->HasFieldData(vector_array_field_id));

    std::vector<std::string> paths = {TestLocalPath +
                                      "test_data/101/10001.parquet"};

    const std::vector<std::string> strategies = {
        knowhere::meta::EMB_LIST_STRATEGY_MUVERA,
        knowhere::meta::EMB_LIST_STRATEGY_LEMUR,
    };

    for (size_t i = 0; i < strategies.size(); ++i) {
        const auto& strategy = strategies[i];
        SCOPED_TRACE(strategy);

        Config config;
        config[knowhere::meta::METRIC_TYPE] = knowhere::metric::MAX_SIM_COSINE;
        config[knowhere::meta::ROWS] = test_data_count_ * chunk_num_ * 3;
        config[knowhere::indexparam::HNSW_M] = "16";
        config[knowhere::indexparam::EFCONSTRUCTION] = "96";
        config[knowhere::indexparam::EF] = "64";
        config["emb_list_strategy"] = strategy;
        if (strategy == knowhere::meta::EMB_LIST_STRATEGY_MUVERA) {
            config["muvera_num_projections"] = "3";
            config["muvera_num_repeats"] = "5";
            config["muvera_seed"] = "42";
        } else {
            config["lemur_hidden_dim"] = "32";
            config["lemur_num_train_samples"] = "1000";
            config["lemur_num_epochs"] = "2";
            config["lemur_batch_size"] = "16";
            config["lemur_learning_rate"] = "0.001";
            config["lemur_seed"] = "42";
            config["lemur_num_layers"] = "1";
        }

        auto published = PublishIndex(paths,
                                      vector_array_field_id,
                                      30 + i,
                                      5000 + i,
                                      5000 + i,
                                      knowhere::kEmbListMetaV2MinVersion,
                                      std::move(config));
        ASSERT_GT(published.stats.MemSize(), 0);
        const auto serialized_size = std::accumulate(
            published.stats.Files().begin(),
            published.stats.Files().end(),
            int64_t{0},
            [](int64_t size, const auto& file) {
                return size + file.file_size;
            });
        ASSERT_GT(serialized_size, 0);

        auto materialized_owner = OpenIndex(published);
        auto materialized_index =
            dynamic_cast<const index::IVectorReader*>(materialized_owner.get());
        ASSERT_NE(materialized_index, nullptr);
        EXPECT_EQ(materialized_index->Dim(), DIM);

        auto mismatched = published;
        mismatched.params[DIM_KEY] = DIM + 1;
        for (const bool mmap : {false, true}) {
            EXPECT_THROW(OpenIndex(mismatched,
                                   mmap,
                                   "test_emb_list_bad_dim_" + strategy),
                         SegcoreError);
        }

        auto owner = OpenIndex(published, true, "test_emb_list_" + strategy);
        auto vec_index = dynamic_cast<const index::IVectorReader*>(owner.get());
        ASSERT_NE(vec_index, nullptr);
        EXPECT_GT(owner->Count(), 0);
        EXPECT_EQ(vec_index->Dim(), DIM);

        const std::vector<int64_t> ids = {0, 1};
        const auto [raw_vectors, offsets] = vec_index->GetEmbListByIds(
            knowhere::GenIdsDataSet(ids.size(), ids.data()),
            knowhere::metric::MAX_SIM_COSINE);
        EXPECT_EQ(offsets, (std::vector<size_t>{0, 3, 6}));
        EXPECT_EQ(raw_vectors.size(), 6 * DIM * sizeof(float));

        auto vec_num = 10;
        std::vector<float> query_vec = generate_float_vector(vec_num, DIM);
        auto query_dataset =
            knowhere::GenDataSet(vec_num, DIM, query_vec.data());
        std::vector<size_t> query_vec_lims = {0, 3, 10};
        query_dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                           const_cast<const size_t*>(query_vec_lims.data()));
        query_dataset->Set(knowhere::meta::EMB_LIST_COUNT,
                           static_cast<int64_t>(query_vec_lims.size() - 1));
        query_dataset->Set(knowhere::meta::NQ,
                           static_cast<int64_t>(query_vec_lims.size() - 1));

        auto search_conf = knowhere::Json{
            {knowhere::indexparam::EF, 64},
            {knowhere::indexparam::RETRIEVAL_ANN_RATIO, 3.0},
            {"emb_list_rerank", true},
        };
        index::VectorSearchParams searchInfo;
        searchInfo.topk_ = 5;
        searchInfo.metric_type_ = knowhere::metric::MAX_SIM_COSINE;
        searchInfo.search_params_ = search_conf;
        SearchResult result;
        milvus::OpContext op_context;
        vec_index->Search(
            query_dataset, searchInfo, nullptr, &op_context, result);

        EXPECT_EQ(result.total_nq_, 2);
        EXPECT_EQ(result.distances_.size(), 2 * searchInfo.topk_);
    }
}
