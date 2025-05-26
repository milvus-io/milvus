// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <boost/format.hpp>
#include <optional>

#include "index/IndexFactory.h"
#include "pb/plan.pb.h"
#include "query/Plan.h"
#include "segcore/segcore_init_c.h"
#include "segcore/SegmentSealed.h"

#include "test_cachinglayer/cachinglayer_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

std::unique_ptr<float[]>
GenRandomFloatVecData(int rows, int dim, int seed = 42) {
    auto vecs = std::make_unique<float[]>(rows * dim);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<> distrib(0.0, 100.0);
    for (int i = 0; i < rows * dim; ++i) vecs[i] = (float)distrib(rng);
    return vecs;
}

inline float
GetKnnSearchRecall(
    size_t nq, int64_t* gt_ids, size_t gt_k, int64_t* res_ids, size_t res_k) {
    uint32_t matched_num = 0;
    for (auto i = 0; i < nq; ++i) {
        std::vector<int64_t> ids_0(gt_ids + i * gt_k,
                                   gt_ids + i * gt_k + res_k);
        std::vector<int64_t> ids_1(res_ids + i * res_k,
                                   res_ids + i * res_k + res_k);

        std::sort(ids_0.begin(), ids_0.end());
        std::sort(ids_1.begin(), ids_1.end());

        std::vector<int64_t> v(std::max(ids_0.size(), ids_1.size()));
        std::vector<int64_t>::iterator it;
        it = std::set_intersection(
            ids_0.begin(), ids_0.end(), ids_1.begin(), ids_1.end(), v.begin());
        v.resize(it - v.begin());
        matched_num += v.size();
    }
    return ((float)matched_num) / ((float)nq * res_k);
}

using Param =
    std::tuple<DataType, knowhere::MetricType, /* IndexType */ std::string>;
class BinlogIndexTest : public ::testing::TestWithParam<Param> {
    void
    SetUp() override {
        std::tie(data_type, metric_type, index_type) = GetParam();

        schema = std::make_shared<Schema>();

        vec_field_id =
            schema->AddDebugField("fakevec", data_type, data_d, metric_type);
        auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
        schema->set_primary_field_id(i64_fid);
        vec_field_data = storage::CreateFieldData(data_type, false, data_d);

        if (data_type == DataType::VECTOR_FLOAT) {
            auto vec_data = GenRandomFloatVecData(data_n, data_d);
            vec_field_data->FillFieldData(vec_data.get(), data_n);
            raw_dataset = knowhere::GenDataSet(data_n, data_d, vec_data.get());
            raw_dataset->SetIsOwner(true);
            vec_data.release();
        } else if (data_type == DataType::VECTOR_SPARSE_FLOAT) {
            auto sparse_vecs = GenerateRandomSparseFloatVector(data_n);
            vec_field_data->FillFieldData(sparse_vecs.get(), data_n);
            data_d = std::dynamic_pointer_cast<
                         milvus::FieldData<milvus::SparseFloatVector>>(
                         vec_field_data)
                         ->Dim();
            raw_dataset =
                knowhere::GenDataSet(data_n, data_d, sparse_vecs.get());
            raw_dataset->SetIsOwner(true);
            raw_dataset->SetIsSparse(true);
            sparse_vecs.release();
        } else {
            throw std::runtime_error("not implemented");
        }
    }

 public:
    IndexMetaPtr
    GetCollectionIndexMeta(std::string index_type) {
        std::map<std::string, std::string> index_params = {
            {"index_type", index_type},
            {"metric_type", metric_type},
            {"nlist", "1024"}};
        std::map<std::string, std::string> type_params = {{"dim", "128"}};
        FieldIndexMeta fieldIndexMeta(
            vec_field_id, std::move(index_params), std::move(type_params));
        auto& config = SegcoreConfig::default_config();
        config.set_chunk_rows(1024);
        config.set_enable_interim_segment_index(true);
        std::map<FieldId, FieldIndexMeta> filedMap = {
            {vec_field_id, fieldIndexMeta}};
        IndexMetaPtr metaPtr =
            std::make_shared<CollectionIndexMeta>(226985, std::move(filedMap));
        return std::move(metaPtr);
    }

    void
    LoadOtherFields() {
        auto dataset = DataGen(schema, data_n);
        auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto load_info = PrepareInsertBinlog(kCollectionID,
                                             kPartitionID,
                                             kSegmentID,
                                             dataset,
                                             cm,
                                             "",
                                             {vec_field_id.get()});
        segment->LoadFieldData(load_info);
    }

    void
    LoadVectorField(std::string mmap_dir_path = "") {
        auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto load_info = PrepareSingleFieldInsertBinlog(kCollectionID,
                                                        kPartitionID,
                                                        kSegmentID,
                                                        vec_field_id.get(),
                                                        {vec_field_data},
                                                        cm,
                                                        mmap_dir_path);
        segment->LoadFieldData(load_info);
    }

 protected:
    milvus::SchemaPtr schema;
    knowhere::MetricType metric_type;
    DataType data_type;
    std::string index_type;
    size_t data_n = 10000;
    size_t data_d = 128;
    size_t topk = 10;
    milvus::FieldDataPtr vec_field_data = nullptr;
    milvus::segcore::SegmentSealedUPtr segment = nullptr;
    milvus::FieldId vec_field_id;
    knowhere::DataSetPtr raw_dataset;
};

INSTANTIATE_TEST_SUITE_P(
    MetricTypeParameters,
    BinlogIndexTest,
    ::testing::Values(
        std::make_tuple(DataType::VECTOR_FLOAT,
                        knowhere::metric::L2,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT),
        std::make_tuple(DataType::VECTOR_SPARSE_FLOAT,
                        knowhere::metric::IP,
                        knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX),
        std::make_tuple(DataType::VECTOR_SPARSE_FLOAT,
                        knowhere::metric::IP,
                        knowhere::IndexEnum::INDEX_SPARSE_WAND)));

TEST_P(BinlogIndexTest, AccuracyWithLoadFieldData) {
    IndexMetaPtr collection_index_meta = GetCollectionIndexMeta(index_type);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();

    auto& segcore_config = milvus::segcore::SegcoreConfig::default_config();
    segcore_config.set_enable_interim_segment_index(true);
    segcore_config.set_nprobe(32);
    // 1. load field data, and build binlog index for binlog data
    LoadVectorField();

    //assert segment has been built binlog index
    EXPECT_TRUE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));

    // 2. search binlog index
    auto num_queries = 10;

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(vec_field_id.get());
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(topk);
    query_info->set_round_decimal(3);
    query_info->set_metric_type(metric_type);
    query_info->set_search_params(R"({"nprobe": 1024})");
    auto plan_str = plan_node.SerializeAsString();

    auto ph_group_raw =
        data_type == DataType::VECTOR_FLOAT
            ? CreatePlaceholderGroupFromBlob(
                  num_queries,
                  data_d,
                  GenRandomFloatVecData(num_queries, data_d).get())
            : CreateSparseFloatPlaceholderGroup(num_queries);

    auto plan = milvus::query::CreateSearchPlanByExpr(
        *schema, plan_str.data(), plan_str.size());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    std::vector<const milvus::query::PlaceholderGroup*> ph_group_arr = {
        ph_group.get()};
    auto nlist = segcore_config.get_nlist();
    auto binlog_index_sr =
        segment->Search(plan.get(), ph_group.get(), 1L << 63, 0);
    ASSERT_EQ(binlog_index_sr->total_nq_, num_queries);
    EXPECT_EQ(binlog_index_sr->unity_topK_, topk);
    EXPECT_EQ(binlog_index_sr->distances_.size(), num_queries * topk);
    EXPECT_EQ(binlog_index_sr->seg_offsets_.size(), num_queries * topk);

    // 3. update vector index
    {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = data_type;
        create_index_info.metric_type = metric_type;
        create_index_info.index_type = index_type;
        create_index_info.index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber();
        auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
            create_index_info, milvus::storage::FileManagerContext());

        auto build_conf =
            knowhere::Json{{knowhere::meta::METRIC_TYPE, metric_type},
                           {knowhere::meta::DIM, std::to_string(data_d)},
                           {knowhere::indexparam::NLIST, "1024"}};
        indexing->BuildWithDataset(raw_dataset, build_conf);

        LoadIndexInfo load_info;
        load_info.field_id = vec_field_id.get();
        load_info.index_params = GenIndexParams(indexing.get());
        load_info.cache_index =
            CreateTestCacheIndex("test", std::move(indexing));
        load_info.index_params["metric_type"] = metric_type;
        ASSERT_NO_THROW(segment->LoadIndex(load_info));
        EXPECT_TRUE(segment->HasIndex(vec_field_id));
        EXPECT_EQ(segment->get_row_count(), data_n);
        // only INDEX_FAISS_IVFFLAT has raw data, thus it should release the raw field data.
        EXPECT_EQ(segment->HasFieldData(vec_field_id), index_type != knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);
        auto ivf_sr = segment->Search(plan.get(), ph_group.get(), 1L << 63, 0);
        auto similary = GetKnnSearchRecall(num_queries,
                                           binlog_index_sr->seg_offsets_.data(),
                                           topk,
                                           ivf_sr->seg_offsets_.data(),
                                           topk);
        ASSERT_GT(similary, 0.45);
    }
}

TEST_P(BinlogIndexTest, AccuracyWithMapFieldData) {
    IndexMetaPtr collection_index_meta = GetCollectionIndexMeta(index_type);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();

    auto& segcore_config = milvus::segcore::SegcoreConfig::default_config();
    segcore_config.set_enable_interim_segment_index(true);
    segcore_config.set_nprobe(32);
    // 1. load field data, and build binlog index for binlog data
    LoadVectorField("./data/mmap-test");

    //assert segment has been built binlog index
    EXPECT_TRUE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));

    // 2. search binlog index
    auto num_queries = 10;

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(vec_field_id.get());

    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(topk);
    query_info->set_round_decimal(3);
    query_info->set_metric_type(metric_type);
    query_info->set_search_params(R"({"nprobe": 1024})");
    auto plan_str = plan_node.SerializeAsString();

    auto ph_group_raw =
        data_type == DataType::VECTOR_FLOAT
            ? CreatePlaceholderGroupFromBlob(
                  num_queries,
                  data_d,
                  GenRandomFloatVecData(num_queries, data_d).get())
            : CreateSparseFloatPlaceholderGroup(num_queries);

    auto plan = milvus::query::CreateSearchPlanByExpr(
        *schema, plan_str.data(), plan_str.size());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    std::vector<const milvus::query::PlaceholderGroup*> ph_group_arr = {
        ph_group.get()};
    auto nlist = segcore_config.get_nlist();
    auto binlog_index_sr =
        segment->Search(plan.get(), ph_group.get(), 1L << 63, 0);
    ASSERT_EQ(binlog_index_sr->total_nq_, num_queries);
    EXPECT_EQ(binlog_index_sr->unity_topK_, topk);
    EXPECT_EQ(binlog_index_sr->distances_.size(), num_queries * topk);
    EXPECT_EQ(binlog_index_sr->seg_offsets_.size(), num_queries * topk);

    // 3. update vector index
    {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = data_type;
        create_index_info.metric_type = metric_type;
        create_index_info.index_type = index_type;
        create_index_info.index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber();
        auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
            create_index_info, milvus::storage::FileManagerContext());

        auto build_conf =
            knowhere::Json{{knowhere::meta::METRIC_TYPE, metric_type},
                           {knowhere::meta::DIM, std::to_string(data_d)},
                           {knowhere::indexparam::NLIST, "1024"}};
        indexing->BuildWithDataset(raw_dataset, build_conf);

        LoadIndexInfo load_info;
        load_info.field_id = vec_field_id.get();
        load_info.index_params = GenIndexParams(indexing.get());
        load_info.cache_index =
            CreateTestCacheIndex("test", std::move(indexing));
        load_info.index_params["metric_type"] = metric_type;
        ASSERT_NO_THROW(segment->LoadIndex(load_info));
        EXPECT_TRUE(segment->HasIndex(vec_field_id));
        EXPECT_EQ(segment->get_row_count(), data_n);
        EXPECT_EQ(segment->HasFieldData(vec_field_id), index_type != knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);
        auto ivf_sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
        auto similary = GetKnnSearchRecall(num_queries,
                                           binlog_index_sr->seg_offsets_.data(),
                                           topk,
                                           ivf_sr->seg_offsets_.data(),
                                           topk);
        ASSERT_GT(similary, 0.45);
    }
}

TEST_P(BinlogIndexTest, DisableInterimIndex) {
    IndexMetaPtr collection_index_meta = GetCollectionIndexMeta(index_type);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();
    SegcoreSetEnableTempSegmentIndex(false);

    LoadVectorField();

    EXPECT_FALSE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));
    // load vector index
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = data_type;
    create_index_info.metric_type = metric_type;
    create_index_info.index_type = index_type;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());

    auto build_conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, metric_type},
                       {knowhere::meta::DIM, std::to_string(data_d)},
                       {knowhere::indexparam::NLIST, "1024"}};

    indexing->BuildWithDataset(raw_dataset, build_conf);

    LoadIndexInfo load_info;
    load_info.field_id = vec_field_id.get();
    load_info.index_params = GenIndexParams(indexing.get());
    load_info.cache_index = CreateTestCacheIndex("test", std::move(indexing));
    load_info.index_params["metric_type"] = metric_type;

    ASSERT_NO_THROW(segment->LoadIndex(load_info));
    EXPECT_TRUE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_EQ(segment->HasFieldData(vec_field_id),
              index_type != knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);
}

TEST_P(BinlogIndexTest, LoadBingLogWihIDMAP) {
    IndexMetaPtr collection_index_meta =
        GetCollectionIndexMeta(knowhere::IndexEnum::INDEX_FAISS_IDMAP);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();
    LoadVectorField();

    EXPECT_FALSE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));
}

TEST_P(BinlogIndexTest, LoadBinlogWithoutIndexMeta) {
    IndexMetaPtr collection_index_meta =
        GetCollectionIndexMeta(knowhere::IndexEnum::INDEX_FAISS_IDMAP);

    segment = CreateSealedSegment(schema, collection_index_meta);
    SegcoreSetEnableTempSegmentIndex(true);

    LoadVectorField();

    EXPECT_FALSE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));
}
