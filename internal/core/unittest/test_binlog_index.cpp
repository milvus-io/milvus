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
#include <regex>

#include "pb/plan.pb.h"
#include "segcore/segcore_init_c.h"
#include "segcore/SegmentSealed.h"
#include "segcore/SegmentSealedImpl.h"
#include "pb/schema.pb.h"
#include "test_utils/DataGen.h"
#include "index/IndexFactory.h"
#include "query/Plan.h"
#include "knowhere/comp/brute_force.h"

using namespace milvus::segcore;
using namespace milvus;
namespace pb = milvus::proto;

std::shared_ptr<float[]>
GenRandomFloatVecData(int rows, int dim, int seed = 42) {
    std::shared_ptr<float[]> vecs =
        std::shared_ptr<float[]>(new float[rows * dim]);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<> distrib(0.0, 100.0);
    for (int i = 0; i < rows * dim; ++i) vecs[i] = (float)distrib(rng);
    return std::move(vecs);
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

using Param = const char*;
class BinlogIndexTest : public ::testing::TestWithParam<Param> {
    void
    SetUp() override {
        auto param = GetParam();
        metricType = param;

        schema = std::make_shared<Schema>();

        auto metric_type = metricType;
        vec_field_id = schema->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, data_d, metric_type);
        auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
        schema->set_primary_field_id(i64_fid);

        // generate vector field data
        vec_data = GenRandomFloatVecData(data_n, data_d);

        vec_field_data =
            storage::CreateFieldData(DataType::VECTOR_FLOAT, data_d);
        vec_field_data->FillFieldData(vec_data.get(), data_n);
    }

 public:
    IndexMetaPtr
    GetCollectionIndexMeta(std::string index_type) {
        std::map<std::string, std::string> index_params = {
            {"index_type", index_type},
            {"metric_type", metricType},
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
        // load id
        LoadFieldDataInfo row_id_info;
        FieldMeta row_id_field_meta(
            FieldName("RowID"), RowFieldID, DataType::INT64);
        auto field_data =
            std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64);
        field_data->FillFieldData(dataset.row_ids_.data(), data_n);
        auto field_data_info = FieldDataInfo{
            RowFieldID.get(), data_n, std::vector<FieldDataPtr>{field_data}};
        segment->LoadFieldData(RowFieldID, field_data_info);
        // load ts
        LoadFieldDataInfo ts_info;
        FieldMeta ts_field_meta(
            FieldName("Timestamp"), TimestampFieldID, DataType::INT64);
        field_data =
            std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64);
        field_data->FillFieldData(dataset.timestamps_.data(), data_n);
        field_data_info = FieldDataInfo{TimestampFieldID.get(),
                                        data_n,
                                        std::vector<FieldDataPtr>{field_data}};
        segment->LoadFieldData(TimestampFieldID, field_data_info);
    }

 protected:
    milvus::SchemaPtr schema;
    const char* metricType;
    size_t data_n = 10000;
    size_t data_d = 128;
    size_t topk = 10;
    milvus::FieldDataPtr vec_field_data = nullptr;
    milvus::segcore::SegmentSealedUPtr segment = nullptr;
    milvus::FieldId vec_field_id;
    std::shared_ptr<float[]> vec_data;
};

INSTANTIATE_TEST_CASE_P(MetricTypeParameters,
                        BinlogIndexTest,
                        ::testing::Values(knowhere::metric::L2));

TEST_P(BinlogIndexTest, Accuracy) {
    IndexMetaPtr collection_index_meta =
        GetCollectionIndexMeta(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();

    auto& segcore_config = milvus::segcore::SegcoreConfig::default_config();
    segcore_config.set_enable_interim_segment_index(true);
    segcore_config.set_nprobe(32);
    // 1. load field data, and build binlog index for binlog data
    auto field_data_info = FieldDataInfo{
        vec_field_id.get(), data_n, std::vector<FieldDataPtr>{vec_field_data}};
    segment->LoadFieldData(vec_field_id, field_data_info);
    //assert segment has been built binlog index
    EXPECT_TRUE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_FALSE(segment->HasFieldData(vec_field_id));

    // 2. search binlog index
    auto num_queries = 10;
    auto query_ptr = GenRandomFloatVecData(num_queries, data_d);

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(vec_field_id.get());
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(topk);
    query_info->set_round_decimal(3);
    query_info->set_metric_type(metricType);
    query_info->set_search_params(R"({"nprobe": 1024})");
    auto plan_str = plan_node.SerializeAsString();

    auto ph_group_raw =
        CreatePlaceholderGroupFromBlob(num_queries, data_d, query_ptr.get());

    auto plan = milvus::query::CreateSearchPlanByExpr(
        *schema, plan_str.data(), plan_str.size());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    std::vector<const milvus::query::PlaceholderGroup*> ph_group_arr = {
        ph_group.get()};
    auto nlist = segcore_config.get_nlist();
    auto binlog_index_sr = segment->Search(plan.get(), ph_group.get());
    ASSERT_EQ(binlog_index_sr->total_nq_, num_queries);
    EXPECT_EQ(binlog_index_sr->unity_topK_, topk);
    EXPECT_EQ(binlog_index_sr->distances_.size(), num_queries * topk);
    EXPECT_EQ(binlog_index_sr->seg_offsets_.size(), num_queries * topk);

    // 3. update vector index
    {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = DataType::VECTOR_FLOAT;
        create_index_info.metric_type = metricType;
        create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
        create_index_info.index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber();
        auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
            create_index_info, milvus::storage::FileManagerContext());

        auto build_conf =
            knowhere::Json{{knowhere::meta::METRIC_TYPE, metricType},
                           {knowhere::meta::DIM, std::to_string(data_d)},
                           {knowhere::indexparam::NLIST, "1024"}};

        auto database = knowhere::GenDataSet(data_n, data_d, vec_data.get());
        indexing->BuildWithDataset(database, build_conf);

        LoadIndexInfo load_info;
        load_info.field_id = vec_field_id.get();

        load_info.index = std::move(indexing);
        load_info.index_params["metric_type"] = metricType;
        segment->DropFieldData(vec_field_id);
        ASSERT_NO_THROW(segment->LoadIndex(load_info));
        EXPECT_TRUE(segment->HasIndex(vec_field_id));
        EXPECT_EQ(segment->get_row_count(), data_n);
        EXPECT_FALSE(segment->HasFieldData(vec_field_id));
        auto ivf_sr = segment->Search(plan.get(), ph_group.get());
        auto similary = GetKnnSearchRecall(num_queries,
                                           binlog_index_sr->seg_offsets_.data(),
                                           topk,
                                           ivf_sr->seg_offsets_.data(),
                                           topk);
        ASSERT_GT(similary, 0.45);
    }
}

TEST_P(BinlogIndexTest, DisableInterimIndex) {
    IndexMetaPtr collection_index_meta =
        GetCollectionIndexMeta(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();
    SegcoreSetEnableTempSegmentIndex(false);

    auto field_data_info = FieldDataInfo{
        vec_field_id.get(), data_n, std::vector<FieldDataPtr>{vec_field_data}};
    segment->LoadFieldData(vec_field_id, field_data_info);

    EXPECT_FALSE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));
    // load vector index
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = metricType;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());

    auto build_conf =
        knowhere::Json{{knowhere::meta::METRIC_TYPE, metricType},
                       {knowhere::meta::DIM, std::to_string(data_d)},
                       {knowhere::indexparam::NLIST, "1024"}};

    auto database = knowhere::GenDataSet(data_n, data_d, vec_data.get());
    indexing->BuildWithDataset(database, build_conf);

    LoadIndexInfo load_info;
    load_info.field_id = vec_field_id.get();

    load_info.index = std::move(indexing);
    load_info.index_params["metric_type"] = metricType;

    segment->DropFieldData(vec_field_id);
    ASSERT_NO_THROW(segment->LoadIndex(load_info));
    EXPECT_TRUE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_FALSE(segment->HasFieldData(vec_field_id));
}

TEST_P(BinlogIndexTest, LoadBingLogWihIDMAP) {
    IndexMetaPtr collection_index_meta =
        GetCollectionIndexMeta(knowhere::IndexEnum::INDEX_FAISS_IDMAP);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();

    auto field_data_info = FieldDataInfo{
        vec_field_id.get(), data_n, std::vector<FieldDataPtr>{vec_field_data}};
    segment->LoadFieldData(vec_field_id, field_data_info);

    EXPECT_FALSE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));
}

TEST_P(BinlogIndexTest, LoadBinlogWithoutIndexMeta) {
    IndexMetaPtr collection_index_meta =
        GetCollectionIndexMeta(knowhere::IndexEnum::INDEX_FAISS_IDMAP);

    segment = CreateSealedSegment(schema, collection_index_meta);
    SegcoreSetEnableTempSegmentIndex(true);

    auto field_data_info = FieldDataInfo{
        vec_field_id.get(), data_n, std::vector<FieldDataPtr>{vec_field_data}};
    segment->LoadFieldData(vec_field_id, field_data_info);

    EXPECT_FALSE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));
}