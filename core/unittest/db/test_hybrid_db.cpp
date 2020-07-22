// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <fiu-control.h>
#include <fiu-local.h>
#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <random>
#include <thread>

#include "cache/CpuCacheMgr.h"
#include "config/Config.h"
#include "db/Constants.h"
#include "db/DB.h"
#include "db/DBFactory.h"
#include "db/DBImpl.h"
#include "db/IDGenerator.h"
#include "db/meta/MetaConsts.h"
#include "db/utils.h"
#include "utils/CommonUtil.h"

namespace {
static const char* COLLECTION_NAME = "test_hybrid";
static constexpr int64_t COLLECTION_DIM = 128;
static constexpr int64_t SECONDS_EACH_HOUR = 3600;
static constexpr int64_t FIELD_NUM = 3;
static constexpr int64_t NQ = 10;
static constexpr int64_t TOPK = 10;

void
BuildCollectionSchema(milvus::engine::meta::CollectionSchema& collection_schema,
                      milvus::engine::meta::hybrid::FieldsSchema& fields_schema,
                      std::unordered_map<std::string, milvus::engine::meta::hybrid::DataType>& attr_type) {
    collection_schema.dimension_ = COLLECTION_DIM;
    collection_schema.collection_id_ = COLLECTION_NAME;

    std::vector<milvus::engine::meta::hybrid::FieldSchema> fields;
    fields.resize(FIELD_NUM);
    for (uint64_t i = 0; i < FIELD_NUM; ++i) {
        fields[i].collection_id_ = COLLECTION_NAME;
        fields[i].field_name_ = "field_" + std::to_string(i);
    }
    fields[0].field_type_ = (int)milvus::engine::meta::hybrid::DataType::INT32;
    fields[1].field_type_ = (int)milvus::engine::meta::hybrid::DataType::INT64;
    fields[2].field_type_ = (int)milvus::engine::meta::hybrid::DataType::FLOAT;

    for (uint64_t i = 0; i < FIELD_NUM; ++i) {
        attr_type.insert(
            std::make_pair(fields[i].field_name_, (milvus::engine::meta::hybrid::DataType)fields[i].field_type_));
    }

    milvus::engine::meta::hybrid::FieldSchema schema;
    schema.field_name_ = "field_3";
    schema.collection_id_ = COLLECTION_NAME;
    schema.field_type_ = (int)(milvus::engine::meta::hybrid::DataType::FLOAT_VECTOR);
    fields.emplace_back(schema);

    fields_schema.fields_schema_ = fields;
}

void
BuildVectors(uint64_t n, uint64_t batch_index, milvus::engine::VectorsData& vectors) {
    vectors.vector_count_ = n;
    vectors.float_data_.clear();
    vectors.float_data_.resize(n * COLLECTION_DIM);
    float* data = vectors.float_data_.data();
    for (uint64_t i = 0; i < n; i++) {
        for (int64_t j = 0; j < COLLECTION_DIM; j++) data[COLLECTION_DIM * i + j] = drand48();
        data[COLLECTION_DIM * i] += i / 2000.;

        vectors.id_array_.push_back(n * batch_index + i);
    }
}

void
BuildEntity(uint64_t n, uint64_t batch_index, milvus::engine::Entity& entity) {
    milvus::engine::VectorsData vectors;
    vectors.vector_count_ = n;
    vectors.float_data_.clear();
    vectors.float_data_.resize(n * COLLECTION_DIM);
    float* data = vectors.float_data_.data();
    for (uint64_t i = 0; i < n; i++) {
        for (int64_t j = 0; j < COLLECTION_DIM; j++) data[COLLECTION_DIM * i + j] = drand48();
        data[COLLECTION_DIM * i] += i / 2000.;

        vectors.id_array_.push_back(n * batch_index + i);
    }
    entity.vector_data_.insert(std::make_pair("field_3", vectors));
    std::vector<int64_t> value_0;
    std::vector<int64_t> value_1;
    std::vector<double> value_2;
    value_0.resize(n);
    value_1.resize(n);
    value_2.resize(n);

    std::default_random_engine e;
    std::uniform_real_distribution<float> u(0, 1);
    for (uint64_t i = 0; i < n; ++i) {
        value_0[i] = i;
        value_1[i] = i + n;
        value_2[i] = u(e);
    }
    entity.entity_count_ = n;
    size_t attr_size = n * (sizeof(int64_t) + sizeof(double) + sizeof(int64_t));
    std::vector<uint8_t> attr_value(attr_size, 0);
    size_t offset = 0;
    memcpy(attr_value.data(), value_0.data(), n * sizeof(int64_t));
    offset += n * sizeof(int64_t);
    memcpy(attr_value.data() + offset, value_1.data(), n * sizeof(int64_t));
    offset += n * sizeof(int64_t);
    memcpy(attr_value.data() + offset, value_2.data(), n * sizeof(double));

    entity.attr_value_ = attr_value;
}

void
ConstructGeneralQuery(milvus::query::GeneralQueryPtr& general_query, milvus::query::QueryPtr& query_ptr) {
    general_query->bin->relation = milvus::query::QueryRelation::AND;
    general_query->bin->left_query = std::make_shared<milvus::query::GeneralQuery>();
    general_query->bin->right_query = std::make_shared<milvus::query::GeneralQuery>();
    auto left = general_query->bin->left_query;
    auto right = general_query->bin->right_query;
    left->bin->relation = milvus::query::QueryRelation::AND;

    auto term_query = std::make_shared<milvus::query::TermQuery>();
    std::vector<int64_t> field_value = {10, 20, 30, 40, 50};
    std::vector<uint8_t> term_value;
    term_value.resize(5 * sizeof(int64_t));
    memcpy(term_value.data(), field_value.data(), 5 * sizeof(int64_t));
    term_query->field_name = "field_0";
    term_query->field_value = term_value;
    term_query->boost = 1;

    auto range_query = std::make_shared<milvus::query::RangeQuery>();
    range_query->field_name = "field_1";
    std::vector<milvus::query::CompareExpr> compare_expr;
    compare_expr.resize(2);
    compare_expr[0].compare_operator = milvus::query::CompareOperator::GTE;
    compare_expr[0].operand = "1000";
    compare_expr[1].compare_operator = milvus::query::CompareOperator::LTE;
    compare_expr[1].operand = "5000";
    range_query->compare_expr = compare_expr;
    range_query->boost = 2;

    auto vector_query = std::make_shared<milvus::query::VectorQuery>();
    vector_query->field_name = "field_3";
    vector_query->topk = TOPK;
    vector_query->boost = 3;
    milvus::json json_params = {{"nprobe", 10}};
    vector_query->extra_params = json_params;
    milvus::query::VectorRecord record;
    record.float_data.resize(NQ * COLLECTION_DIM);
    float* data = record.float_data.data();
    for (uint64_t i = 0; i < NQ; i++) {
        for (int64_t j = 0; j < COLLECTION_DIM; j++) data[COLLECTION_DIM * i + j] = drand48();
        data[COLLECTION_DIM * i] += i / 2000.;
    }
    vector_query->query_vector = record;

    left->bin->left_query = std::make_shared<milvus::query::GeneralQuery>();
    left->bin->right_query = std::make_shared<milvus::query::GeneralQuery>();
    left->bin->left_query->leaf = std::make_shared<milvus::query::LeafQuery>();
    left->bin->right_query->leaf = std::make_shared<milvus::query::LeafQuery>();
    left->bin->left_query->leaf->term_query = term_query;
    left->bin->right_query->leaf->range_query = range_query;

    right->leaf = std::make_shared<milvus::query::LeafQuery>();

    std::string vector_placeholder = "placeholder_1";

    right->leaf->vector_placeholder = vector_placeholder;
    query_ptr->root = general_query->bin;
    query_ptr->vectors.insert(std::make_pair(vector_placeholder, vector_query));
}

}  // namespace

TEST_F(DBTest, HYBRID_DB_TEST) {
    milvus::engine::meta::CollectionSchema collection_info;
    milvus::engine::meta::hybrid::FieldsSchema fields_info;
    std::unordered_map<std::string, milvus::engine::meta::hybrid::DataType> attr_type;
    BuildCollectionSchema(collection_info, fields_info, attr_type);

    auto stat = db_->CreateHybridCollection(collection_info, fields_info);
    ASSERT_TRUE(stat.ok());
    milvus::engine::meta::CollectionSchema collection_info_get;
    milvus::engine::meta::hybrid::FieldsSchema fields_info_get;
    collection_info_get.collection_id_ = COLLECTION_NAME;
    stat = db_->DescribeHybridCollection(collection_info_get, fields_info_get);
    ASSERT_TRUE(stat.ok());
    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);

    uint64_t qb = 1000;
    milvus::engine::Entity entity;
    BuildEntity(qb, 0, entity);

    std::vector<std::string> field_names = {"field_0", "field_1", "field_2"};

    stat = db_->InsertEntities(COLLECTION_NAME, "", field_names, entity, attr_type);
    ASSERT_TRUE(stat.ok());

    stat = db_->Flush();
    ASSERT_TRUE(stat.ok());

    milvus::engine::CollectionIndex index;
    index.engine_type_ = (int)milvus::engine::EngineType::FAISS_IDMAP;
    index.extra_params_ = {{"nlist", 16384}};

    //    stat = db_->CreateIndex(COLLECTION_NAME, index);
    ASSERT_TRUE(stat.ok());
}

// TEST_F(DBTest, HYBRID_SEARCH_TEST) {
//    milvus::engine::meta::CollectionSchema collection_info;
//    milvus::engine::meta::hybrid::FieldsSchema fields_info;
//    std::unordered_map<std::string, milvus::engine::meta::hybrid::DataType> attr_type;
//    BuildCollectionSchema(collection_info, fields_info, attr_type);
//
//    auto stat = db_->CreateHybridCollection(collection_info, fields_info);
//    ASSERT_TRUE(stat.ok());
//    milvus::engine::meta::CollectionSchema collection_info_get;
//    milvus::engine::meta::hybrid::FieldsSchema fields_info_get;
//    collection_info_get.collection_id_ = COLLECTION_NAME;
//    stat = db_->DescribeHybridCollection(collection_info_get, fields_info_get);
//    ASSERT_TRUE(stat.ok());
//    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);
//
//    uint64_t qb = 1000;
//    milvus::engine::Entity entity;
//    BuildEntity(qb, 0, entity);
//
//    std::vector<std::string> field_names = {"field_0", "field_1", "field_2"};
//
//    stat = db_->InsertEntities(COLLECTION_NAME, "", field_names, entity, attr_type);
//    ASSERT_TRUE(stat.ok());
//
//    stat = db_->Flush(COLLECTION_NAME);
//    ASSERT_TRUE(stat.ok());
//
//    // Construct general query
//    auto general_query = std::make_shared<milvus::query::GeneralQuery>();
//    auto query_ptr = std::make_shared<milvus::query::Query>();
//    ConstructGeneralQuery(general_query, query_ptr);
//
//    std::vector<std::string> tags;
//    milvus::engine::QueryResult result;
//    stat = db_->HybridQuery(dummy_context_, COLLECTION_NAME, tags, general_query, query_ptr, field_names, attr_type,
//                            result);
//    ASSERT_TRUE(stat.ok());
//    ASSERT_EQ(result.row_num_, NQ);
//    ASSERT_EQ(result.result_ids_.size(), NQ * TOPK);
//}
//
// TEST_F(DBTest, COMPACT_TEST) {
//    milvus::engine::meta::CollectionSchema collection_info;
//    milvus::engine::meta::hybrid::FieldsSchema fields_info;
//    std::unordered_map<std::string, milvus::engine::meta::hybrid::DataType> attr_type;
//    BuildCollectionSchema(collection_info, fields_info, attr_type);
//
//    auto stat = db_->CreateHybridCollection(collection_info, fields_info);
//    ASSERT_TRUE(stat.ok());
//    milvus::engine::meta::CollectionSchema collection_info_get;
//    milvus::engine::meta::hybrid::FieldsSchema fields_info_get;
//    collection_info_get.collection_id_ = COLLECTION_NAME;
//    stat = db_->DescribeHybridCollection(collection_info_get, fields_info_get);
//    ASSERT_TRUE(stat.ok());
//    ASSERT_EQ(collection_info_get.dimension_, COLLECTION_DIM);
//
//    uint64_t vector_count = 1000;
//    milvus::engine::Entity entity;
//    BuildEntity(vector_count, 0, entity);
//
//    std::vector<std::string> field_names = {"field_0", "field_1", "field_2"};
//
//    stat = db_->InsertEntities(COLLECTION_NAME, "", field_names, entity, attr_type);
//    ASSERT_TRUE(stat.ok());
//
//    stat = db_->Flush();
//    ASSERT_TRUE(stat.ok());
//
//    std::vector<milvus::engine::IDNumber> ids_to_delete;
//    ids_to_delete.emplace_back(entity.id_array_.front());
//    ids_to_delete.emplace_back(entity.id_array_.back());
//    stat = db_->DeleteVectors(collection_info.collection_id_, ids_to_delete);
//    ASSERT_TRUE(stat.ok());
//
//    stat = db_->Flush();
//    ASSERT_TRUE(stat.ok());
//
//    stat = db_->Compact(dummy_context_, collection_info.collection_id_);
//    ASSERT_TRUE(stat.ok());
//
//    const int topk = 1, nprobe = 1;
//    milvus::json json_params = {{"nprobe", nprobe}};
//
//    std::vector<std::string> tags;
//    milvus::engine::ResultIds result_ids;
//    milvus::engine::ResultDistances result_distances;
//
//    stat = db_->QueryByIDs(dummy_context_, collection_info.collection_id_, tags, topk, json_params, ids_to_delete,
//                           result_ids, result_distances);
//    ASSERT_TRUE(stat.ok());
//    ASSERT_EQ(result_ids[0], -1);
//    ASSERT_EQ(result_distances[0], std::numeric_limits<float>::max());
//}
//
// TEST_F(DBTest2, GET_ENTITY_BY_ID_TEST) {
//    milvus::engine::meta::CollectionSchema collection_schema;
//    milvus::engine::meta::hybrid::FieldsSchema fields_schema;
//    std::unordered_map<std::string, milvus::engine::meta::hybrid::DataType> attr_type;
//    BuildCollectionSchema(collection_schema, fields_schema, attr_type);
//
//    auto stat = db_->CreateHybridCollection(collection_schema, fields_schema);
//    ASSERT_TRUE(stat.ok());
//
//    uint64_t vector_count = 1000;
//    milvus::engine::Entity entity;
//    BuildEntity(vector_count, 0, entity);
//
//    std::vector<std::string> field_names = {"field_0", "field_1", "field_2"};
//
//    stat = db_->InsertEntities(COLLECTION_NAME, "", field_names, entity, attr_type);
//    ASSERT_TRUE(stat.ok());
//
//    stat = db_->Flush();
//    ASSERT_TRUE(stat.ok());
//
//    std::vector<milvus::engine::AttrsData> attrs;
//    std::vector<milvus::engine::VectorsData> vectors;
//    stat = db_->GetEntitiesByID(COLLECTION_NAME, entity.id_array_, field_names, vectors, attrs);
//    ASSERT_TRUE(stat.ok());
//    ASSERT_EQ(vectors.size(), entity.id_array_.size());
//    ASSERT_EQ(vectors[0].float_data_.size(), COLLECTION_DIM);
//    ASSERT_EQ(attrs[0].attr_data_.at("field_0").size(), sizeof(int32_t));
//
//    for (int64_t i = 0; i < COLLECTION_DIM; i++) {
//        ASSERT_FLOAT_EQ(vectors[0].float_data_[i], entity.vector_data_.at("field_3").float_data_[i]);
//    }
//
//    std::vector<int64_t> empty_array;
//    vectors.clear();
//    attrs.clear();
//    field_names.clear();
//    stat = db_->GetEntitiesByID(COLLECTION_NAME, empty_array, field_names, vectors, attrs);
//    ASSERT_TRUE(stat.ok());
//    for (auto& vector : vectors) {
//        ASSERT_EQ(vector.vector_count_, 0);
//        ASSERT_TRUE(vector.float_data_.empty());
//        ASSERT_TRUE(vector.binary_data_.empty());
//    }
//}
