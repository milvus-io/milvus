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
#include <fiu/fiu-local.h>
#include <gtest/gtest.h>

#include <src/cache/CpuCacheMgr.h>
#include <algorithm>
#include <experimental/filesystem>
#include <set>
#include <string>

#include "db/SnapshotUtils.h"
#include "db/SnapshotVisitor.h"
#include "db/merge/MergeAdaptiveStrategy.h"
#include "db/merge/MergeLayerStrategy.h"
#include "db/merge/MergeSimpleStrategy.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/InActiveResourcesGCEvent.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/utils.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "segment/Segment.h"

using SegmentVisitor = milvus::engine::SegmentVisitor;
using InActiveResourcesGCEvent = milvus::engine::snapshot::InActiveResourcesGCEvent;

namespace {
const char* VECTOR_FIELD_NAME = "vector";

milvus::Status
CreateCollection(const std::shared_ptr<DB>& db, const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
    context.collection = collection_schema;
    auto vector_field = std::make_shared<Field>(VECTOR_FIELD_NAME, 0, milvus::engine::DataType::VECTOR_FLOAT);
    auto vector_field_element =
        std::make_shared<FieldElement>(0, 0, "ivfsq8", milvus::engine::FieldElementType::FET_INDEX);
    auto int_field = std::make_shared<Field>("int", 0, milvus::engine::DataType::INT32);
    context.fields_schema[vector_field] = {vector_field_element};
    context.fields_schema[int_field] = {};

    return db->CreateCollection(context);
}

static constexpr int64_t COLLECTION_DIM = 10;

milvus::Status
CreateCollection2(std::shared_ptr<DB> db, const std::string& collection_name, bool auto_genid = true) {
    CreateCollectionContext context;
    milvus::json collection_params;
    collection_params[milvus::engine::PARAM_UID_AUTOGEN] = auto_genid;

    auto collection_schema = std::make_shared<Collection>(collection_name, collection_params);
    context.collection = collection_schema;

    milvus::json params;
    params[milvus::knowhere::meta::DIM] = COLLECTION_DIM;
    auto vector_field = std::make_shared<Field>(VECTOR_FIELD_NAME, 0, milvus::engine::DataType::VECTOR_FLOAT, params);
    context.fields_schema[vector_field] = {};

    std::unordered_map<std::string, milvus::engine::DataType> attr_type = {
        {"field_0", milvus::engine::DataType::INT32},
        {"field_1", milvus::engine::DataType::INT64},
        {"field_2", milvus::engine::DataType::DOUBLE},
    };

    std::vector<std::string> field_names;
    for (auto& pair : attr_type) {
        auto field = std::make_shared<Field>(pair.first, 0, pair.second);
        context.fields_schema[field] = {};
        field_names.push_back(pair.first);
    }

    return db->CreateCollection(context);
}

milvus::Status
CreateCollection3(std::shared_ptr<DB> db, const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
    context.collection = collection_schema;

    milvus::json params;
    params[milvus::knowhere::meta::DIM] = COLLECTION_DIM;
    auto vector_field = std::make_shared<Field>("float_vector", 0, milvus::engine::DataType::VECTOR_FLOAT, params);
    context.fields_schema[vector_field] = {};

    std::unordered_map<std::string, milvus::engine::DataType> attr_type = {
        {"int64", milvus::engine::DataType::INT64},
    };

    std::vector<std::string> field_names;
    for (auto& pair : attr_type) {
        auto field = std::make_shared<Field>(pair.first, 0, pair.second);
        context.fields_schema[field] = {};
        field_names.push_back(pair.first);
    }

    return db->CreateCollection(context);
}

void
BuildEntities(uint64_t n, uint64_t batch_index, milvus::engine::DataChunkPtr& data_chunk, bool gen_id = false) {
    data_chunk = std::make_shared<milvus::engine::DataChunk>();
    data_chunk->count_ = n;

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

    if (gen_id) {
        milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
        raw->data_.resize(vectors.id_array_.size() * sizeof(int64_t));
        memcpy(raw->data_.data(), vectors.id_array_.data(), vectors.id_array_.size() * sizeof(int64_t));
        data_chunk->fixed_fields_[milvus::engine::FIELD_UID] = raw;
    }

    {
        milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
        raw->data_.resize(vectors.float_data_.size() * sizeof(float));
        memcpy(raw->data_.data(), vectors.float_data_.data(), vectors.float_data_.size() * sizeof(float));
        data_chunk->fixed_fields_[VECTOR_FIELD_NAME] = raw;
    }

    std::vector<int32_t> value_0;
    std::vector<int64_t> value_1;
    std::vector<double> value_2;
    value_0.resize(n);
    value_1.resize(n);
    value_2.resize(n);

    std::default_random_engine e;
    std::uniform_real_distribution<double> u(0, 1);
    for (uint64_t i = 0; i < n; ++i) {
        value_0[i] = i;
        value_1[i] = i + n;
        value_2[i] = u(e);
    }

    {
        milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
        raw->data_.resize(value_0.size() * sizeof(int32_t));
        memcpy(raw->data_.data(), value_0.data(), value_0.size() * sizeof(int32_t));
        data_chunk->fixed_fields_["field_0"] = raw;
    }

    {
        milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
        raw->data_.resize(value_1.size() * sizeof(int64_t));
        memcpy(raw->data_.data(), value_1.data(), value_1.size() * sizeof(int64_t));
        data_chunk->fixed_fields_["field_1"] = raw;
    }

    {
        milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
        raw->data_.resize(value_2.size() * sizeof(double));
        memcpy(raw->data_.data(), value_2.data(), value_2.size() * sizeof(double));
        data_chunk->fixed_fields_["field_2"] = raw;
    }
}

void
CopyChunkData(const milvus::engine::DataChunkPtr& src_chunk, milvus::engine::DataChunkPtr& target_chunk) {
    target_chunk = std::make_shared<milvus::engine::DataChunk>();
    target_chunk->count_ = src_chunk->count_;
    for (auto& pair : src_chunk->fixed_fields_) {
        milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
        raw->data_ = pair.second->data_;
        target_chunk->fixed_fields_.insert(std::make_pair(pair.first, raw));
    }
    for (auto& pair : src_chunk->variable_fields_) {
        milvus::engine::VaribleDataPtr raw = std::make_shared<milvus::engine::VaribleData>();
        raw->data_ = pair.second->data_;
        target_chunk->variable_fields_.insert(std::make_pair(pair.first, raw));
    }
}

void
BuildQueryPtr(const std::string& collection_name, int64_t n, int64_t topk, std::vector<std::string>& field_names,
              std::vector<std::string>& partitions, milvus::query::QueryPtr& query_ptr) {
    auto general_query = std::make_shared<milvus::query::GeneralQuery>();
    query_ptr->collection_id = collection_name;
    query_ptr->field_names = field_names;
    query_ptr->partitions = partitions;
    std::set<std::string> index_fields = {"int64", "float_vector"};
    query_ptr->index_fields = index_fields;

    auto left_query = std::make_shared<milvus::query::GeneralQuery>();
    auto term_query = std::make_shared<milvus::query::TermQuery>();
    std::vector<int32_t> term_value(n, 0);
    for (uint64_t i = 0; i < n; i++) {
        term_value[i] = i;
    }
    term_query->json_obj = {{"int64", {{"values", term_value}}}};
    std::cout << term_query->json_obj.dump() << std::endl;
    left_query->leaf = std::make_shared<milvus::query::LeafQuery>();
    left_query->leaf->term_query = term_query;
    general_query->bin->left_query = left_query;

    auto right_query = std::make_shared<milvus::query::GeneralQuery>();
    right_query->leaf = std::make_shared<milvus::query::LeafQuery>();
    std::string placeholder = "placeholder_1";
    right_query->leaf->vector_placeholder = placeholder;
    general_query->bin->right_query = right_query;

    auto vector_query = std::make_shared<milvus::query::VectorQuery>();
    vector_query->field_name = "float_vector";
    vector_query->topk = topk;
    milvus::query::VectorRecord vector_record;
    vector_record.float_data.resize(n * COLLECTION_DIM);
    for (uint64_t i = 0; i < n; i++) {
        for (int64_t j = 0; j < COLLECTION_DIM; j++) vector_record.float_data[COLLECTION_DIM * i + j] = drand48();
        vector_record.float_data[COLLECTION_DIM * i] += i / 2000.;
    }
    vector_query->query_vector = vector_record;
    vector_query->metric_type = "L2";
    vector_query->extra_params = {{"nprobe", 1024}};

    query_ptr->root = general_query;
    query_ptr->vectors.insert(std::make_pair(placeholder, vector_query));
    query_ptr->metric_types.insert({"float_vector", "L2"});
    general_query->bin->relation = milvus::query::QueryRelation::AND;
}

void
BuildEntities2(uint64_t n, uint64_t batch_index, milvus::engine::DataChunkPtr& data_chunk) {
    data_chunk = std::make_shared<milvus::engine::DataChunk>();
    data_chunk->count_ = n;

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

    milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
    data_chunk->fixed_fields_["float_vector"] = raw;
    raw->data_.resize(vectors.float_data_.size() * sizeof(float));
    memcpy(raw->data_.data(), vectors.float_data_.data(), vectors.float_data_.size() * sizeof(float));

    std::vector<int64_t> value_1;
    value_1.resize(n);

    for (uint64_t i = 0; i < n; ++i) {
        value_1[i] = i;
    }

    {
        milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
        data_chunk->fixed_fields_["int64"] = raw;
        raw->data_.resize(value_1.size() * sizeof(int64_t));
        memcpy(raw->data_.data(), value_1.data(), value_1.size() * sizeof(int64_t));
    }
}
}  // namespace

TEST_F(DBTest, CollectionTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) { return ++lsn; };
    std::string c1 = "c1";
    auto status = CreateCollection(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok()) << status.ToString();

    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(ss);
    ASSERT_EQ(ss->GetName(), c1);

    bool has;
    status = db_->HasCollection(c1, has);
    ASSERT_TRUE(has);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(ss->GetCollectionCommit()->GetRowCount(), 0);
    int64_t row_cnt = 0;
    status = db_->CountEntities(c1, row_cnt);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(row_cnt, 0);

    std::vector<std::string> names;
    status = db_->ListCollections(names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(names.size(), 1);
    ASSERT_EQ(names[0], c1);

    std::string c1_1 = "c1";
    status = CreateCollection(db_, c1_1, next_lsn());
    ASSERT_FALSE(status.ok());

    std::string c2 = "c2";
    status = CreateCollection(db_, c2, next_lsn());
    ASSERT_TRUE(status.ok());

    status = db_->ListCollections(names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(names.size(), 2);

    status = db_->DropCollection(c1);
    ASSERT_TRUE(status.ok());

    status = db_->ListCollections(names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(names.size(), 1);
    ASSERT_EQ(names[0], c2);

    status = db_->DropCollection(c1);
    ASSERT_FALSE(status.ok());
}

TEST_F(DBTest, PartitionTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) { return ++lsn; };
    std::string c1 = "c1";
    auto status = CreateCollection(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok());

    std::vector<std::string> partition_names;
    status = db_->ListPartitions(c1, partition_names);
    ASSERT_EQ(partition_names.size(), 1);
    ASSERT_EQ(partition_names[0], "_default");

    std::string p1 = "p1";
    std::string c2 = "c2";
    status = db_->CreatePartition(c2, p1);
    ASSERT_FALSE(status.ok());

    status = db_->CreatePartition(c1, p1);
    ASSERT_TRUE(status.ok());

    status = db_->ListPartitions(c1, partition_names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(partition_names.size(), 2);

    status = db_->CreatePartition(c1, p1);
    ASSERT_FALSE(status.ok());

    status = db_->DropPartition(c1, "p3");
    ASSERT_FALSE(status.ok());

    status = db_->DropPartition(c1, p1);
    ASSERT_TRUE(status.ok());
    status = db_->ListPartitions(c1, partition_names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(partition_names.size(), 1);
}

TEST_F(DBTest, VisitorTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) { return ++lsn; };

    std::string c1 = "c1";
    auto status = CreateCollection(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok());

    std::stringstream p_name;
    auto num = RandomInt(1, 3);
    for (auto i = 0; i < num; ++i) {
        p_name.str("");
        p_name << "partition_" << i;
        status = db_->CreatePartition(c1, p_name.str());
        ASSERT_TRUE(status.ok());
    }

    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());

    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    auto new_total = 0;
    auto& partitions = ss->GetResources<Partition>();
    ID_TYPE partition_id;
    for (auto& kv : partitions) {
        num = RandomInt(1, 3);
        auto row_cnt = 100;
        for (auto i = 0; i < num; ++i) {
            ASSERT_TRUE(CreateSegment(ss, kv.first, next_lsn(), sf_context, row_cnt).ok());
        }
        new_total += num;
        partition_id = kv.first;
    }

    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());

    auto executor = [&](const Segment::Ptr& segment, SegmentIterator* handler) -> Status {
        auto visitor = SegmentVisitor::Build(ss, segment->GetID());
        if (!visitor) {
            return Status(milvus::SS_ERROR, "Cannot build segment visitor");
        }
        std::cout << visitor->ToString() << std::endl;
        return Status::OK();
    };

    auto segment_handler = std::make_shared<SegmentIterator>(ss, executor);
    segment_handler->Iterate();
    std::cout << segment_handler->GetStatus().ToString() << std::endl;
    ASSERT_TRUE(segment_handler->GetStatus().ok());

    auto row_cnt = ss->GetCollectionCommit()->GetRowCount();
    auto new_segment_row_cnt = 1024;
    {
        OperationContext context;
        context.lsn = next_lsn();
        context.prev_partition = ss->GetResource<Partition>(partition_id);
        auto op = std::make_shared<NewSegmentOperation>(context, ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        SegmentFilePtr seg_file;
        auto nsf_context = sf_context;
        nsf_context.segment_id = new_seg->GetID();
        nsf_context.partition_id = new_seg->GetPartitionId();
        status = op->CommitNewSegmentFile(nsf_context, seg_file);
        ASSERT_TRUE(status.ok());
        auto ctx = op->GetContext();
        ASSERT_TRUE(ctx.new_segment);
        auto visitor = SegmentVisitor::Build(ss, ctx.new_segment, ctx.new_segment_files);
        ASSERT_TRUE(visitor);
        ASSERT_EQ(visitor->GetSegment(), new_seg);
        ASSERT_FALSE(visitor->GetSegment()->IsActive());

        int file_num = 0;
        auto field_visitors = visitor->GetFieldVisitors();
        for (auto& kv : field_visitors) {
            auto& field_visitor = kv.second;
            auto field_element_visitors = field_visitor->GetElementVistors();
            for (auto& kkvv : field_element_visitors) {
                auto& field_element_visitor = kkvv.second;
                auto file = field_element_visitor->GetFile();
                if (file) {
                    file_num++;
                    ASSERT_FALSE(file->IsActive());
                }
            }
        }
        ASSERT_EQ(file_num, 1);

        std::cout << visitor->ToString() << std::endl;
        status = op->CommitRowCount(new_segment_row_cnt);
        status = op->Push();
        ASSERT_TRUE(status.ok());
    }
    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(ss->GetCollectionCommit()->GetRowCount(), row_cnt + new_segment_row_cnt);
    std::cout << ss->ToString() << std::endl;
}

TEST_F(DBTest, QueryTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) { return ++lsn; };

    std::string c1 = "c1";
    auto status = CreateCollection3(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok());

    const uint64_t entity_count = 10000;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities2(entity_count, 0, data_chunk);

    status = db_->Insert(c1, "", data_chunk);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    milvus::server::ContextPtr ctx1;
    milvus::query::QueryPtr query_ptr = std::make_shared<milvus::query::Query>();
    milvus::engine::QueryResultPtr result = std::make_shared<milvus::engine::QueryResult>();

    std::vector<std::string> field_names;
    std::vector<std::string> partitions;
    int64_t nq = 5;
    int64_t topk = 10;
    BuildQueryPtr(c1, nq, topk, field_names, partitions, query_ptr);
    status = db_->Query(ctx1, query_ptr, result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result->row_num_, nq);
}

TEST_F(DBTest, InsertTest) {
    auto do_insert = [&](bool autogen_id, bool provide_id) -> void {
        CreateCollectionContext context;
        context.lsn = 0;
        std::string collection_name = "INSERT_TEST";
        auto collection_schema = std::make_shared<Collection>(collection_name);
        milvus::json params;
        params[milvus::engine::PARAM_UID_AUTOGEN] = autogen_id;
        collection_schema->SetParams(params);
        context.collection = collection_schema;

        std::string field_name = "field_0";
        auto field = std::make_shared<Field>(field_name, 0, milvus::engine::DataType::INT32);
        context.fields_schema[field] = {};

        field = std::make_shared<Field>(milvus::engine::FIELD_UID, 0, milvus::engine::DataType::INT64);
        context.fields_schema[field] = {};

        auto status = db_->CreateCollection(context);

        milvus::engine::DataChunkPtr data_chunk = std::make_shared<milvus::engine::DataChunk>();
        data_chunk->count_ = 100;
        if (provide_id) {
            milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
            raw->data_.resize(100 * sizeof(int64_t));
            int64_t* p = (int64_t*)raw->data_.data();
            for (int64_t i = 0; i < data_chunk->count_; ++i) {
                p[i] = i;
            }
            data_chunk->fixed_fields_[milvus::engine::FIELD_UID] = raw;
        }
        {
            milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
            raw->data_.resize(100 * sizeof(int32_t));
            int32_t* p = (int32_t*)raw->data_.data();
            for (int64_t i = 0; i < data_chunk->count_; ++i) {
                p[i] = i + 5000;
            }
            data_chunk->fixed_fields_[field_name] = raw;
        }

        status = db_->Insert(collection_name, "", data_chunk);
        if (autogen_id == provide_id) {
            ASSERT_FALSE(status.ok());
        } else {
            ASSERT_TRUE(status.ok());
        }

        status = db_->Flush();
        ASSERT_TRUE(status.ok());

        int64_t row_count = 0;
        status = db_->CountEntities(collection_name, row_count);
        ASSERT_TRUE(status.ok());
        if (autogen_id == provide_id) {
            ASSERT_EQ(row_count, 0);
        } else {
            ASSERT_EQ(row_count, data_chunk->count_);
        }

        status = db_->DropCollection(collection_name);
        ASSERT_TRUE(status.ok());
    };

    // create collection with auto-generate id, insert entities with user id, insert action failed
    do_insert(true, true);

    // create collection with auto-generate id, insert entities without user id, insert action succeed
    do_insert(true, false);

    // create collection without auto-generate id, insert entities with user id, insert action succeed
    do_insert(false, true);

    // create collection without auto-generate id, insert entities without user id, insert action failed
    do_insert(false, false);
}

TEST(MergeTest, MergeStrategyTest) {
    milvus::engine::Partition2SegmentsMap part2segments;
    milvus::engine::SegmentInfoList segmet_list_1 = {
        milvus::engine::SegmentInfo(1, 100, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(2, 2500, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(3, 300, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(4, 5, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(5, 60000, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(6, 99999, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(7, 60, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(8, 800, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(9, 6600, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(10, 110000, milvus::engine::utils::GetMicroSecTimeStamp()),
    };
    milvus::engine::SegmentInfoList segmet_list_2 = {
        milvus::engine::SegmentInfo(11, 9000, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(12, 1500, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(13, 20, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(14, 1, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(15, 100001, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(16, 15000, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(17, 0, milvus::engine::utils::GetMicroSecTimeStamp()),
    };
    milvus::engine::SegmentInfoList segmet_list_3 = {
        milvus::engine::SegmentInfo(21, 19000, milvus::engine::utils::GetMicroSecTimeStamp()),
        milvus::engine::SegmentInfo(22, 40500, milvus::engine::utils::GetMicroSecTimeStamp() - 2000000),
        milvus::engine::SegmentInfo(23, 1000, milvus::engine::utils::GetMicroSecTimeStamp() - 1000000),
    };
    part2segments.insert(std::make_pair(1, segmet_list_1));
    part2segments.insert(std::make_pair(2, segmet_list_2));
    part2segments.insert(std::make_pair(3, segmet_list_3));

    int64_t row_per_segment = 100000;
    {
        milvus::engine::SegmentGroups groups;
        milvus::engine::MergeAdaptiveStrategy strategy;
        auto status = strategy.RegroupSegments(part2segments, row_per_segment, groups);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(groups.size(), 4);
        std::set<size_t> compare = {3, 2, 7, 5};
        std::set<size_t> result;
        for (auto& group : groups) {
            result.insert(group.size());
        }
        ASSERT_EQ(compare, result);
    }

    {
        milvus::engine::SegmentGroups groups;
        milvus::engine::MergeSimpleStrategy strategy;
        auto status = strategy.RegroupSegments(part2segments, row_per_segment, groups);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(groups.size(), 4);
        std::set<size_t> compare = {3, 3, 5, 6};
        std::set<size_t> result;
        for (auto& group : groups) {
            result.insert(group.size());
        }
        ASSERT_EQ(compare, result);
    }

    {
        milvus::engine::SegmentGroups groups;
        milvus::engine::MergeLayerStrategy strategy;
        auto status = strategy.RegroupSegments(part2segments, row_per_segment, groups);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(groups.size(), 4);
        std::set<size_t> compare = {2, 3, 3, 6};
        std::set<size_t> result;
        for (auto& group : groups) {
            result.insert(group.size());
        }
        ASSERT_EQ(compare, result);
    }
}

TEST_F(DBTest, MergeTest) {
    std::string collection_name = "MERGE_TEST";
    auto status = CreateCollection2(db_, collection_name);
    ASSERT_TRUE(status.ok());

    const uint64_t entity_count = 100;
    milvus::engine::DataChunkPtr data_chunk;

    // insert entities into collection multiple times
    int64_t repeat = 2;
    for (int32_t i = 0; i < repeat; i++) {
        BuildEntities(entity_count, 0, data_chunk);
        status = db_->Insert(collection_name, "", data_chunk);
        ASSERT_TRUE(status.ok());

        status = db_->Flush();
        ASSERT_TRUE(status.ok());
    }

    // wait to merge finished
    sleep(2);
    /* STATUS_CHECK((*op)(store)); */
    /* auto event = std::make_shared<InActiveResourcesGCEvent>(); */
    /* milvus::engine::snapshot::EventExecutor::GetInstance().Submit(event, true); */
    /* event->WaitToFinish(); */

    // validate entities count
    int64_t row_count = 0;
    status = db_->CountEntities(collection_name, row_count);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(row_count, entity_count * repeat);

    // validate segment files count is correct
    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    ASSERT_TRUE(status.ok());
    std::cout << ss->ToString() << std::endl;

    auto root_path = GetOptions().meta_.path_ + milvus::engine::COLLECTIONS_FOLDER;
    std::vector<std::string> segment_paths;

    auto seg_executor = [&](const SegmentPtr& segment, SegmentIterator* handler) -> Status {
        std::string res_path = milvus::engine::snapshot::GetResPath<Segment>(root_path, segment);
        std::cout << res_path << std::endl;
        if (!std::experimental::filesystem::is_directory(res_path)) {
            return Status(milvus::SS_ERROR, res_path + " not exist");
        }
        segment_paths.push_back(res_path);
        return Status::OK();
    };
    auto segment_iter = std::make_shared<SegmentIterator>(ss, seg_executor);
    segment_iter->Iterate();
    status = segment_iter->GetStatus();
    ASSERT_TRUE(status.ok()) << status.ToString();

    std::set<std::string> segment_file_paths;
    auto sf_executor = [&](const SegmentFilePtr& segment_file, SegmentFileIterator* handler) -> Status {
        std::string res_path = milvus::engine::snapshot::GetResPath<SegmentFile>(root_path, segment_file);
        if (std::experimental::filesystem::is_regular_file(res_path) ||
            std::experimental::filesystem::is_regular_file(res_path +
                                                           milvus::codec::IdBloomFilterFormat::FilePostfix()) ||
            std::experimental::filesystem::is_regular_file(res_path +
                                                           milvus::codec::DeletedDocsFormat::FilePostfix())) {
            segment_file_paths.insert(res_path);
            std::cout << res_path << std::endl;
        }
        return Status::OK();
    };
    auto sf_iterator = std::make_shared<SegmentFileIterator>(ss, sf_executor);
    sf_iterator->Iterate();

    std::set<std::string> expect_file_paths;
    std::experimental::filesystem::recursive_directory_iterator iter(root_path);
    std::experimental::filesystem::recursive_directory_iterator end;
    std::cout << "==============" << std::endl;
    for (; iter != end; ++iter) {
        if (std::experimental::filesystem::is_regular_file((*iter).path())) {
            auto path = (*iter).path().filename().string();
            std::cout << path << std::endl;
            expect_file_paths.insert(path);
        }
    }

    // PXU TODO: Need to be turn-on later after GC changes
    /* ASSERT_EQ(expect_file_paths.size(), segment_file_paths.size() + 1); */
}

TEST_F(DBTest, GetEntityTest) {
    auto insert_entities = [&](const std::string& collection, const std::string& partition, uint64_t count,
                               uint64_t batch_index, milvus::engine::IDNumbers& ids,
                               milvus::engine::DataChunkPtr& data_chunk) -> Status {
        milvus::engine::DataChunkPtr consume_chunk;
        BuildEntities(count, batch_index, consume_chunk);
        CopyChunkData(consume_chunk, data_chunk);

        // Note: consume_chunk is consumed by insert()
        STATUS_CHECK(db_->Insert(collection, partition, consume_chunk));
        STATUS_CHECK(db_->Flush(collection));
        auto iter = consume_chunk->fixed_fields_.find(milvus::engine::FIELD_UID);
        if (iter == consume_chunk->fixed_fields_.end()) {
            return Status(1, "Cannot find uid field");
        }
        auto& ids_buffer = iter->second;
        ids.resize(consume_chunk->count_);
        memcpy(ids.data(), ids_buffer->data_.data(), ids_buffer->Size());

        milvus::engine::BinaryDataPtr raw = std::make_shared<milvus::engine::BinaryData>();
        raw->data_ = ids_buffer->data_;
        data_chunk->fixed_fields_[milvus::engine::FIELD_UID] = raw;

        return Status::OK();
    };

    auto fill_field_names = [&](const milvus::engine::snapshot::FieldElementMappings& field_mappings,
                                std::vector<std::string>& field_names) -> void {
        if (field_names.empty()) {
            for (const auto& schema : field_mappings) {
                field_names.emplace_back(schema.first->GetName());
            }
        } else {
            for (const auto& name : field_names) {
                bool find_field_name = false;
                for (const auto& kv : field_mappings) {
                    if (name == kv.first->GetName()) {
                        find_field_name = true;
                        break;
                    }
                }
                if (not find_field_name) {
                    return;
                }
            }
        }
    };

    auto get_row_size = [&](const std::vector<bool>& valid_row) -> int {
        int valid_row_size = 0;
        for (auto valid : valid_row) {
            if (valid)
                valid_row_size++;
        }
        return valid_row_size;
    };

    std::string collection_name = "GET_ENTITY_TEST";
    auto status = CreateCollection2(db_, collection_name);
    ASSERT_TRUE(status.ok()) << status.ToString();

    milvus::engine::IDNumbers entity_ids;
    milvus::engine::DataChunkPtr dataChunkPtr;
    insert_entities(collection_name, "", 2000, 0, entity_ids, dataChunkPtr);
    std::cout << "Post InsertEntities" << std::endl;
    ASSERT_TRUE(status.ok()) << status.ToString();

    milvus::engine::snapshot::CollectionPtr collection;
    milvus::engine::snapshot::FieldElementMappings field_mappings;
    status = db_->GetCollectionInfo(collection_name, collection, field_mappings);
    ASSERT_TRUE(status.ok()) << status.ToString();
    std::cout << "Post GetCollectionInfo" << std::endl;

    {
        std::vector<std::string> field_names;
        fill_field_names(field_mappings, field_names);
        milvus::engine::DataChunkPtr get_data_chunk;
        std::vector<bool> valid_row;

        status = db_->GetEntityByID(collection_name, entity_ids, field_names, valid_row, get_data_chunk);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_TRUE(get_data_chunk->count_ == get_row_size(valid_row));

        for (const auto& name : field_names) {
            ASSERT_TRUE(get_data_chunk->fixed_fields_[name]->Size() == dataChunkPtr->fixed_fields_[name]->Size());
            ASSERT_TRUE(get_data_chunk->fixed_fields_[name]->data_ == dataChunkPtr->fixed_fields_[name]->data_);
        }
    }
    std::cout << "Post GetEntityByID1" << std::endl;

    {
        std::vector<std::string> field_names;
        fill_field_names(field_mappings, field_names);
        milvus::engine::DataChunkPtr get_data_chunk;
        std::vector<bool> valid_row;
        field_names.emplace_back("Hello World");
        field_names.emplace_back("GoodBye World");

        status = db_->GetEntityByID(collection_name, entity_ids, field_names, valid_row, get_data_chunk);
        ASSERT_TRUE(!status.ok());
    }
    std::cout << "Post GetEntityByID2" << std::endl;

    {
        std::vector<std::string> field_names;
        fill_field_names(field_mappings, field_names);
        milvus::engine::DataChunkPtr get_data_chunk;
        std::vector<bool> valid_row;
        std::reverse(entity_ids.begin(), entity_ids.end());
        entity_ids.push_back(-1);
        entity_ids.push_back(-2);
        std::reverse(entity_ids.begin(), entity_ids.end());

        status = db_->GetEntityByID(collection_name, entity_ids, field_names, valid_row, get_data_chunk);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_TRUE(get_data_chunk->count_ == get_row_size(valid_row));

        for (const auto& name : field_names) {
            ASSERT_TRUE(get_data_chunk->fixed_fields_[name]->Size() == dataChunkPtr->fixed_fields_[name]->Size());
            ASSERT_TRUE(get_data_chunk->fixed_fields_[name]->data_ == dataChunkPtr->fixed_fields_[name]->data_);
        }
    }
    std::cout << "Post GetEntityByID3" << std::endl;
}

TEST_F(DBTest, CompactTest) {
    std::string collection_name = "COMPACT_TEST";
    auto status = CreateCollection2(db_, collection_name);
    ASSERT_TRUE(status.ok());

    // insert 1000 entities into default partition
    const uint64_t entity_count = 100;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(entity_count, 0, data_chunk);

    status = db_->Insert(collection_name, "", data_chunk);
    ASSERT_TRUE(status.ok());

    milvus::engine::IDNumbers batch_entity_ids;
    milvus::engine::utils::GetIDFromChunk(data_chunk, batch_entity_ids);
    ASSERT_EQ(batch_entity_ids.size(), entity_count);

    auto delete_entity = [&](int64_t from, int64_t to) -> void {
        int64_t delete_count = to - from;
        if (delete_count < 0) {
            return;
        }
        std::vector<milvus::engine::idx_t> delete_ids;
        for (auto i = from; i < to; ++i) {
            delete_ids.push_back(batch_entity_ids[i]);
        }
        status = db_->DeleteEntityByID(collection_name, delete_ids);
        ASSERT_TRUE(status.ok());
    };

    // delete entities from 100 to 300
    int64_t delete_count_1 = 20;
    delete_entity(10, 10 + delete_count_1);

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    auto validate_entity_data = [&]() -> void {
        std::vector<std::string> field_names = {"field_0"};
        std::vector<bool> valid_row;
        milvus::engine::DataChunkPtr fetch_chunk;
        status = db_->GetEntityByID(collection_name, batch_entity_ids, field_names, valid_row, fetch_chunk);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(valid_row.size(), batch_entity_ids.size());
        auto& chunk = fetch_chunk->fixed_fields_["field_0"];
        ASSERT_NE(chunk, nullptr);
        int32_t* p = (int32_t*)(chunk->data_.data());
        int64_t index = 0;
        for (uint64_t i = 0; i < valid_row.size(); ++i) {
            if (!valid_row[i]) {
                continue;
            }
            ASSERT_EQ(p[index++], i);
        }
    };

    // validate the left data is correct after deletion
    validate_entity_data();

    //    // delete entities from 700 to 800
    //    int64_t delete_count_2 = 100;
    //    delete_entity(700, 700 + delete_count_2);
    //
    //    status = db_->Flush();
    //    ASSERT_TRUE(status.ok());
    //
    //    auto validate_compact = [&](double threshold) -> void {
    //        int64_t row_count = 0;
    //        status = db_->CountEntities(collection_name, row_count);
    //        ASSERT_TRUE(status.ok());
    //        ASSERT_EQ(row_count, entity_count - delete_count_1 - delete_count_2);
    //
    //        status = db_->Compact(dummy_context_, collection_name, threshold);
    //        ASSERT_TRUE(status.ok());
    //
    //        validate_entity_data();
    //
    //        status = db_->CountEntities(collection_name, row_count);
    //        ASSERT_TRUE(status.ok());
    //        ASSERT_EQ(row_count, entity_count - delete_count_1 - delete_count_2);
    //
    //        validate_entity_data();
    //    };
    //
    //    // compact the collection, when threshold = 0.001, the compact do nothing
    //    validate_compact(0.001); // compact skip
    //    validate_compact(0.5); // do compact
}

TEST_F(DBTest, IndexTest) {
    std::string collection_name = "INDEX_TEST";
    auto status = CreateCollection2(db_, collection_name);
    ASSERT_TRUE(status.ok());

    // insert 10000 entities into default partition
    const uint64_t entity_count = 10000;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(entity_count, 0, data_chunk);

    status = db_->Insert(collection_name, "", data_chunk);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    // create index for vector field, validate the index
    {
        milvus::engine::CollectionIndex index;
        index.index_name_ = "my_index1";
        index.index_type_ = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
        index.metric_name_ = milvus::knowhere::Metric::L2;
        index.extra_params_["nlist"] = 2048;
        status = db_->CreateIndex(dummy_context_, collection_name, VECTOR_FIELD_NAME, index);
        ASSERT_TRUE(status.ok());

        milvus::engine::CollectionIndex index_get;
        status = db_->DescribeIndex(collection_name, VECTOR_FIELD_NAME, index_get);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(index.index_name_, index_get.index_name_);
        ASSERT_EQ(index.index_type_, index_get.index_type_);
        ASSERT_EQ(index.metric_name_, index_get.metric_name_);
        ASSERT_EQ(index.extra_params_, index_get.extra_params_);
    }

    // create index for structured fields, validate the index
    {
        milvus::engine::CollectionIndex index;
        index.index_name_ = "my_index2";
        index.index_type_ = milvus::engine::DEFAULT_STRUCTURED_INDEX;
        status = db_->CreateIndex(dummy_context_, collection_name, "field_0", index);
        ASSERT_TRUE(status.ok());
        status = db_->CreateIndex(dummy_context_, collection_name, "field_1", index);
        ASSERT_TRUE(status.ok());
        status = db_->CreateIndex(dummy_context_, collection_name, "field_2", index);
        ASSERT_TRUE(status.ok());

        milvus::engine::CollectionIndex index_get;
        status = db_->DescribeIndex(collection_name, "field_0", index_get);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(index.index_name_, index_get.index_name_);
        ASSERT_EQ(index.index_type_, index_get.index_type_);
    }

    // drop index of vector field
    {
        status = db_->DropIndex(collection_name, VECTOR_FIELD_NAME);
        ASSERT_TRUE(status.ok());

        milvus::engine::CollectionIndex index_get;
        status = db_->DescribeIndex(collection_name, VECTOR_FIELD_NAME, index_get);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(index_get.index_name_.empty());
    }

    // drop index of structured field 'field_0'
    {
        status = db_->DropIndex(collection_name, "field_0");
        ASSERT_TRUE(status.ok());

        milvus::engine::CollectionIndex index_get;
        status = db_->DescribeIndex(collection_name, "field_0", index_get);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(index_get.index_name_.empty());
    }
}

TEST_F(DBTest, StatsTest) {
    std::string collection_name = "STATS_TEST";
    auto status = CreateCollection2(db_, collection_name);
    ASSERT_TRUE(status.ok());

    std::string partition_name = "p1";
    status = db_->CreatePartition(collection_name, partition_name);
    ASSERT_TRUE(status.ok());

    // insert 10000 entities into default partition
    // insert 10000 entities into partition 'p1'
    const uint64_t entity_count = 10000;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(entity_count, 0, data_chunk);

    status = db_->Insert(collection_name, "", data_chunk);
    ASSERT_TRUE(status.ok());

    BuildEntities(entity_count, 0, data_chunk);
    status = db_->Insert(collection_name, partition_name, data_chunk);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    // create index for vector field
    {
        milvus::engine::CollectionIndex index;
        index.index_type_ = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
        index.metric_name_ = milvus::knowhere::Metric::L2;
        index.extra_params_["nlist"] = 2048;
        status = db_->CreateIndex(dummy_context_, collection_name, VECTOR_FIELD_NAME, index);
        ASSERT_TRUE(status.ok());
    }

    // create index for structured fields
    {
        milvus::engine::CollectionIndex index;
        index.index_type_ = milvus::engine::DEFAULT_STRUCTURED_INDEX;
        status = db_->CreateIndex(dummy_context_, collection_name, "field_0", index);
        ASSERT_TRUE(status.ok());
        status = db_->CreateIndex(dummy_context_, collection_name, "field_1", index);
        ASSERT_TRUE(status.ok());
        status = db_->CreateIndex(dummy_context_, collection_name, "field_2", index);
        ASSERT_TRUE(status.ok());
    }

    // validate collection stats
    milvus::json json_stats;
    status = db_->GetCollectionStats(collection_name, json_stats);
    ASSERT_TRUE(status.ok());

    std::string ss = json_stats.dump();
    ASSERT_FALSE(ss.empty());

    int64_t row_count = json_stats[milvus::engine::JSON_ROW_COUNT].get<int64_t>();
    ASSERT_EQ(row_count, entity_count * 2);

    int64_t data_size = json_stats[milvus::engine::JSON_DATA_SIZE].get<int64_t>();
    ASSERT_GT(data_size, 0);

    auto partitions = json_stats[milvus::engine::JSON_PARTITIONS];
    ASSERT_EQ(partitions.size(), 2);

    for (int32_t i = 0; i < 2; ++i) {
        auto partition = partitions[i];
        row_count = partition[milvus::engine::JSON_ROW_COUNT].get<int64_t>();
        ASSERT_EQ(row_count, entity_count);
        auto segments = partition[milvus::engine::JSON_SEGMENTS];
        ASSERT_EQ(segments.size(), 1);

        auto segment = segments[0];
        row_count = segment[milvus::engine::JSON_ROW_COUNT].get<int64_t>();
        ASSERT_EQ(row_count, entity_count);

        data_size = segment[milvus::engine::JSON_DATA_SIZE].get<int64_t>();
        ASSERT_GT(data_size, 0);

        auto files = segment[milvus::engine::JSON_FILES];
        ASSERT_GT(files.size(), 0);

        for (uint64_t k = 0; k < files.size(); ++k) {
            auto file = files[k];
            std::string field_name = file[milvus::engine::JSON_FIELD].get<std::string>();
            ASSERT_FALSE(field_name.empty());
            std::string path = file[milvus::engine::JSON_PATH].get<std::string>();
            ASSERT_FALSE(path.empty());
            data_size = file[milvus::engine::JSON_DATA_SIZE].get<int64_t>();
            ASSERT_GE(data_size, 0);
        }
    }
}

TEST_F(DBTest, FetchTest1) {
    std::string collection_name = "STATS_TEST";
    auto status = CreateCollection2(db_, collection_name);
    ASSERT_TRUE(status.ok());

    std::string partition_name1 = "p1";
    status = db_->CreatePartition(collection_name, partition_name1);
    ASSERT_TRUE(status.ok());

    std::string partition_name2 = "p2";
    status = db_->CreatePartition(collection_name, partition_name2);
    ASSERT_TRUE(status.ok());

    milvus::engine::IDNumbers ids_1, ids_2;
    std::vector<float> fetch_vectors;
    {
        // insert 100 entities into partition 'p1'
        const uint64_t entity_count = 100;
        milvus::engine::DataChunkPtr data_chunk;
        BuildEntities(entity_count, 0, data_chunk);

        float* p = (float*)(data_chunk->fixed_fields_[VECTOR_FIELD_NAME]->data_.data());
        for (int64_t i = 0; i < COLLECTION_DIM; ++i) {
            fetch_vectors.push_back(p[i]);
        }

        status = db_->Insert(collection_name, partition_name1, data_chunk);
        ASSERT_TRUE(status.ok());

        milvus::engine::utils::GetIDFromChunk(data_chunk, ids_1);
        ASSERT_EQ(ids_1.size(), entity_count);
    }

    {
        // insert 101 entities into partition 'p2'
        const uint64_t entity_count = 101;
        milvus::engine::DataChunkPtr data_chunk;
        BuildEntities(entity_count, 0, data_chunk);

        float* p = (float*)(data_chunk->fixed_fields_[VECTOR_FIELD_NAME]->data_.data());
        for (int64_t i = 0; i < COLLECTION_DIM; ++i) {
            fetch_vectors.push_back(p[i]);
        }

        status = db_->Insert(collection_name, partition_name2, data_chunk);
        ASSERT_TRUE(status.ok());

        milvus::engine::utils::GetIDFromChunk(data_chunk, ids_2);
        ASSERT_EQ(ids_2.size(), entity_count);
    }

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    // fetch no.1 entity from partition 'p1'
    // fetch no.2 entity from partition 'p2'
    std::vector<std::string> field_names = {milvus::engine::FIELD_UID, VECTOR_FIELD_NAME};
    std::vector<bool> valid_row;
    milvus::engine::DataChunkPtr fetch_chunk;
    milvus::engine::IDNumbers fetch_ids = {ids_1[0], ids_2[0]};
    status = db_->GetEntityByID(collection_name, fetch_ids, field_names, valid_row, fetch_chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(fetch_chunk->count_, fetch_ids.size());
    ASSERT_EQ(fetch_chunk->fixed_fields_[VECTOR_FIELD_NAME]->data_.size(),
              fetch_ids.size() * COLLECTION_DIM * sizeof(float));

    // compare result
    std::vector<float> result_vectors;
    float* p = (float*)(fetch_chunk->fixed_fields_[VECTOR_FIELD_NAME]->data_.data());
    for (int64_t i = 0; i < COLLECTION_DIM * fetch_ids.size(); i++) {
        result_vectors.push_back(p[i]);
    }
    ASSERT_EQ(fetch_vectors, result_vectors);

    //    std::string collection_name = "STATS_TEST";
    //    auto status = CreateCollection2(db_, collection_name, 0);
    //    ASSERT_TRUE(status.ok());
    //
    //    std::string partition_name1 = "p1";
    //    status = db_->CreatePartition(collection_name, partition_name1);
    //    ASSERT_TRUE(status.ok());
    //
    //    milvus::engine::IDNumbers ids_1;
    //    std::vector<float> fetch_vectors;
    //    {
    //        // insert 100 entities into partition 'p1'
    //        const uint64_t entity_count = 100;
    //        milvus::engine::DataChunkPtr data_chunk;
    //        BuildEntities(entity_count, 0, data_chunk);
    //
    //        float* p = (float*)(data_chunk->fixed_fields_[VECTOR_FIELD_NAME]->data_.data());
    //        for (int64_t i = 0; i < COLLECTION_DIM; ++i) {
    //            fetch_vectors.push_back(p[i]);
    //        }
    //
    //        status = db_->Insert(collection_name, partition_name1, data_chunk);
    //        ASSERT_TRUE(status.ok());
    //
    //        milvus::engine::utils::GetIDFromChunk(data_chunk, ids_1);
    //        ASSERT_EQ(ids_1.size(), entity_count);
    //    }
    //
    //    status = db_->Flush();
    //    ASSERT_TRUE(status.ok());
    //
    //    // fetch no.1 entity from partition 'p1'
    //    // fetch no.2 entity from partition 'p2'
    //    std::vector<std::string> field_names = {milvus::engine::FIELD_UID, VECTOR_FIELD_NAME};
    //    std::vector<bool> valid_row;
    //    milvus::engine::DataChunkPtr fetch_chunk;
    //    milvus::engine::IDNumbers fetch_ids = {ids_1[0]};
    //    status = db_->GetEntityByID(collection_name, fetch_ids, field_names, valid_row, fetch_chunk);
    //    ASSERT_TRUE(status.ok());
    //    ASSERT_EQ(fetch_chunk->count_, fetch_ids.size());
    //    ASSERT_EQ(fetch_chunk->fixed_fields_[VECTOR_FIELD_NAME]->data_.size(),
    //              fetch_ids.size() * COLLECTION_DIM * sizeof(float));
    //
    //    // compare result
    //    std::vector<float> result_vectors;
    //    float* p = (float*)(fetch_chunk->fixed_fields_[VECTOR_FIELD_NAME]->data_.data());
    //    for (int64_t i = 0; i < COLLECTION_DIM; i++) {
    //        result_vectors.push_back(p[i]);
    //    }
    //    ASSERT_EQ(fetch_vectors, result_vectors);
}

TEST_F(DBTest, FetchTest2) {
    std::string collection_name = "STATS_TEST";
    auto status = CreateCollection2(db_, collection_name);
    ASSERT_TRUE(status.ok());

    std::string partition_name = "p1";
    status = db_->CreatePartition(collection_name, partition_name);
    ASSERT_TRUE(status.ok());

    // insert 1000 entities into default partition
    // insert 1000 entities into partition 'p1'
    const uint64_t entity_count = 1000;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(entity_count, 0, data_chunk);

    status = db_->Insert(collection_name, "", data_chunk);
    ASSERT_TRUE(status.ok());

    BuildEntities(entity_count, 0, data_chunk);
    status = db_->Insert(collection_name, partition_name, data_chunk);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    // get all the entities id of partition 'p1'
    milvus::engine::IDNumbers batch_entity_ids;
    milvus::engine::utils::GetIDFromChunk(data_chunk, batch_entity_ids);
    ASSERT_EQ(batch_entity_ids.size(), entity_count);

    // delete first 10 entities from partition 'p1'
    int64_t delete_count = 10;
    milvus::engine::IDNumbers delete_entity_ids = batch_entity_ids;
    delete_entity_ids.resize(delete_count);
    status = db_->DeleteEntityByID(collection_name, delete_entity_ids);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    // try fetch first 10 entities of parititon 'p1', since they were deleted, nothing returned
    std::vector<std::string> field_names = {milvus::engine::FIELD_UID, VECTOR_FIELD_NAME, "field_0"};
    std::vector<bool> valid_row;
    milvus::engine::DataChunkPtr fetch_chunk;
    status = db_->GetEntityByID(collection_name, delete_entity_ids, field_names, valid_row, fetch_chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(fetch_chunk->count_, 0);
    for (auto valid : valid_row) {
        ASSERT_FALSE(valid);
    }

    // fetch all entities from the partition 'p1', 990 entities returned
    fetch_chunk = nullptr;
    status = db_->GetEntityByID(collection_name, batch_entity_ids, field_names, valid_row, fetch_chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(fetch_chunk->count_, batch_entity_ids.size() - delete_entity_ids.size());
    auto& uid = fetch_chunk->fixed_fields_[milvus::engine::FIELD_UID];
    ASSERT_EQ(uid->Size() / sizeof(int64_t), fetch_chunk->count_);
    auto& vectors = fetch_chunk->fixed_fields_[VECTOR_FIELD_NAME];
    ASSERT_EQ(vectors->Size() / (COLLECTION_DIM * sizeof(float)), fetch_chunk->count_);

    // get collection stats
    // the segment of default partition has 1000 entities
    // the segment of partition 'p1' only has 990 entities
    milvus::json json_stats;
    status = db_->GetCollectionStats(collection_name, json_stats);
    ASSERT_TRUE(status.ok());

    auto partitions = json_stats[milvus::engine::JSON_PARTITIONS];
    ASSERT_EQ(partitions.size(), 2);

    for (int32_t i = 0; i < 2; ++i) {
        auto partition = partitions[i];
        std::string tag = partition[milvus::engine::JSON_PARTITION_TAG].get<std::string>();

        auto segments = partition[milvus::engine::JSON_SEGMENTS];
        ASSERT_EQ(segments.size(), 1);

        auto segment = segments[0];
        int64_t id = segment[milvus::engine::JSON_ID].get<int64_t>();

        milvus::engine::IDNumbers segment_entity_ids;
        status = db_->ListIDInSegment(collection_name, id, segment_entity_ids);
        ASSERT_TRUE(status.ok());

        if (tag == partition_name) {
            ASSERT_EQ(segment_entity_ids.size(), batch_entity_ids.size() - delete_entity_ids.size());
        } else {
            ASSERT_EQ(segment_entity_ids.size(), batch_entity_ids.size());
        }
    }
}

TEST_F(DBTest, DeleteEntitiesTest) {
    std::string collection_name = "test_collection_delete_";
    CreateCollection2(db_, collection_name, false);

    // insert 100 entities into default partition without flush
    milvus::engine::IDNumbers entity_ids;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(100, 0, data_chunk, true);
    auto status = db_->Insert(collection_name, "", data_chunk);
    milvus::engine::utils::GetIDFromChunk(data_chunk, entity_ids);

    // delete all the entities in memory
    status = db_->DeleteEntityByID(collection_name, entity_ids);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // flush empty segment
    db_->Flush(collection_name);

    auto insert_entities = [&](const std::string& collection, const std::string& partition, uint64_t count,
                               uint64_t batch_index, milvus::engine::IDNumbers& ids) -> Status {
        milvus::engine::DataChunkPtr data_chunk;
        BuildEntities(count, batch_index, data_chunk, true);
        STATUS_CHECK(db_->Insert(collection, partition, data_chunk));
        STATUS_CHECK(db_->Flush(collection));

        ids.clear();
        milvus::engine::utils::GetIDFromChunk(data_chunk, ids);
        return Status::OK();
    };

    // insert 1000 entities into default partition
    status = insert_entities(collection_name, "", 1000, 0, entity_ids);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // delete the first entity
    milvus::engine::IDNumbers delete_ids = {entity_ids[0]};
    status = db_->DeleteEntityByID(collection_name, delete_ids);
    ASSERT_TRUE(status.ok()) << status.ToString();

    milvus::engine::IDNumbers whole_delete_ids;
    fiu_init(0);
    fiu_enable_random("MemCollection.ApplyDeletes.RandomSleep", 1, nullptr, 0, 0.5);
    for (size_t i = 0; i < 5; i++) {
        std::string partition0 = collection_name + "p_" + std::to_string(i) + "_0";
        std::string partition1 = collection_name + "p_" + std::to_string(i) + "_1";

        status = db_->CreatePartition(collection_name, partition0);
        ASSERT_TRUE(status.ok()) << status.ToString();

        status = db_->CreatePartition(collection_name, partition1);
        ASSERT_TRUE(status.ok()) << status.ToString();

        // insert 1000 entities into partition_0
        milvus::engine::IDNumbers partition0_ids;
        status = insert_entities(collection_name, partition0, 1000, 2 * i + 1, partition0_ids);
        ASSERT_TRUE(status.ok()) << status.ToString();

        // insert 1000 entities into partition_1
        milvus::engine::IDNumbers partition1_ids;
        status = insert_entities(collection_name, partition1, 1000, 2 * i + 2, partition1_ids);
        ASSERT_TRUE(status.ok()) << status.ToString();

        // delete the first entity of each partition
        milvus::engine::IDNumbers partition_delete_ids = {partition0_ids[0], partition1_ids[0]};
        whole_delete_ids.insert(whole_delete_ids.begin(), partition_delete_ids.begin(), partition_delete_ids.end());
        db_->DeleteEntityByID(collection_name, partition_delete_ids);
        ASSERT_TRUE(status.ok()) << status.ToString();

        // drop partition_1
        status = db_->DropPartition(collection_name, partition1);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    // flush will apply the deletion
    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    fiu_disable("MemCollection.ApplyDeletes.RandomSleep");

    // get first entity from partition_0, the entity has been deleted, no entity returned
    // get first entity from partition_1, the partition has been deleted, no entity returned
    std::vector<bool> valid_row;
    milvus::engine::DataChunkPtr entity_data_chunk;
    for (auto& id : whole_delete_ids) {
        std::cout << "get entity: " << id << std::endl;
        status = db_->GetEntityByID(collection_name, {id}, {}, valid_row, entity_data_chunk);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(entity_data_chunk->count_, 0);
    }
}

TEST_F(DBTest, DeleteStaleTest) {
    const int del_id_pair = 3;
    auto insert_entities = [&](const std::string& collection, const std::string& partition, uint64_t count,
                               uint64_t batch_index, milvus::engine::IDNumbers& ids) -> Status {
        milvus::engine::DataChunkPtr data_chunk;
        BuildEntities(count, batch_index, data_chunk);
        STATUS_CHECK(db_->Insert(collection, partition, data_chunk));
        STATUS_CHECK(db_->Flush(collection));
        auto iter = data_chunk->fixed_fields_.find(milvus::engine::FIELD_UID);
        if (iter == data_chunk->fixed_fields_.end()) {
            return Status(1, "Cannot find uid field");
        }
        auto& ids_buffer = iter->second;
        ids.resize(data_chunk->count_);
        memcpy(ids.data(), ids_buffer->data_.data(), ids_buffer->Size());

        return Status::OK();
    };

    auto build_task = [&](const std::string& collection, const std::string& field) {
        milvus::engine::CollectionIndex index;
        index.index_name_ = "my_index1";
        index.index_type_ = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
        index.metric_name_ = milvus::knowhere::Metric::L2;
        index.extra_params_["nlist"] = 2048;
        auto status = db_->CreateIndex(dummy_context_, collection, field, index);
        ASSERT_TRUE(status.ok()) << status.ToString();
    };

    auto delete_task = [&](const std::string& collection, const milvus::engine::IDNumbers& del_ids) {
        auto status = Status::OK();
        for (size_t i = 0; i < del_id_pair; i++) {
            milvus::engine::IDNumbers ids = {del_ids[2 * i], del_ids[2 * i + 1]};
            status = db_->DeleteEntityByID(collection, ids);
            ASSERT_TRUE(status.ok()) << status.ToString();
            sleep(1);
        }
    };

    const std::string collection_name = "test_delete_stale_";
    auto status = CreateCollection2(db_, collection_name);
    ASSERT_TRUE(status.ok()) << status.ToString();
    milvus::engine::IDNumbers del_ids;
    milvus::engine::IDNumbers entity_ids;
    status = insert_entities(collection_name, "", 10000, 0, entity_ids);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = db_->Flush(collection_name);
    ASSERT_TRUE(status.ok()) << status.ToString();

    milvus::engine::IDNumbers entity_ids2;
    status = insert_entities(collection_name, "", 10000, 1, entity_ids2);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = db_->Flush(collection_name);
    ASSERT_TRUE(status.ok()) << status.ToString();

    for (size_t i = 0; i < del_id_pair; i++) {
        del_ids.push_back(entity_ids[i]);
        del_ids.push_back(entity_ids2[i]);
    }

    fiu_init(0);
    fiu_enable_random("MemCollection.ApplyDeletes.RandomSleep", 1, nullptr, 0, 0.5);
    auto build_thread = std::thread(build_task, collection_name, VECTOR_FIELD_NAME);
    auto delete_thread = std::thread(delete_task, collection_name, del_ids);
    delete_thread.join();
    build_thread.join();
    fiu_disable("MemCollection.ApplyDeletes.RandomSleep");
    db_->Flush();
    int64_t row_count;
    status = db_->CountEntities(collection_name, row_count);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(row_count, 10000 * 2 - 2 * del_id_pair);

    std::vector<bool> valid_row;
    milvus::engine::DataChunkPtr entity_data_chunk;
    for (size_t j = 0; j < del_ids.size(); j++) {
        status = db_->GetEntityByID(collection_name, {del_ids[j]}, {}, valid_row, entity_data_chunk);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(entity_data_chunk->count_, 0) << "[" << j << "] Delete id " << del_ids[j] << " failed.";
    }
}

TEST_F(DBTest, LoadTest) {
    std::string collection_name = "LOAD_TEST";
    auto status = CreateCollection2(db_, collection_name);
    ASSERT_TRUE(status.ok());

    std::string partition_name = "p1";
    status = db_->CreatePartition(collection_name, partition_name);
    ASSERT_TRUE(status.ok());

    // insert 1000 entities into default partition
    // insert 1000 entities into partition 'p1'
    const uint64_t entity_count = 1000;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(entity_count, 0, data_chunk);

    status = db_->Insert(collection_name, "", data_chunk);
    ASSERT_TRUE(status.ok());

    BuildEntities(entity_count, 0, data_chunk);
    status = db_->Insert(collection_name, partition_name, data_chunk);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    auto& cache_mgr = milvus::cache::CpuCacheMgr::GetInstance();
    cache_mgr.ClearCache();

    // load "vector", "field_1"
    std::vector<std::string> fields = {VECTOR_FIELD_NAME, "field_1"};
    status = db_->LoadCollection(dummy_context_, collection_name, fields);
    ASSERT_TRUE(status.ok());

    // 2 segments, 2 fields, at least 4 files loaded
    ASSERT_GE(cache_mgr.ItemCount(), 4);

    int64_t total_size = entity_count * (COLLECTION_DIM * sizeof(float) + sizeof(int64_t)) * 2;
    ASSERT_GE(cache_mgr.CacheUsage(), total_size);

    // load all fields
    fields.clear();
    cache_mgr.ClearCache();

    status = db_->LoadCollection(dummy_context_, collection_name, fields);
    ASSERT_TRUE(status.ok());

    // 2 segments, 4 fields, at least 8 files loaded
    ASSERT_GE(cache_mgr.ItemCount(), 8);

    total_size =
        entity_count * (COLLECTION_DIM * sizeof(float) + sizeof(int32_t) + sizeof(int64_t) + sizeof(double)) * 2;
    ASSERT_GE(cache_mgr.CacheUsage(), total_size);
}
