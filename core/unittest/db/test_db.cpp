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

#include <string>
#include <set>
#include <algorithm>

#include "segment/Segment.h"
#include "db/utils.h"
#include "db/SnapshotUtils.h"
#include "db/SnapshotVisitor.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/ResourceHelper.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

using SegmentVisitor = milvus::engine::SegmentVisitor;

namespace {
const char* VECTOR_FIELD_NAME = "vector";

milvus::Status
CreateCollection(std::shared_ptr<DBImpl> db, const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
    context.collection = collection_schema;
    auto vector_field = std::make_shared<Field>(VECTOR_FIELD_NAME, 0,
                                                milvus::engine::DataType::VECTOR_FLOAT);
    auto vector_field_element = std::make_shared<FieldElement>(0, 0, "ivfsq8",
                                                               milvus::engine::FieldElementType::FET_INDEX);
    auto int_field = std::make_shared<Field>("int", 0,
                                             milvus::engine::DataType::INT32);
    context.fields_schema[vector_field] = {vector_field_element};
    context.fields_schema[int_field] = {};

    return db->CreateCollection(context);
}

static constexpr int64_t COLLECTION_DIM = 128;

milvus::Status
CreateCollection2(std::shared_ptr<DBImpl> db, const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
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

void
BuildEntities(uint64_t n, uint64_t batch_index, milvus::engine::DataChunkPtr& data_chunk) {
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

    milvus::engine::FIXED_FIELD_DATA& raw = data_chunk->fixed_fields_[VECTOR_FIELD_NAME];
    raw.resize(vectors.float_data_.size() * sizeof(float));
    memcpy(raw.data(), vectors.float_data_.data(), vectors.float_data_.size() * sizeof(float));

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
        milvus::engine::FIXED_FIELD_DATA& raw = data_chunk->fixed_fields_["field_0"];
        raw.resize(value_0.size() * sizeof(int32_t));
        memcpy(raw.data(), value_0.data(), value_0.size() * sizeof(int32_t));
    }

    {
        milvus::engine::FIXED_FIELD_DATA& raw = data_chunk->fixed_fields_["field_1"];
        raw.resize(value_1.size() * sizeof(int64_t));
        memcpy(raw.data(), value_1.data(), value_1.size() * sizeof(int64_t));
    }

    {
        milvus::engine::FIXED_FIELD_DATA& raw = data_chunk->fixed_fields_["field_2"];
        raw.resize(value_2.size() * sizeof(double));
        memcpy(raw.data(), value_2.data(), value_2.size() * sizeof(double));
    }
}
}  // namespace

TEST_F(DBTest, CollectionTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };
    std::string c1 = "c1";
    auto status = CreateCollection(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok());

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
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };
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
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };

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
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };

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
    auto &partitions = ss->GetResources<Partition>();
    ID_TYPE partition_id;
    for (auto &kv : partitions) {
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

    milvus::server::ContextPtr ctx1;
    std::vector<std::string> partition_patterns;
    milvus::query::GeneralQueryPtr general_query;
    milvus::query::QueryPtr query_ptr;
    std::vector<std::string> field_names;
    std::unordered_map<std::string, milvus::engine::DataType> attr_type;
    milvus::engine::QueryResult result;
    //db_->Query(ctx1, c1, partition_patterns, general_query, query_ptr, field_names, attr_type, result);
}

TEST_F(DBTest, InsertTest) {
    std::string collection_name = "MERGE_TEST";
    auto status = CreateCollection2(db_, collection_name, 0);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    const uint64_t entity_count = 100;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(entity_count, 0, data_chunk);

    status = db_->Insert(collection_name, "", data_chunk);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    int64_t row_count = 0;
    status = db_->CountEntities(collection_name, row_count);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(row_count, entity_count);
}

TEST_F(DBTest, MergeTest) {
    std::string collection_name = "MERGE_TEST";
    auto status = CreateCollection2(db_, collection_name, 0);
    ASSERT_TRUE(status.ok());

    const uint64_t entity_count = 100;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(entity_count, 0, data_chunk);

    int64_t repeat = 2;
    for (int32_t i = 0; i < repeat; i++) {
        status = db_->Insert(collection_name, "", data_chunk);
        ASSERT_TRUE(status.ok());

        status = db_->Flush();
        ASSERT_TRUE(status.ok());
    }

    sleep(2); // wait to merge

    int64_t row_count = 0;
    status = db_->CountEntities(collection_name, row_count);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(row_count, entity_count * repeat);

    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    ASSERT_TRUE(status.ok());
    std::cout << ss->ToString() << std::endl;

    auto root_path = GetOptions().meta_.path_ + milvus::engine::COLLECTIONS_FOLDER;
    std::vector<std::string> segment_paths;

    auto seg_executor = [&] (const SegmentPtr& segment, SegmentIterator* handler) -> Status {
        std::string res_path = milvus::engine::snapshot::GetResPath<Segment>(root_path, segment);
        std::cout << res_path << std::endl;
        if (!boost::filesystem::is_directory(res_path)) {
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
    auto sf_executor = [&] (const SegmentFilePtr& segment_file, SegmentFileIterator* handler) -> Status {
        std::string res_path = milvus::engine::snapshot::GetResPath<SegmentFile>(root_path, segment_file);
        if (boost::filesystem::is_regular_file(res_path)
            || boost::filesystem::is_regular_file(res_path + milvus::codec::IdBloomFilterFormat::FilePostfix())
            || boost::filesystem::is_regular_file(res_path + milvus::codec::DeletedDocsFormat::FilePostfix())) {
            segment_file_paths.insert(res_path);
            std::cout << res_path << std::endl;
        }
        return Status::OK();
    };
    auto sf_iterator = std::make_shared<SegmentFileIterator>(ss, sf_executor);
    sf_iterator->Iterate();

    std::set<std::string> expect_file_paths;
    boost::filesystem::recursive_directory_iterator iter(root_path);
    boost::filesystem::recursive_directory_iterator end;
    for (; iter != end ; ++iter) {
        if (boost::filesystem::is_regular_file((*iter).path())) {
            expect_file_paths.insert((*iter).path().filename().string());
        }
    }

    // TODO: Fix segment file suffix issue.
    ASSERT_EQ(expect_file_paths.size(), segment_file_paths.size());
}

TEST_F(DBTest, IndexTest) {
    std::string collection_name = "INDEX_TEST";
    auto status = CreateCollection2(db_, collection_name, 0);
    ASSERT_TRUE(status.ok());

    const uint64_t entity_count = 10000;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(entity_count, 0, data_chunk);

    status = db_->Insert(collection_name, "", data_chunk);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

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

    {
        milvus::engine::CollectionIndex index;
        index.index_name_ = "my_index2";
        index.index_type_ = milvus::engine::DEFAULT_STRUCTURED_INDEX_NAME;
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

    {
        status = db_->DropIndex(collection_name, VECTOR_FIELD_NAME);
        ASSERT_TRUE(status.ok());

        milvus::engine::CollectionIndex index_get;
        status = db_->DescribeIndex(collection_name, VECTOR_FIELD_NAME, index_get);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(index_get.index_name_.empty());
    }

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
    auto status = CreateCollection2(db_, collection_name, 0);
    ASSERT_TRUE(status.ok());

    std::string partition_name = "p1";
    status = db_->CreatePartition(collection_name, partition_name);
    ASSERT_TRUE(status.ok());

    const uint64_t entity_count = 10000;
    milvus::engine::DataChunkPtr data_chunk;
    BuildEntities(entity_count, 0, data_chunk);

    status = db_->Insert(collection_name, "", data_chunk);
    ASSERT_TRUE(status.ok());

    status = db_->Insert(collection_name, partition_name, data_chunk);
    ASSERT_TRUE(status.ok());

    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    {
        milvus::engine::CollectionIndex index;
        index.index_type_ = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
        index.metric_name_ = milvus::knowhere::Metric::L2;
        index.extra_params_["nlist"] = 2048;
        status = db_->CreateIndex(dummy_context_, collection_name, VECTOR_FIELD_NAME, index);
        ASSERT_TRUE(status.ok());
    }

    {
        milvus::engine::CollectionIndex index;
        index.index_type_ = milvus::engine::DEFAULT_STRUCTURED_INDEX_NAME;
        status = db_->CreateIndex(dummy_context_, collection_name, "field_0", index);
        ASSERT_TRUE(status.ok());
        status = db_->CreateIndex(dummy_context_, collection_name, "field_1", index);
        ASSERT_TRUE(status.ok());
        status = db_->CreateIndex(dummy_context_, collection_name, "field_2", index);
        ASSERT_TRUE(status.ok());
    }

    milvus::json json_stats;
    status = db_->GetCollectionStats(collection_name, json_stats);
    int64_t row_count = json_stats[milvus::engine::JSON_ROW_COUNT];
    ASSERT_EQ(row_count, entity_count * 2);

//    std::string ss = json_stats.dump();
//    std::cout << ss << std::endl;
}
