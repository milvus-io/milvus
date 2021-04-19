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

// #include <fiu-control.h>
// #include <fiu-local.h>
// #include <gtest/gtest.h>

#include <iostream>
#include <string>

// #include "db/SnapshotVisitor.h"
// #include "db/Types.h"
// #include "db/snapshot/IterateHandler.h"
// #include "db/snapshot/Resources.h"
// #include "db/utils.h"
// #include "knowhere/index/vector_index/helpers/IndexParameter.h"
// #include "segment/SegmentReader.h"
// #include "segment/SegmentWriter.h"
// #include "src/dog_segment/SegmentBase.h"
// #include "utils/Json.h"
#include "dog_segment/SegmentBase.h"
using std::cin;
using std::cout;
using std::endl;

using SegmentVisitor = milvus::engine::SegmentVisitor;

namespace {
// milvus::Status
// CreateCollection(std::shared_ptr<DB> db, const std::string& collection_name, const LSN_TYPE& lsn) {
//     CreateCollectionContext context;
//     context.lsn = lsn;
//     auto collection_schema = std::make_shared<Collection>(collection_name);
//     context.collection = collection_schema;

//     int64_t collection_id = 0;
//     int64_t field_id = 0;
//     /* field uid */
//     auto uid_field = std::make_shared<Field>(milvus::engine::FIELD_UID, 0, milvus::engine::DataType::INT64,
//                                              milvus::engine::snapshot::JEmpty, field_id);
//     auto uid_field_element_blt =
//         std::make_shared<FieldElement>(collection_id, field_id, milvus::engine::ELEMENT_BLOOM_FILTER,
//                                        milvus::engine::FieldElementType::FET_BLOOM_FILTER);
//     auto uid_field_element_del =
//         std::make_shared<FieldElement>(collection_id, field_id, milvus::engine::ELEMENT_DELETED_DOCS,
//                                        milvus::engine::FieldElementType::FET_DELETED_DOCS);

//     field_id++;
//     /* field vector */
//     milvus::json vector_param = {{milvus::knowhere::meta::DIM, 4}};
//     auto vector_field =
//         std::make_shared<Field>("vector", 0, milvus::engine::DataType::VECTOR_FLOAT, vector_param, field_id);
//     auto vector_field_element_index =
//         std::make_shared<FieldElement>(collection_id, field_id, milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8,
//                                        milvus::engine::FieldElementType::FET_INDEX);
//     /* another field*/
//     auto int_field = std::make_shared<Field>("int", 0, milvus::engine::DataType::INT32,
//                                              milvus::engine::snapshot::JEmpty, field_id++);

//     context.fields_schema[uid_field] = {uid_field_element_blt, uid_field_element_del};
//     context.fields_schema[vector_field] = {vector_field_element_index};
//     context.fields_schema[int_field] = {};

//     return db->CreateCollection(context);
// }
// }  // namespace

TEST_F(DogSegmentTest, TestABI) {
    using namespace milvus::engine;
    using namespace milvus::dog_segment;
    ASSERT_EQ(TestABI(), 42);
    assert(true);
}

// TEST_F(DogSegmentTest, TestCreateAndSchema) {
//     using namespace milvus::engine;
//     using namespace milvus::dog_segment;
//     // step1: create segment from current snapshot.

//     LSN_TYPE lsn = 0;
//     auto next_lsn = [&]() -> decltype(lsn) { return ++lsn; };

//     // step 1.1: create collection
//     std::string db_root = "/tmp/milvus_test/db/table";
//     std::string collection_name = "c1";
//     auto status = CreateCollection(db_, collection_name, next_lsn());
//     ASSERT_TRUE(status.ok());

//     // step 1.2: get snapshot
//     ScopedSnapshotT snapshot;
//     status = Snapshots::GetInstance().GetSnapshot(snapshot, collection_name);
//     ASSERT_TRUE(status.ok());
//     ASSERT_TRUE(snapshot);
//     ASSERT_EQ(snapshot->GetName(), collection_name);

//     // step 1.3: get partition_id
//     cout << endl;
//     cout << endl;
//     ID_TYPE partition_id = snapshot->GetResources<Partition>().begin()->first;
//     cout << partition_id;

//     // step 1.5 create schema from ids
//     auto collection = snapshot->GetCollection();

//     auto field_names = snapshot->GetFieldNames();
//     auto schema = std::make_shared<Schema>();
//     for (const auto& field_name : field_names) {
//         auto the_field = snapshot->GetField(field_name);
//         auto param = the_field->GetParams();
//         auto type = the_field->GetFtype();
//         cout << field_name        //
//              << " " << (int)type  //
//              << " " << param      //
//              << endl;
//         FieldMeta field(field_name, type);
//         int dim = 1;
//         if(field.is_vector()) {
//             field.set_dim(dim);
//         }
//         schema->AddField(field);

//     }
//     // step 1.6 create a segment from ids
//     auto segment = CreateSegment(schema);
//     std::vector<id_t> primary_ids;
// }



TEST_F(DogSegmentTest, MockTest) {
    using namespace milvus::dog_segment;
    using namespace milvus::engine;
    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    schema->AddField("age", DataType::INT32);
    std::vector<char> raw_data;
    std::vector<Timestamp> timestamps;
    std::vector<uint64_t> uids;
    int N = 10000;
    std::default_random_engine e(67);
    for(int i = 0; i < N; ++i) {
        uids.push_back(100000 + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for(auto &x: vec) {
            x = e() % 2000 * 0.001 - 1.0;
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }
    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
    assert(raw_data.size() == line_sizeof * N);

    auto segment = CreateSegment(schema);
    DogDataChunk data_chunk{raw_data.data(), (int)line_sizeof, N};
    segment->Insert(N, uids.data(), timestamps.data(), data_chunk);
    QueryResult query_result;
    segment->Query(nullptr, 0, query_result);
    int i = 0;
    i++;
}

//TEST_F(DogSegmentTest, DogSegmentTest) {
//    LSN_TYPE lsn = 0;
//    auto next_lsn = [&]() -> decltype(lsn) { return ++lsn; };
//
//    std::string db_root = "/tmp/milvus_test/db/table";
//    std::string c1 = "c1";
//    auto status = CreateCollection(db_, c1, next_lsn());
//    ASSERT_TRUE(status.ok());
//
//    ScopedSnapshotT snapshot;
//    status = Snapshots::GetInstance().GetSnapshot(snapshot, c1);
//    ASSERT_TRUE(status.ok());
//    ASSERT_TRUE(snapshot);
//    ASSERT_EQ(snapshot->GetName(), c1);
//    {
//        SegmentFileContext sf_context;
//        SFContextBuilder(sf_context, snapshot);
//    }
//    std::vector<SegmentFileContext> segfile_ctxs;
//    SFContextsBuilder(segfile_ctxs, snapshot);
//
//    std::cout << snapshot->ToString() << std::endl;
//
//    ID_TYPE partition_id;
//    {
//        auto& partitions = snapshot->GetResources<Partition>();
//        partition_id = partitions.begin()->first;
//    }
//
//    [&next_lsn,      //
//     &segfile_ctxs,  //
//     &partition_id,  //
//     &snapshot,      //
//     &db_root] {
//        /* commit new segment */
//        OperationContext op_ctx;
//        op_ctx.lsn = next_lsn();
//        op_ctx.prev_partition = snapshot->GetResource<Partition>(partition_id);
//
//        auto new_seg_op = std::make_shared<NewSegmentOperation>(op_ctx, snapshot);
//        SegmentPtr new_seg;
//        auto status = new_seg_op->CommitNewSegment(new_seg);
//        ASSERT_TRUE(status.ok());
//
//        /* commit new segment file */
//        for (auto& cctx : segfile_ctxs) {
//            SegmentFilePtr seg_file;
//            auto nsf_context = cctx;
//            nsf_context.segment_id = new_seg->GetID();
//            nsf_context.partition_id = new_seg->GetPartitionId();
//            status = new_seg_op->CommitNewSegmentFile(nsf_context, seg_file);
//        }
//
//        /* build segment visitor */
//        auto ctx = new_seg_op->GetContext();
//        ASSERT_TRUE(ctx.new_segment);
//        auto visitor = SegmentVisitor::Build(snapshot, ctx.new_segment, ctx.new_segment_files);
//        ASSERT_TRUE(visitor);
//        ASSERT_EQ(visitor->GetSegment(), new_seg);
//        ASSERT_FALSE(visitor->GetSegment()->IsActive());
//        // std::cout << visitor->ToString() << std::endl;
//        // std::cout << snapshot->ToString() << std::endl;
//
//        /* write data */
//        milvus::segment::SegmentWriter segment_writer(db_root, visitor);
//
//        //        std::vector<milvus::segment::doc_id_t> raw_uids = {123};
//        //        std::vector<uint8_t> raw_vectors = {1, 2, 3, 4};
//        //        status = segment_writer.AddChunk("test", raw_vectors, raw_uids);
//        //        ASSERT_TRUE(status.ok())
//        //
//        //        status = segment_writer.Serialize();
//        //        ASSERT_TRUE(status.ok());
//
//        /* read data */
//        //        milvus::segment::SSSegmentReader segment_reader(db_root, visitor);
//        //
//        //        status = segment_reader.Load();
//        //        ASSERT_TRUE(status.ok());
//        //
//        //        milvus::segment::SegmentPtr segment_ptr;
//        //        status = segment_reader.GetSegment(segment_ptr);
//        //        ASSERT_TRUE(status.ok());
//        //
//        //        auto& out_uids = segment_ptr->vectors_ptr_->GetUids();
//        //        ASSERT_EQ(raw_uids.size(), out_uids.size());
//        //        ASSERT_EQ(raw_uids[0], out_uids[0]);
//        //        auto& out_vectors = segment_ptr->vectors_ptr_->GetData();
//        //        ASSERT_EQ(raw_vectors.size(), out_vectors.size());
//        //        ASSERT_EQ(raw_vectors[0], out_vectors[0]);
//    }();
//
//    status = db_->DropCollection(c1);
//    ASSERT_TRUE(status.ok());
//}
