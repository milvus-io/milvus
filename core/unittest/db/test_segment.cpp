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

#include "db/utils.h"
#include "db/SnapshotVisitor.h"
#include "db/Types.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/Resources.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "utils/Json.h"

using SegmentVisitor = milvus::engine::SegmentVisitor;

namespace {
milvus::Status
CreateCollection(std::shared_ptr<DBImpl> db, const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
    context.collection = collection_schema;

    int64_t collection_id = 0;
    int64_t field_id = 0;
    /* field uid */
    auto uid_field = std::make_shared<Field>(milvus::engine::DEFAULT_UID_NAME, 0,
            milvus::engine::DataType::INT64, milvus::engine::snapshot::JEmpty, field_id);
    auto uid_field_element_blt = std::make_shared<FieldElement>(collection_id, field_id,
            milvus::engine::DEFAULT_BLOOM_FILTER_NAME, milvus::engine::FieldElementType::FET_BLOOM_FILTER);
    auto uid_field_element_del = std::make_shared<FieldElement>(collection_id, field_id,
            milvus::engine::DEFAULT_DELETED_DOCS_NAME, milvus::engine::FieldElementType::FET_DELETED_DOCS);

    field_id++;
    /* field vector */
    milvus::json vector_param = {{milvus::knowhere::meta::DIM, 4}};
    auto vector_field = std::make_shared<Field>("vector", 0, milvus::engine::DataType::VECTOR_FLOAT, vector_param,
            field_id);
    auto vector_field_element_index = std::make_shared<FieldElement>(collection_id, field_id,
            milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, milvus::engine::FieldElementType::FET_INDEX);

    context.fields_schema[uid_field] = {uid_field_element_blt, uid_field_element_del};
    context.fields_schema[vector_field] = {vector_field_element_index};

    return db->CreateCollection(context);
}
}  // namespace

TEST_F(SegmentTest, SegmentTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };

    std::string db_root = "/tmp/milvus_test/db/table";
    std::string c1 = "c1";
    auto status = CreateCollection(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok());

    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(ss);
    ASSERT_EQ(ss->GetName(), c1);

    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    std::vector<SegmentFileContext> contexts;
    SFContextsBuilder(contexts, ss);


    // std::cout << ss->ToString() << std::endl;

    auto& partitions = ss->GetResources<Partition>();
    ID_TYPE partition_id;
    for (auto& kv : partitions) {
        /* select the first partition */
        partition_id = kv.first;
        break;
    }

    std::vector<milvus::segment::doc_id_t> raw_uids = {123};
    std::vector<uint8_t> raw_vectors = {1, 2, 3, 4};

    {
        /* commit new segment */
        OperationContext context;
        context.lsn = next_lsn();
        context.prev_partition = ss->GetResource<Partition>(partition_id);
        auto op = std::make_shared<NewSegmentOperation>(context, ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());

        /* commit new segment file */
        for (auto& cctx : contexts) {
            SegmentFilePtr seg_file;
            auto nsf_context = cctx;
            nsf_context.segment_id = new_seg->GetID();
            nsf_context.partition_id = new_seg->GetPartitionId();
            status = op->CommitNewSegmentFile(nsf_context, seg_file);
        }

        /* build segment visitor */
        auto ctx = op->GetContext();
        ASSERT_TRUE(ctx.new_segment);
        auto visitor = SegmentVisitor::Build(ss, ctx.new_segment, ctx.new_segment_files);
        ASSERT_TRUE(visitor);
        ASSERT_EQ(visitor->GetSegment(), new_seg);
        ASSERT_FALSE(visitor->GetSegment()->IsActive());
        // std::cout << visitor->ToString() << std::endl;
        // std::cout << ss->ToString() << std::endl;

        /* write data */
        milvus::segment::SegmentWriter segment_writer(db_root, visitor);

//        status = segment_writer.AddChunk("test", raw_vectors, raw_uids);
//        ASSERT_TRUE(status.ok())
//
//        status = segment_writer.Serialize();
//        ASSERT_TRUE(status.ok());

        /* read data */
//        milvus::segment::SSSegmentReader segment_reader(db_root, visitor);
//
//        status = segment_reader.Load();
//        ASSERT_TRUE(status.ok());
//
//        milvus::segment::SegmentPtr segment_ptr;
//        status = segment_reader.GetSegment(segment_ptr);
//        ASSERT_TRUE(status.ok());
//
//        auto& out_uids = segment_ptr->vectors_ptr_->GetUids();
//        ASSERT_EQ(raw_uids.size(), out_uids.size());
//        ASSERT_EQ(raw_uids[0], out_uids[0]);
//        auto& out_vectors = segment_ptr->vectors_ptr_->GetData();
//        ASSERT_EQ(raw_vectors.size(), out_vectors.size());
//        ASSERT_EQ(raw_vectors[0], out_vectors[0]);
    }

    status = db_->DropCollection(c1);
    ASSERT_TRUE(status.ok());
}
