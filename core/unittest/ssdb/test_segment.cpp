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

#include "ssdb/utils.h"
#include "db/SnapshotVisitor.h"
#include "db/Types.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/Resources.h"
#include "segment/SSSegmentReader.h"
#include "segment/SSSegmentWriter.h"
#include "segment/Types.h"

using SegmentVisitor = milvus::engine::SegmentVisitor;

namespace {
milvus::Status
CreateCollection(std::shared_ptr<SSDBImpl> db, const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
    context.collection = collection_schema;

    int64_t collection_id = 0;
    int64_t field_id = 0;
    /* field uid */
    auto uid_field = std::make_shared<Field>(milvus::engine::DEFAULT_UID_NAME, 0,
            milvus::engine::FieldType::UID, milvus::engine::snapshot::JEmpty, field_id);
    auto uid_field_element_blt = std::make_shared<FieldElement>(collection_id, field_id,
            milvus::engine::DEFAULT_BLOOM_FILTER_NAME, milvus::engine::FieldElementType::FET_BLOOM_FILTER);
    auto uid_field_element_del = std::make_shared<FieldElement>(collection_id, field_id,
            milvus::engine::DEFAULT_DELETED_DOCS_NAME, milvus::engine::FieldElementType::FET_DELETED_DOCS);

    field_id++;
    /* field vector */
    auto vector_field = std::make_shared<Field>("vector", 0, milvus::engine::FieldType::VECTOR_FLOAT,
            milvus::engine::snapshot::JEmpty, field_id);
    auto vector_field_element_index = std::make_shared<FieldElement>(collection_id, field_id,
            milvus::engine::DEFAULT_INDEX_NAME, milvus::engine::FieldElementType::FET_INDEX);

    context.fields_schema[uid_field] = {uid_field_element_blt, uid_field_element_del};
    context.fields_schema[vector_field] = {vector_field_element_index};

    return db->CreateCollection(context);
}
}  // namespace

TEST_F(SSSegmentTest, SegmentTest) {
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

    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    auto& partitions = ss->GetResources<Partition>();
    for (auto& kv : partitions) {
        ASSERT_TRUE(CreateSegment(ss, kv.first, next_lsn(), sf_context).ok());
    }

    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());

    std::vector<milvus::segment::doc_id_t> raw_uids = {123};
    std::vector<uint8_t> raw_vectors = {1, 2, 3, 4};
    auto& segments = ss->GetResources<Segment>();
    for (auto& kv : segments) {
        auto segment = kv.second;
        auto visitor = SegmentVisitor::Build(ss, segment->GetID());
        milvus::segment::SSSegmentWriter segment_writer(visitor);

        status = segment_writer.AddVectors("test", raw_vectors, raw_uids);
        ASSERT_TRUE(status.ok());

        // status = segment_writer.Serialize();
        ASSERT_TRUE(status.ok());
    }

    status = db_->DropCollection(c1);
    ASSERT_TRUE(status.ok());
}
