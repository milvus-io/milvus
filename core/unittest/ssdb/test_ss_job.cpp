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

#include <gtest/gtest.h>

#include "db/SnapshotVisitor.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/SSBuildIndexJob.h"
#include "scheduler/job/SSSearchJob.h"
#include "ssdb/utils.h"

using SegmentVisitor = milvus::engine::SegmentVisitor;

namespace {
milvus::Status
CreateCollection(std::shared_ptr<SSDBImpl> db, const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
    context.collection = collection_schema;
    auto vector_field = std::make_shared<Field>("vector", 0,
                                                milvus::engine::FieldType::VECTOR);
    auto vector_field_element = std::make_shared<FieldElement>(0, 0, "ivfsq8",
                                                               milvus::engine::FieldElementType::FET_INDEX);
    auto int_field = std::make_shared<Field>("int", 0,
                                             milvus::engine::FieldType::INT32);
    context.fields_schema[vector_field] = {vector_field_element};
    context.fields_schema[int_field] = {};

    return db->CreateCollection(context);
}
}  // namespace

TEST_F(SSSchedulerTest, SSJobTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };

    std::string c1 = "c1";
    auto status = CreateCollection(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok());

    status = db_->CreatePartition(c1, "p_0");
    ASSERT_TRUE(status.ok());

    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());

    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    auto& partitions = ss->GetResources<Partition>();
    ASSERT_EQ(partitions.size(), 2);
    for (auto& kv : partitions) {
        int64_t row_cnt = 100;
        ASSERT_TRUE(CreateSegment(ss, kv.first, next_lsn(), sf_context, row_cnt).ok());
    }

    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());

    /* collect all valid segment */
    std::vector<milvus::engine::SegmentVisitorPtr> segment_visitors;
    auto executor = [&](const SegmentPtr& segment, SegmentIterator* handler) -> Status {
        auto visitor = SegmentVisitor::Build(ss, segment->GetID());
        if (visitor == nullptr) {
            return Status(milvus::SS_ERROR, "Cannot build segment visitor");
        }
        segment_visitors.push_back(visitor);
        return Status::OK();
    };

    auto segment_iter = std::make_shared<SegmentIterator>(ss, executor);
    segment_iter->Iterate();
    ASSERT_TRUE(segment_iter->GetStatus().ok());
    ASSERT_EQ(segment_visitors.size(), 2);

    /* create BuildIndexJob */
    milvus::scheduler::SSBuildIndexJobPtr build_index_job =
        std::make_shared<milvus::scheduler::SSBuildIndexJob>("");
    for (auto& sv : segment_visitors) {
        build_index_job->AddSegmentVisitor(sv);
    }

    /* put search job to scheduler and wait result */
    milvus::scheduler::JobMgrInst::GetInstance()->Put(build_index_job);
    build_index_job->WaitFinish();

    /* create SearchJob */
    milvus::scheduler::SSSearchJobPtr search_job =
        std::make_shared<milvus::scheduler::SSSearchJob>(nullptr, "", nullptr);
    for (auto& sv : segment_visitors) {
        search_job->AddSegmentVisitor(sv);
    }

    /* put search job to scheduler and wait result */
    milvus::scheduler::JobMgrInst::GetInstance()->Put(search_job);
    search_job->WaitFinish();
}
