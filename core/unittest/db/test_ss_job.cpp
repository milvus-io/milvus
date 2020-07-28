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

#include "scheduler/job/BuildIndexJob.h"
#include "scheduler/job/SearchJob.h"

namespace milvus {
namespace scheduler {

class TestJob : public Job {
 public:
    TestJob() : Job(JobType::INVALID) {}
};

TEST(JobTest, TestJob) {
//    engine::DBOptions options;
//    auto build_index_ptr = std::make_shared<SSBuildIndexJob>(options);
//    build_index_ptr->Dump();
//    build_index_ptr->AddSegmentVisitor(nullptr);
//
//    TestJob test_job;
//    test_job.Dump();
//
//    /* collect all valid segment */
//    std::vector<milvus::engine::SegmentVisitorPtr> segment_visitors;
//    auto executor = [&](const SegmentPtr& segment, SegmentIterator* handler) -> Status {
//        auto visitor = SegmentVisitor::Build(ss, segment->GetID());
//        if (visitor == nullptr) {
//            return Status(milvus::SS_ERROR, "Cannot build segment visitor");
//        }
//        segment_visitors.push_back(visitor);
//        return Status::OK();
//    };
//
//    auto segment_iter = std::make_shared<SegmentIterator>(ss, executor);
//    segment_iter->Iterate();
//    ASSERT_TRUE(segment_iter->GetStatus().ok());
//    ASSERT_EQ(segment_visitors.size(), 2);

    /* create BuildIndexJob */
//    milvus::scheduler::BuildIndexJobPtr build_index_job =
//        std::make_shared<milvus::scheduler::SSBuildIndexJob>("");
//    for (auto& sv : segment_visitors) {
//        build_index_job->AddSegmentVisitor(sv);
//    }

    /* put search job to scheduler and wait result */
//    milvus::scheduler::JobMgrInst::GetInstance()->Put(build_index_job);
//    build_index_job->WaitFinish();

//    /* create SearchJob */
//    milvus::scheduler::SearchJobPtr search_job =
//        std::make_shared<milvus::scheduler::SSSearchJob>(nullptr, "", nullptr);
//    for (auto& sv : segment_visitors) {
//        search_job->AddSegmentVisitor(sv);
//    }
//
//    /* put search job to scheduler and wait result */
//    milvus::scheduler::JobMgrInst::GetInstance()->Put(search_job);
//    search_job->WaitFinish();
}

}  // namespace scheduler
}  // namespace milvus
