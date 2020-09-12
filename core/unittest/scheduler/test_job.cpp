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

#include "scheduler/job/Job.h"
#include "scheduler/job/BuildIndexJob.h"
#include "scheduler/job/DeleteJob.h"
#include "scheduler/job/SearchJob.h"

namespace milvus {
namespace scheduler {
class TestJob : public Job {
 public:
    TestJob() : Job(JobType::INVALID) {}
};

TEST(JobTest, TestJob) {
    engine::DBOptions options;
    auto build_index_ptr = std::make_shared<BuildIndexJob>(nullptr, options);
    build_index_ptr->Dump();
    build_index_ptr->AddToIndexFiles(nullptr);

    TestJob test_job;
    test_job.Dump();

    auto delete_ptr = std::make_shared<DeleteJob>("collection_id", nullptr, 1);
    delete_ptr->Dump();

    engine::VectorsData vectors;
    auto search_ptr = std::make_shared<SearchJob>(nullptr, 1, 1, vectors);
    search_ptr->Dump();
    search_ptr->AddIndexFile(nullptr);
}

}  // namespace scheduler
}  // namespace milvus
