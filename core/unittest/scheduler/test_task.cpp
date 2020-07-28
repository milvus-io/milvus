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

#include <fiu-local.h>
#include <fiu-control.h>
#include <gtest/gtest.h>
#include <opentracing/mocktracer/tracer.h>
#include <src/scheduler/SchedInst.h>
#include <src/scheduler/resource/CpuResource.h>

#include "db/DBFactory.h"
#include "scheduler/tasklabel/BroadcastLabel.h"
#include "scheduler/task/BuildIndexTask.h"
#include "scheduler/task/SearchTask.h"
#include "scheduler/task/TestTask.h"

namespace milvus {
namespace scheduler {

TEST(TaskTest, INVALID_INDEX) {
    auto dummy_context = std::make_shared<milvus::server::Context>("dummy_request_id");
    opentracing::mocktracer::MockTracerOptions tracer_options;
    auto mock_tracer =
        std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
    auto mock_span = mock_tracer->StartSpan("mock_span");
    auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
    dummy_context->SetTraceContext(trace_context);

    milvus::engine::DBOptions options;
    auto search_task =
        std::make_shared<SearchTask>(dummy_context, options, nullptr, 0, nullptr);
    search_task->Load(LoadType::TEST, 10);

    auto build_task = std::make_shared<BuildIndexTask>(options, "", 0, nullptr);
    build_task->Load(LoadType::TEST, 10);

    build_task->Execute();
}

TEST(TaskTest, TEST_PATH) {
    Path path;
    auto empty_path = path.Current();
    ASSERT_TRUE(empty_path.empty());
    empty_path = path.Next();
    ASSERT_TRUE(empty_path.empty());
    empty_path = path.Last();
    ASSERT_TRUE(empty_path.empty());
}

}  // namespace scheduler
}  // namespace milvus
