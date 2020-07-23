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

//#include "db/meta/SqliteMetaImpl.h"
#include "db/DBFactory.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/SSBuildIndexJob.h"
#include "scheduler/job/SSSearchJob.h"
#include "scheduler/resource/CpuResource.h"
#include "scheduler/tasklabel/BroadcastLabel.h"
#include "scheduler/task/SSBuildIndexTask.h"
#include "scheduler/task/SSSearchTask.h"
#include "scheduler/task/SSTestTask.h"

namespace milvus {
namespace scheduler {

TEST(SSTaskTest, INVALID_INDEX) {
    auto dummy_context = std::make_shared<milvus::server::Context>("dummy_request_id");
    opentracing::mocktracer::MockTracerOptions tracer_options;
    auto mock_tracer =
        std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
    auto mock_span = mock_tracer->StartSpan("mock_span");
    auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
    dummy_context->SetTraceContext(trace_context);
}

TEST(SSTaskTest, TEST_TASK) {
    auto dummy_context = std::make_shared<milvus::server::Context>("dummy_request_id");

//    auto file = std::make_shared<SegmentSchema>();
//    file->index_params_ = "{ \"nlist\": 16384 }";
//    file->dimension_ = 64;
    auto label = std::make_shared<BroadcastLabel>();

    SSTestTask task(dummy_context, nullptr, label);
    task.Load(LoadType::CPU2GPU, 0);
    auto th = std::thread([&]() {
        task.Execute();
    });
    task.Wait();

    if (th.joinable()) {
        th.join();
    }

//    static const char* CONFIG_PATH = "/tmp/milvus_test";
//    auto options = milvus::engine::DBFactory::BuildOption();
//    options.meta_.path_ = CONFIG_PATH;
//    options.meta_.backend_uri_ = "sqlite://:@:/";
//    options.insert_cache_immediately_ = true;
//
//    file->collection_id_ = "111";
//    file->location_ = "/tmp/milvus_test/index_file1.txt";
//    auto build_index_job = std::make_shared<milvus::scheduler::SSBuildIndexJob>(options);
//    XSSBuildIndexTask build_index_task(nullptr, label);
//    build_index_task.job_ = build_index_job;
//
//    build_index_task.Load(LoadType::TEST, 0);
//
//    fiu_init(0);
//    fiu_enable("XBuildIndexTask.Load.throw_std_exception", 1, NULL, 0);
//    build_index_task.Load(LoadType::TEST, 0);
//    fiu_disable("XBuildIndexTask.Load.throw_std_exception");
//
//    fiu_enable("XBuildIndexTask.Load.out_of_memory", 1, NULL, 0);
//    build_index_task.Load(LoadType::TEST, 0);
//    fiu_disable("XBuildIndexTask.Load.out_of_memory");
//
//    build_index_task.Execute();
//    // always enable 'create_table_success'
//    fiu_enable("XBuildIndexTask.Execute.create_table_success", 1, NULL, 0);
//
//    milvus::json json = {{"nlist", 16384}};
//    build_index_task.to_index_engine_ =
//        EngineFactory::Build(file->dimension_, file->location_, (EngineType)file->engine_type_,
//                             (MetricType)file->metric_type_, json);
//
//    build_index_task.Execute();
//
//    fiu_enable("XBuildIndexTask.Execute.build_index_fail", 1, NULL, 0);
//    build_index_task.to_index_engine_ =
//        EngineFactory::Build(file->dimension_, file->location_, (EngineType)file->engine_type_,
//                             (MetricType)file->metric_type_, json);
//    build_index_task.Execute();
//    fiu_disable("XBuildIndexTask.Execute.build_index_fail");
//
//    // always enable 'has_collection'
//    fiu_enable("XBuildIndexTask.Execute.has_collection", 1, NULL, 0);
//    build_index_task.to_index_engine_ =
//        EngineFactory::Build(file->dimension_, file->location_, (EngineType)file->engine_type_,
//                             (MetricType)file->metric_type_, json);
//    build_index_task.Execute();
//
//    fiu_enable("XBuildIndexTask.Execute.throw_std_exception", 1, NULL, 0);
//    build_index_task.to_index_engine_ =
//        EngineFactory::Build(file->dimension_, file->location_, (EngineType)file->engine_type_,
//                             (MetricType)file->metric_type_, json);
//    build_index_task.Execute();
//    fiu_disable("XBuildIndexTask.Execute.throw_std_exception");
//
//    fiu_enable("XBuildIndexTask.Execute.update_table_file_fail", 1, NULL, 0);
//    build_index_task.to_index_engine_ =
//        EngineFactory::Build(file->dimension_, file->location_, (EngineType)file->engine_type_,
//                             (MetricType)file->metric_type_, json);
//    build_index_task.Execute();
//    fiu_disable("XBuildIndexTask.Execute.update_table_file_fail");
//
//    fiu_disable("XBuildIndexTask.Execute.throw_std_exception");
//    fiu_disable("XBuildIndexTask.Execute.has_collection");
//    fiu_disable("XBuildIndexTask.Execute.create_table_success");
//    build_index_task.Execute();
//
//    // search task
//    engine::VectorsData vector;
//    auto search_job = std::make_shared<SearchJob>(dummy_context, 1, 1, vector);
//    file->metric_type_ = static_cast<int>(MetricType::IP);
//    file->engine_type_ = static_cast<int>(engine::EngineType::FAISS_IVFSQ8H);
//    opentracing::mocktracer::MockTracerOptions tracer_options;
//    auto mock_tracer =
//        std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
//    auto mock_span = mock_tracer->StartSpan("mock_span");
//    auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
//    dummy_context->SetTraceContext(trace_context);
//    XSearchTask search_task(dummy_context, file, label);
//    search_task.job_ = search_job;
//    std::string cpu_resouce_name = "cpu_name1";
//    std::vector<std::string> path = {cpu_resouce_name};
//    search_task.task_path_ = Path(path, 0);
//    ResMgrInst::GetInstance()->Add(std::make_shared<CpuResource>(cpu_resouce_name, 1, true));
//
//    search_task.Load(LoadType::CPU2GPU, 0);
//    search_task.Load(LoadType::GPU2CPU, 0);
//
//    fiu_enable("XSearchTask.Load.throw_std_exception", 1, NULL, 0);
//    search_task.Load(LoadType::GPU2CPU, 0);
//    fiu_disable("XSearchTask.Load.throw_std_exception");
//
//    fiu_enable("XSearchTask.Load.out_of_memory", 1, NULL, 0);
//    search_task.Load(LoadType::GPU2CPU, 0);
//    fiu_disable("XSearchTask.Load.out_of_memory");
//
//    fiu_enable("XSearchTask.Execute.search_fail", 1, NULL, 0);
//    search_task.Execute();
//    fiu_disable("XSearchTask.Execute.search_fail");
//
//    fiu_enable("XSearchTask.Execute.throw_std_exception", 1, NULL, 0);
//    search_task.Execute();
//    fiu_disable("XSearchTask.Execute.throw_std_exception");
//
//    search_task.Execute();
//
//    scheduler::ResultIds ids, tar_ids;
//    scheduler::ResultDistances distances, tar_distances;
//    XSearchTask::MergeTopkToResultSet(ids, distances, 1, 1, 1, true, tar_ids, tar_distances);
}

TEST(SSTaskTest, TEST_PATH) {
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
