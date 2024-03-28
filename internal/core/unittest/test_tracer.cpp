
// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <memory>
#include <string.h>

#include "common/Tracer.h"
#include "common/EasyAssert.h"
#include "common/Tracer.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/config.h"

using namespace milvus;
using namespace milvus::tracer;
using namespace opentelemetry::trace;

TEST(Tracer, Init) {
    auto config = std::make_shared<TraceConfig>();
    config->exporter = "stdout";
    config->nodeID = 1;
    initTelemetry(*config);
    auto span = StartSpan("test");
    ASSERT_TRUE(span->IsRecording());

    config = std::make_shared<TraceConfig>();
    config->exporter = "jaeger";
    config->jaegerURL = "http://localhost:14268/api/traces";
    config->nodeID = 1;
    initTelemetry(*config);
    span = StartSpan("test");
    ASSERT_TRUE(span->IsRecording());
}

TEST(Tracer, Span) {
    auto config = std::make_shared<TraceConfig>();
    config->exporter = "stdout";
    config->nodeID = 1;
    initTelemetry(*config);

    const auto trace_id_vec = std::vector<uint8_t>({0x01,
                                                    0x23,
                                                    0x45,
                                                    0x67,
                                                    0x89,
                                                    0xab,
                                                    0xcd,
                                                    0xef,
                                                    0xfe,
                                                    0xdc,
                                                    0xba,
                                                    0x98,
                                                    0x76,
                                                    0x54,
                                                    0x32,
                                                    0x10});
    const auto span_id_vec =
        std::vector<uint8_t>({0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef});

    auto ctx = std::make_shared<TraceContext>();
    ctx->traceID = trace_id_vec.data();
    ctx->spanID = span_id_vec.data();
    ctx->traceFlags = 1;
    auto span = StartSpan("test", ctx.get());

    ASSERT_TRUE(span->GetContext().trace_id() ==
                trace::TraceId({ctx->traceID, 16}));
}

TEST(Tracer, Config) {
    const auto trace_id_vec = std::vector<uint8_t>({0x01,
                                                    0x23,
                                                    0x45,
                                                    0x67,
                                                    0x89,
                                                    0xab,
                                                    0xcd,
                                                    0xef,
                                                    0xfe,
                                                    0xdc,
                                                    0xba,
                                                    0x98,
                                                    0x76,
                                                    0x54,
                                                    0x32,
                                                    0x10});
    const auto span_id_vec =
        std::vector<uint8_t>({0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef});

    auto ctx = std::make_shared<TraceContext>();
    ctx->traceID = trace_id_vec.data();
    ctx->spanID = span_id_vec.data();
    ctx->traceFlags = 1;

    knowhere::Json search_cfg = {};

    // save trace context into search conf
    search_cfg[knowhere::meta::TRACE_ID] =
        tracer::GetTraceIDAsVector(ctx.get());
    search_cfg[knowhere::meta::SPAN_ID] = tracer::GetSpanIDAsVector(ctx.get());
    search_cfg[knowhere::meta::TRACE_FLAGS] = ctx->traceFlags;
    std::cout << "search config: " << search_cfg.dump() << std::endl;

    auto trace_id_cfg =
        search_cfg[knowhere::meta::TRACE_ID].get<std::vector<uint8_t>>();
    auto span_id_cfg =
        search_cfg[knowhere::meta::SPAN_ID].get<std::vector<uint8_t>>();

    ASSERT_TRUE(memcmp(ctx->traceID, trace_id_cfg.data(), 16) == 0);
    ASSERT_TRUE(memcmp(ctx->spanID, span_id_cfg.data(), 8) == 0);
}
