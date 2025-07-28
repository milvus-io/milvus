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

    auto ctx = std::make_shared<TraceContext>();
    ctx->traceID = new uint8_t[16]{0x01,
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
                                   0x10};
    ctx->spanID =
        new uint8_t[8]{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef};
    ctx->traceFlags = 1;
    auto span = StartSpan("test", ctx.get());

    ASSERT_TRUE(span->GetContext().trace_id() ==
                trace::TraceId({ctx->traceID, 16}));

    delete[] ctx->traceID;
    delete[] ctx->spanID;
}

TEST(Tracer, Hex) {
    auto ctx = std::make_shared<TraceContext>();
    ctx->traceID = new uint8_t[16]{0x01,
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
                                   0x10};
    ctx->spanID =
        new uint8_t[8]{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef};
    ctx->traceFlags = 1;

    knowhere::Json search_cfg = {};

    // save trace context into search conf
    search_cfg[knowhere::meta::TRACE_ID] =
        tracer::GetTraceIDAsHexStr(ctx.get());
    search_cfg[knowhere::meta::SPAN_ID] = tracer::GetSpanIDAsHexStr(ctx.get());
    search_cfg[knowhere::meta::TRACE_FLAGS] = ctx->traceFlags;
    std::cout << "search config: " << search_cfg.dump() << std::endl;

    auto trace_id_str = GetIDFromHexStr(search_cfg[knowhere::meta::TRACE_ID]);
    auto span_id_str = GetIDFromHexStr(search_cfg[knowhere::meta::SPAN_ID]);

    ASSERT_TRUE(strncmp((char*)ctx->traceID, trace_id_str.c_str(), 16) == 0);
    ASSERT_TRUE(strncmp((char*)ctx->spanID, span_id_str.c_str(), 8) == 0);

    delete[] ctx->traceID;
    delete[] ctx->spanID;
}

TEST(Tracer, GetTraceID) {
    auto trace_id = GetTraceID();
    ASSERT_TRUE(trace_id.empty());

    auto config = std::make_shared<TraceConfig>();
    config->exporter = "stdout";
    config->nodeID = 1;
    initTelemetry(*config);

    auto span = StartSpan("test");
    SetRootSpan(span);
    trace_id = GetTraceID();
    ASSERT_TRUE(trace_id.size() == 32);

    CloseRootSpan();
    trace_id = GetTraceID();
    ASSERT_TRUE(trace_id.empty());
}

TEST(Tracer, ParseHeaders) {
    // Test empty headers
    auto headers_map = parseHeaders("");
    ASSERT_TRUE(headers_map.empty());

    // Test simple JSON headers
    std::string json_headers =
        R"({"Authorization": "Bearer token123", "Content-Type": "application/json"})";
    headers_map = parseHeaders(json_headers);
    ASSERT_EQ(headers_map.size(), 2);
    ASSERT_EQ(headers_map["Authorization"], "Bearer token123");
    ASSERT_EQ(headers_map["Content-Type"], "application/json");

    // Test JSON with whitespace
    std::string json_headers_with_spaces =
        R"({ "key1" : "value1" , "key2" : "value2" })";
    headers_map = parseHeaders(json_headers_with_spaces);
    ASSERT_EQ(headers_map.size(), 2);
    ASSERT_EQ(headers_map["key1"], "value1");
    ASSERT_EQ(headers_map["key2"], "value2");

    // Test invalid JSON
    std::string invalid_json = "invalid json string";
    headers_map = parseHeaders(invalid_json);
    ASSERT_TRUE(headers_map.empty());

    // Test empty JSON object
    std::string empty_json = "{}";
    headers_map = parseHeaders(empty_json);
    ASSERT_TRUE(headers_map.empty());
}

TEST(Tracer, OTLPHttpExporter) {
    auto config = std::make_shared<TraceConfig>();
    config->exporter = "otlp";
    config->otlpMethod = "http";
    config->otlpEndpoint = "http://localhost:4318/v1/traces";
    config->otlpHeaders =
        R"({"Authorization": "Bearer test-token", "Content-Type": "application/json"})";
    config->nodeID = 1;

    initTelemetry(*config);
    auto span = StartSpan("test_otlp_http");
    ASSERT_TRUE(span->IsRecording());

    // Test with empty headers
    config->otlpHeaders = "";
    initTelemetry(*config);
    span = StartSpan("test_otlp_http_empty_headers");
    ASSERT_TRUE(span->IsRecording());

    // Test with invalid JSON headers
    config->otlpHeaders = "invalid json";
    initTelemetry(*config);
    span = StartSpan("test_otlp_http_invalid_headers");
    ASSERT_TRUE(span->IsRecording());
}

TEST(Tracer, OTLPGrpcExporter) {
    auto config = std::make_shared<TraceConfig>();
    config->exporter = "otlp";
    config->otlpMethod = "grpc";
    config->otlpEndpoint = "localhost:4317";
    config->otlpHeaders = R"({"Authorization": "Bearer grpc-token"})";
    config->oltpSecure = false;
    config->nodeID = 1;

    initTelemetry(*config);
    auto span = StartSpan("test_otlp_grpc");
    ASSERT_TRUE(span->IsRecording());

    // Test with secure connection
    config->oltpSecure = true;
    initTelemetry(*config);
    span = StartSpan("test_otlp_grpc_secure");
    ASSERT_TRUE(span->IsRecording());

    // Test with empty headers
    config->otlpHeaders = "";
    config->oltpSecure = false;
    initTelemetry(*config);
    span = StartSpan("test_otlp_grpc_empty_headers");
    ASSERT_TRUE(span->IsRecording());
}

TEST(Tracer, OTLPLegacyConfiguration) {
    auto config = std::make_shared<TraceConfig>();
    config->exporter = "otlp";
    config->otlpMethod = "";  // legacy configuration
    config->otlpEndpoint = "localhost:4317";
    config->otlpHeaders = R"({"legacy": "header"})";
    config->oltpSecure = false;
    config->nodeID = 1;

    initTelemetry(*config);
    auto span = StartSpan("test_otlp_legacy");
    ASSERT_TRUE(span->IsRecording());
}

TEST(Tracer, OTLPInvalidMethod) {
    auto config = std::make_shared<TraceConfig>();
    config->exporter = "otlp";
    config->otlpMethod = "invalid_method";
    config->otlpEndpoint = "localhost:4317";
    config->nodeID = 1;

    initTelemetry(*config);
    auto span = StartSpan("test_otlp_invalid");
    // Should fall back to noop provider when export creation fails
    ASSERT_FALSE(span->IsRecording());
}

TEST(Tracer, OTLPComplexHeaders) {
    auto config = std::make_shared<TraceConfig>();
    config->exporter = "otlp";
    config->otlpMethod = "http";
    config->otlpEndpoint = "http://localhost:4318/v1/traces";
    config->otlpHeaders = R"({
        "Authorization": "Bearer complex-token-123",
        "X-Custom-Header": "custom-value",
        "User-Agent": "Milvus-Tracer/1.0",
        "Accept": "application/json"
    })";
    config->nodeID = 1;

    initTelemetry(*config);
    auto span = StartSpan("test_otlp_complex_headers");
    ASSERT_TRUE(span->IsRecording());
}

TEST(Tracer, OTLPEmptyExporter) {
    auto config = std::make_shared<TraceConfig>();
    config->exporter = "";  // empty exporter
    config->nodeID = 1;

    initTelemetry(*config);
    auto span = StartSpan("test_empty_exporter");
    // Should fall back to noop provider
    ASSERT_FALSE(span->IsRecording());
}

TEST(Tracer, OTLPInvalidExporter) {
    auto config = std::make_shared<TraceConfig>();
    config->exporter = "invalid_exporter";
    config->nodeID = 1;

    initTelemetry(*config);
    auto span = StartSpan("test_invalid_exporter");
    // Should fall back to noop provider
    ASSERT_FALSE(span->IsRecording());
}

TEST(Tracer, OTLPHeadersParsingEdgeCases) {
    // Test with whitespace in JSON
    std::string json_with_spaces =
        R"({ "key1" : "value1" , "key2" : "value2" })";
    auto headers_map = parseHeaders(json_with_spaces);
    ASSERT_EQ(headers_map.size(), 2);
    ASSERT_EQ(headers_map["key1"], "value1");
    ASSERT_EQ(headers_map["key2"], "value2");

    // Test with nested JSON (should fail gracefully)
    std::string nested_json = R"({"key": {"nested": "value"}})";
    headers_map = parseHeaders(nested_json);
    ASSERT_TRUE(headers_map.empty());

    // Test with array JSON (should fail gracefully)
    std::string array_json = R"(["header1", "header2"])";
    headers_map = parseHeaders(array_json);
    ASSERT_TRUE(headers_map.empty());

    // Test with null JSON
    std::string null_json = "null";
    headers_map = parseHeaders(null_json);
    ASSERT_TRUE(headers_map.empty());
}
