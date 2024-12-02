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

#include "Tracer.h"
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include "log/Log.h"

#include <atomic>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <utility>

#include "opentelemetry/exporters/jaeger/jaeger_exporter_factory.h"
#include "opentelemetry/exporters/ostream/span_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/samplers/always_on.h"
#include "opentelemetry/sdk/trace/samplers/parent.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/sdk/version/version.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_metadata.h"

namespace milvus::tracer {

namespace trace = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;

namespace trace_sdk = opentelemetry::sdk::trace;
namespace resource = opentelemetry::sdk::resource;
namespace jaeger = opentelemetry::exporter::jaeger;
namespace ostream = opentelemetry::exporter::trace;
namespace otlp = opentelemetry::exporter::otlp;

static std::atomic<bool> enable_trace = true;
static std::shared_ptr<trace::TracerProvider> noop_trace_provider =
    std::make_shared<opentelemetry::trace::NoopTracerProvider>();

void
initTelemetry(const TraceConfig& cfg) {
    bool export_created = true;
    std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> exporter;
    if (cfg.exporter == "stdout") {
        exporter = ostream::OStreamSpanExporterFactory::Create();
    } else if (cfg.exporter == "jaeger") {
        auto opts = jaeger::JaegerExporterOptions{};
        opts.transport_format = jaeger::TransportFormat::kThriftHttp;
        opts.endpoint = cfg.jaegerURL;
        exporter = jaeger::JaegerExporterFactory::Create(opts);
        LOG_INFO("init jaeger exporter, endpoint: {}", opts.endpoint);
    } else if (cfg.exporter == "otlp") {
        if (cfg.otlpMethod == "http") {
            auto opts = otlp::OtlpHttpExporterOptions{};
            opts.url = cfg.otlpEndpoint;
            exporter = otlp::OtlpHttpExporterFactory::Create(opts);
            LOG_INFO("init otlp http exporter, endpoint: {}", opts.url);
        } else if (cfg.otlpMethod == "grpc" ||
                   cfg.otlpMethod == "") {  // legacy configuration
            auto opts = otlp::OtlpGrpcExporterOptions{};
            opts.endpoint = cfg.otlpEndpoint;
            opts.use_ssl_credentials = cfg.oltpSecure;
            exporter = otlp::OtlpGrpcExporterFactory::Create(opts);
            LOG_INFO("init otlp grpc exporter, endpoint: {}", opts.endpoint);
        } else {
            LOG_INFO("unknown otlp exporter method: {}", cfg.otlpMethod);
            export_created = false;
        }
    } else {
        LOG_INFO("Empty Trace");
        export_created = false;
    }
    if (export_created) {
        auto processor = trace_sdk::BatchSpanProcessorFactory::Create(
            std::move(exporter), {});
        resource::ResourceAttributes attributes = {
            {"service.name", TRACE_SERVICE_SEGCORE}, {"NodeID", cfg.nodeID}};
        auto resource = resource::Resource::Create(attributes);
        auto sampler = std::make_unique<trace_sdk::ParentBasedSampler>(
            std::make_shared<trace_sdk::AlwaysOnSampler>());
        std::shared_ptr<trace::TracerProvider> provider =
            trace_sdk::TracerProviderFactory::Create(
                std::move(processor), resource, std::move(sampler));
        trace::Provider::SetTracerProvider(provider);
        enable_trace.store(true);
    } else {
        trace::Provider::SetTracerProvider(noop_trace_provider);
        enable_trace.store(false);
    }
}

std::shared_ptr<trace::Tracer>
GetTracer() {
    auto provider = trace::Provider::GetTracerProvider();
    return provider->GetTracer(TRACE_SERVICE_SEGCORE,
                               OPENTELEMETRY_SDK_VERSION);
}

std::shared_ptr<trace::Span>
StartSpan(const std::string& name, TraceContext* parentCtx) {
    trace::StartSpanOptions opts;
    if (enable_trace.load() && parentCtx != nullptr &&
        parentCtx->traceID != nullptr && parentCtx->spanID != nullptr) {
        if (EmptyTraceID(parentCtx) || EmptySpanID(parentCtx)) {
            return noop_trace_provider->GetTracer("noop")->StartSpan("noop");
        }
        opts.parent = trace::SpanContext(
            trace::TraceId({parentCtx->traceID, trace::TraceId::kSize}),
            trace::SpanId({parentCtx->spanID, trace::SpanId::kSize}),
            trace::TraceFlags(parentCtx->traceFlags),
            true);
    }
    return GetTracer()->StartSpan(name, opts);
}

std::shared_ptr<trace::Span>
StartSpan(const std::string& name, const std::shared_ptr<trace::Span>& span) {
    trace::StartSpanOptions opts;
    if (span != nullptr) {
        opts.parent = span->GetContext();
    }
    return GetTracer()->StartSpan(name, opts);
}

thread_local std::shared_ptr<trace::Span> local_span;

std::string
GetTraceID() {
    // !!! The span is not sent by the calling context, but saved in the thread local now.
    // So we cannot get the accurate trace id if the thread is switched to execute another task.
    // Because we don't use folly thread pool to yield multi task,
    // the trace id is almost accurate by now.
    // It's safe to access local_span without mutex, because the span is not sent to other threads.
    if (local_span == nullptr) {
        return std::string();
    }
    auto ctx = local_span->GetContext();
    auto trace_id = ctx.trace_id();
    // The span is noop if the trace is off, 00000000... will be returned,
    // so return "0" string directly to distinguish from the no local span.
    if (!trace_id.IsValid()) {
        return "0";
    }
    auto rep = trace_id.Id();
    return BytesToHexStr(rep.data(), rep.size());
}

void
SetRootSpan(std::shared_ptr<trace::Span> span) {
    if (enable_trace.load()) {
        local_span = std::move(span);
    }
}

std::shared_ptr<trace::Span>
GetRootSpan() {
    if (enable_trace) {
        return local_span;
    }
    return nullptr;
}

void
CloseRootSpan() {
    if (enable_trace.load()) {
        local_span = nullptr;
    }
}

void
AddEvent(const std::string& event_label) {
    if (enable_trace.load() && local_span != nullptr) {
        local_span->AddEvent(event_label);
    }
}

bool
isEmptyID(const uint8_t* id, int length) {
    if (id != nullptr) {
        for (int i = 0; i < length; i++) {
            if (id[i] != 0) {
                return false;
            }
        }
    }
    return true;
}

bool
EmptyTraceID(const TraceContext* ctx) {
    return isEmptyID(ctx->traceID, trace::TraceId::kSize);
}

bool
EmptySpanID(const TraceContext* ctx) {
    return isEmptyID(ctx->spanID, trace::SpanId::kSize);
}

std::string
BytesToHexStr(const uint8_t* data, const size_t len) {
    std::stringstream ss;
    for (size_t i = 0; i < len; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0')
           << static_cast<int>(data[i]);
    }
    return ss.str();
}

std::string
GetIDFromHexStr(const std::string& hexStr) {
    std::stringstream ss;
    for (size_t i = 0; i < hexStr.length(); i += 2) {
        std::string byteStr = hexStr.substr(i, 2);
        char byte = static_cast<char>(std::stoi(byteStr, nullptr, 16));
        ss << byte;
    }
    return ss.str();
}

std::string
GetTraceIDAsHexStr(const TraceContext* ctx) {
    if (ctx != nullptr && !EmptyTraceID(ctx)) {
        return BytesToHexStr(ctx->traceID,
                             opentelemetry::trace::TraceId::kSize);
    } else {
        return std::string();
    }
}

std::string
GetSpanIDAsHexStr(const TraceContext* ctx) {
    if (ctx != nullptr && !EmptySpanID(ctx)) {
        return BytesToHexStr(ctx->spanID, opentelemetry::trace::SpanId::kSize);
    } else {
        return std::string();
    }
}

AutoSpan::AutoSpan(const std::string& name,
                   TraceContext* ctx,
                   bool is_root_span)
    : is_root_span_(is_root_span) {
    span_ = StartSpan(name, ctx);
    if (is_root_span) {
        SetRootSpan(span_);
    }
}

AutoSpan::AutoSpan(const std::string& name,
                   const std::shared_ptr<trace::Span>& span)
    : is_root_span_(false) {
    span_ = StartSpan(name, span);
}

std::shared_ptr<trace::Span>
AutoSpan::GetSpan() {
    return span_;
}

AutoSpan::~AutoSpan() {
    if (span_ != nullptr) {
        span_->End();
    }
    if (is_root_span_) {
        CloseRootSpan();
    }
}

}  // namespace milvus::tracer
