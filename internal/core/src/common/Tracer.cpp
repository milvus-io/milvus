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
#include "log/Log.h"
#include "Tracer.h"

#include <utility>

#include "opentelemetry/exporters/ostream/span_exporter_factory.h"
#include "opentelemetry/exporters/jaeger/jaeger_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h"
#include "opentelemetry/sdk/trace/samplers/always_on.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/samplers/parent.h"
#include "opentelemetry/sdk/resource/resource.h"
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

static bool enable_trace = true;
static std::shared_ptr<trace::TracerProvider> noop_trace_provider =
    std::make_shared<opentelemetry::trace::NoopTracerProvider>();

void
initTelementry(TraceConfig* config) {
    std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> exporter;
    if (config->exporter == "stdout") {
        exporter = ostream::OStreamSpanExporterFactory::Create();
    } else if (config->exporter == "jaeger") {
        auto opts = jaeger::JaegerExporterOptions{};
        opts.transport_format = jaeger::TransportFormat::kThriftHttp;
        opts.endpoint = config->jaegerURL;
        exporter = jaeger::JaegerExporterFactory::Create(opts);
        LOG_INFO("init jaeger exporter, endpoint:", opts.endpoint);
    } else if (config->exporter == "otlp") {
        auto opts = otlp::OtlpGrpcExporterOptions{};
        opts.endpoint = config->otlpEndpoint;
        opts.use_ssl_credentials = config->oltpSecure;
        exporter = otlp::OtlpGrpcExporterFactory::Create(opts);
        LOG_INFO("init otlp exporter, endpoint:", opts.endpoint);
    } else {
        LOG_INFO("Empty Trace");
        enable_trace = false;
    }
    if (enable_trace) {
        auto processor = trace_sdk::BatchSpanProcessorFactory::Create(
            std::move(exporter), {});
        resource::ResourceAttributes attributes = {{"service.name", "segcore"},
                                                   {"NodeID", config->nodeID}};
        auto resource = resource::Resource::Create(attributes);
        auto sampler = std::make_unique<trace_sdk::ParentBasedSampler>(
            std::make_shared<trace_sdk::AlwaysOnSampler>());
        std::shared_ptr<trace::TracerProvider> provider =
            trace_sdk::TracerProviderFactory::Create(
                std::move(processor), resource, std::move(sampler));
        trace::Provider::SetTracerProvider(provider);
    } else {
        trace::Provider::SetTracerProvider(noop_trace_provider);
    }
}

std::shared_ptr<trace::Tracer>
GetTracer() {
    auto provider = trace::Provider::GetTracerProvider();
    return provider->GetTracer("segcore", OPENTELEMETRY_SDK_VERSION);
}

std::shared_ptr<trace::Span>
StartSpan(const std::string& name, TraceContext* parentCtx) {
    trace::StartSpanOptions opts;
    if (enable_trace && parentCtx != nullptr && parentCtx->traceID != nullptr &&
        parentCtx->spanID != nullptr) {
        if (isEmptyID(parentCtx->traceID, trace::TraceId::kSize) ||
            isEmptyID(parentCtx->spanID, trace::SpanId::kSize)) {
            return noop_trace_provider->GetTracer("noop")->StartSpan("noop");
        }
        opts.parent = trace::SpanContext(
            trace::TraceId({parentCtx->traceID, trace::TraceId::kSize}),
            trace::SpanId({parentCtx->spanID, trace::SpanId::kSize}),
            trace::TraceFlags(parentCtx->flag),
            true);
    }
    return GetTracer()->StartSpan(name, opts);
}

thread_local std::shared_ptr<trace::Span> local_span;
void
SetRootSpan(std::shared_ptr<trace::Span> span) {
    if (enable_trace) {
        local_span = std::move(span);
    }
}

void
CloseRootSpan() {
    if (enable_trace) {
        local_span = nullptr;
    }
}

void
AddEvent(std::string event_label) {
    if (enable_trace && local_span != nullptr) {
        local_span->AddEvent(event_label);
    }
}

bool
isEmptyID(const uint8_t* id, int length) {
    for (int i = 0; i < length; i++) {
        if (id[i] != 0) {
            return false;
        }
    }
    return true;
}

}  // namespace milvus::tracer
