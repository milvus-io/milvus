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

static const int trace_id_size = 2 * opentelemetry::trace::TraceId::kSize;

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
    } else if (config->exporter == "otlp") {
        auto opts = otlp::OtlpGrpcExporterOptions{};
        opts.endpoint = config->otlpEndpoint;
        exporter = otlp::OtlpGrpcExporterFactory::Create(opts);
    } else {
        LOG_SEGCORE_INFO_ << "Empty Trace";
    }
    auto processor =
        trace_sdk::BatchSpanProcessorFactory::Create(std::move(exporter), {});
    resource::ResourceAttributes attributes = {{"service.name", "segcore"},
                                               {"NodeID", config->nodeID}};
    auto resource = resource::Resource::Create(attributes);
    auto sampler = std::make_unique<trace_sdk::ParentBasedSampler>(
        std::make_shared<trace_sdk::AlwaysOnSampler>());
    std::shared_ptr<trace::TracerProvider> provider =
        trace_sdk::TracerProviderFactory::Create(
            std::move(processor), resource, std::move(sampler));
    trace::Provider::SetTracerProvider(provider);
}

std::shared_ptr<trace::Tracer>
GetTracer() {
    auto provider = trace::Provider::GetTracerProvider();
    return provider->GetTracer("segcore", OPENTELEMETRY_SDK_VERSION);
}

std::shared_ptr<trace::Span>
StartSpan(std::string name, TraceContext* parentCtx) {
    trace::StartSpanOptions opts;
    if (parentCtx != nullptr && parentCtx->traceID != nullptr &&
        parentCtx->spanID != nullptr) {
        opts.parent = trace::SpanContext(
            trace::TraceId({parentCtx->traceID, trace::TraceId::kSize}),
            trace::SpanId({parentCtx->spanID, trace::SpanId::kSize}),
            trace::TraceFlags(parentCtx->flag),
            true);
    }
    return GetTracer()->StartSpan(name, opts);
}

void
logTraceContext(const std::string& extended_info,
                const trace::SpanContext& ctx) {
    char traceID[trace_id_size];
    ctx.trace_id().ToLowerBase16(
        nostd::span<char, 2 * opentelemetry::trace::TraceId::kSize>{
            &traceID[0], trace_id_size});
    LOG_SEGCORE_DEBUG_ << extended_info << ":" << traceID;
}

}  // namespace milvus::tracer
