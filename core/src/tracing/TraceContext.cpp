#include "tracing/TraceContext.h"

TraceContext::TraceContext(std::unique_ptr<opentracing::SpanContext>& span_context)
    : span_context_(std::move(span_context)) {
}

const std::unique_ptr<opentracing::SpanContext>&
TraceContext::getSpanContext() const {
    return span_context_;
}
