#pragma once

#include <opentracing/tracer.h>
#include <string>

class TraceContext {
 public:
    explicit TraceContext(std::unique_ptr<opentracing::SpanContext>& span_context);

    const std::unique_ptr<opentracing::SpanContext>&
    getSpanContext() const;

 private:
    std::unique_ptr<opentracing::SpanContext> span_context_;
};