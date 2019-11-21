#include "tracing/TraceContext.h"

TraceContext::TraceContext(std::unique_ptr<opentracing::Span>& span) : span_(std::move(span)) {
}

std::unique_ptr<TraceContext>
TraceContext::Child(const std::string& operation_name) const {
    auto child_span = span_->tracer().StartSpan(operation_name, {opentracing::ChildOf(&(span_->context()))});
    return std::make_unique<TraceContext>(child_span);
}

std::unique_ptr<TraceContext>
TraceContext::Follower(const std::string& operation_name) const {
    auto follower_span = span_->tracer().StartSpan(operation_name, {opentracing::FollowsFrom(&(span_->context()))});
    return std::make_unique<TraceContext>(follower_span);
}

const std::unique_ptr<opentracing::Span>&
TraceContext::getSpan() const {
    return span_;
}
