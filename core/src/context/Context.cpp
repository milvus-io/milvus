#include "context/Context.h"

Context::Context(const std::string& request_id) : request_id_(request_id) {
}

const std::shared_ptr<TraceContext>&
Context::GetTraceContext() const {
    return trace_context_;
}

void
Context::SetTraceContext(const std::shared_ptr<TraceContext>& trace_context) {
    trace_context_ = trace_context;
}
