#include "server/context/Context.h"

namespace milvus {
namespace server {

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
std::shared_ptr<Context>
Context::Child(const std::string& operation_name) const {
    auto new_context = std::make_shared<Context>(request_id_);
    new_context->SetTraceContext(trace_context_->Child(operation_name));
    return new_context;
}

std::shared_ptr<Context>
Context::Follower(const std::string& operation_name) const {
    auto new_context = std::make_shared<Context>(request_id_);
    new_context->SetTraceContext(trace_context_->Follower(operation_name));
    return new_context;
}

}  // namespace server
}  // namespace milvus
