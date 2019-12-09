#pragma once

#include <grpcpp/server_context.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "tracing/TraceContext.h"

namespace milvus {
namespace server {

class Context {
 public:
    explicit Context(const std::string& request_id);

    std::shared_ptr<Context>
    Child(const std::string& operation_name) const;

    std::shared_ptr<Context>
    Follower(const std::string& operation_name) const;

    void
    SetTraceContext(const std::shared_ptr<tracing::TraceContext>& trace_context);

    const std::shared_ptr<tracing::TraceContext>&
    GetTraceContext() const;

 private:
    std::string request_id_;
    std::shared_ptr<tracing::TraceContext> trace_context_;
};

}  // namespace server
}  // namespace milvus