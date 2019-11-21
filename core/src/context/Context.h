#pragma once

#include <grpcpp/server_context.h>
#include <memory>
#include <string>
#include <unordered_map>

#include "tracing/TraceContext.h"

class Context {
 public:
    explicit Context(const std::string& request_id);

    void
    SetTraceContext(const std::shared_ptr<TraceContext>& trace_context);

    const std::shared_ptr<TraceContext>&
    GetTraceContext() const;

 private:
    std::string request_id_;
    std::shared_ptr<TraceContext> trace_context_;
};