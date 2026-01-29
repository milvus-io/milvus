// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>
#include <utility>

#include "common/Tracer.h"
#include "common/type_c.h"

namespace milvus {
struct OpContext;
}

namespace milvus::tracer {

inline std::string&
RequestTraceIDStorage() {
    static thread_local std::string trace_id;
    return trace_id;
}

inline void
SetRequestTraceID(std::string trace_id) {
    RequestTraceIDStorage() = std::move(trace_id);
}

inline void
SetRequestTraceID(const TraceContext* ctx) {
    auto trace_id = GetTraceIDAsHexStr(ctx);
    if (!trace_id.empty()) {
        SetRequestTraceID(std::move(trace_id));
    }
}

inline std::string
GetRequestTraceID() {
    const auto& trace_id = RequestTraceIDStorage();
    if (!trace_id.empty()) {
        return trace_id;
    }
    return GetTraceID();
}

inline std::string
GetRequestTraceID(const TraceContext* ctx) {
    auto trace_id = GetTraceIDAsHexStr(ctx);
    if (!trace_id.empty()) {
        return trace_id;
    }
    return GetRequestTraceID();
}

inline std::string
GetRequestTraceID(const CTraceContext& c_trace) {
    if (c_trace.requestID != nullptr && c_trace.requestID[0] != '\0') {
        return c_trace.requestID;
    }
    auto trace_ctx = TraceContext{
        c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
    return GetTraceIDAsHexStr(&trace_ctx);
}

class ScopedRequestTraceID {
 public:
    explicit ScopedRequestTraceID(std::string trace_id)
        : previous_(RequestTraceIDStorage()) {
        SetRequestTraceID(std::move(trace_id));
    }

    explicit ScopedRequestTraceID(const TraceContext* ctx)
        : ScopedRequestTraceID(GetTraceIDAsHexStr(ctx)) {
    }

    ~ScopedRequestTraceID() {
        SetRequestTraceID(std::move(previous_));
    }

 private:
    std::string previous_;
};

inline std::string
GetRequestTraceID(const milvus::OpContext* /* op_ctx */) {
    return GetRequestTraceID();
}

class ScopedOpContextTraceID {
 public:
    ScopedOpContextTraceID(const milvus::OpContext* /* op_ctx */,
                           std::string trace_id)
        : scoped_trace_(std::move(trace_id)) {
    }

    ScopedOpContextTraceID(const milvus::OpContext* op_ctx,
                           const TraceContext* ctx)
        : ScopedOpContextTraceID(op_ctx, GetRequestTraceID(ctx)) {
    }

 private:
    ScopedRequestTraceID scoped_trace_;
};

}  // namespace milvus::tracer
