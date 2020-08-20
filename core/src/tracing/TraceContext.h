// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <opentracing/tracer.h>

#include <memory>
#include <string>

namespace milvus {
namespace tracing {

class TraceContext {
 public:
    explicit TraceContext(std::unique_ptr<opentracing::Span>& span);

    std::unique_ptr<TraceContext>
    Child(const std::string& operation_name) const;

    std::unique_ptr<TraceContext>
    Follower(const std::string& operation_name) const;

    const std::unique_ptr<opentracing::Span>&
    GetSpan() const;

 private:
    //    std::unique_ptr<opentracing::SpanContext> span_context_;
    std::unique_ptr<opentracing::Span> span_;
};

using TraceContextPtr = std::shared_ptr<TraceContext>;

}  // namespace tracing
}  // namespace milvus
