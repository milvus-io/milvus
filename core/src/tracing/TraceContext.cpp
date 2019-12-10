// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "tracing/TraceContext.h"

#include <utility>

namespace milvus {
namespace tracing {

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
TraceContext::GetSpan() const {
    return span_;
}

}  // namespace tracing
}  // namespace milvus
