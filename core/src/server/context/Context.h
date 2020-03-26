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

using ContextPtr = std::shared_ptr<milvus::server::Context>;

class ContextChild {
 public:
    explicit ContextChild(const ContextPtr& context, const std::string& operation_name);
    ~ContextChild();

    ContextPtr
    Context() {
        return context_;
    }

    void
    Finish();

 private:
    ContextPtr context_;
};

class ContextFollower {
 public:
    explicit ContextFollower(const ContextPtr& context, const std::string& operation_name);
    ~ContextFollower();

    ContextPtr
    Context() {
        return context_;
    }

    void
    Finish();

 private:
    ContextPtr context_;
};

}  // namespace server
}  // namespace milvus
