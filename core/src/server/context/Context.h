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

#include <grpcpp/server_context.h>

#include "server/context/ConnectionContext.h"
#include "server/delivery/request/Types.h"
#include "tracing/TraceContext.h"

namespace milvus {
namespace server {

class RequestContext {
 public:
    std::string&
    request_id() {
        return request_id_;
    }

    std::string&
    collection_name() {
        return collection_name_;
    }

    std::string&
    client_tag() {
        return client_tag_;
    }

    std::string&
    client_ipport() {
        return client_ipport_;
    }

    std::string&
    command_tag() {
        return command_tag_;
    }

 private:
    std::string request_id_;
    std::string collection_name_;
    std::string client_tag_;
    std::string client_ipport_;
    std::string command_tag_;
};

class Context : public RequestContext {
 public:
    explicit Context(std::string request_id);

    inline std::string
    ReqID() const {
        return req_id_;
    }

    std::shared_ptr<Context>
    Child(const std::string& operation_name) const;

    std::shared_ptr<Context>
    Follower(const std::string& operation_name) const;

    void
    SetTraceContext(const tracing::TraceContextPtr& trace_context);

    const tracing::TraceContextPtr&
    GetTraceContext() const;

    void
    SetConnectionContext(ConnectionContextPtr& context);

    bool
    IsConnectionBroken() const;

    ReqType
    GetReqType() const;

    void
    SetReqType(ReqType type);

 private:
    std::string req_id_;
    ReqType req_type_;
    tracing::TraceContextPtr trace_context_;
    ConnectionContextPtr context_;
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
