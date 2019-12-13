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

# pragma once

#include <string>

#include "oatpp/core/data/mapping/type/Object.hpp"
#include "oatpp/core/macro/codegen.hpp"

#include "server/web_impl/dto/ResultDto.hpp"

#include "server/delivery/RequestHandler.h"
#include "server/context/Context.h"

namespace milvus {
namespace server {
namespace web {

class WebHandler {
 public:
    WebHandler() = default;

    StatusDto::ObjectWrapper
    CreateTable(const std::string& table_name, int64_t dimension,
                int64_t index_file_size, int64_t metric_type);

    HasTableDto::ObjectWrapper
    hasTable(const std::string& tableName);

    WebHandler&
    RegisterRequestHandler(const RequestHandler& handler) {
        request_handler_ = handler;
    }

 private:
    // TODO: just for coding
    std::shared_ptr<Context> context_ptr_ = std::make_shared<Context>("CreateTable");
    RequestHandler request_handler_;
};

} // namespace web
} // namespace server
} // namespace milvus

