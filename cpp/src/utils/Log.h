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

#pragma once

#include "Error.h"
#include "easylogging++.h"

namespace zilliz {
namespace milvus {
namespace server {

#define SERVER_DOMAIN_NAME "[SERVER] "
#define SERVER_ERROR_TEXT "SERVER Error:"

#define SERVER_LOG_TRACE LOG(TRACE) << SERVER_DOMAIN_NAME
#define SERVER_LOG_DEBUG LOG(DEBUG) << SERVER_DOMAIN_NAME
#define SERVER_LOG_INFO LOG(INFO) << SERVER_DOMAIN_NAME
#define SERVER_LOG_WARNING LOG(WARNING) << SERVER_DOMAIN_NAME
#define SERVER_LOG_ERROR LOG(ERROR) << SERVER_DOMAIN_NAME
#define SERVER_LOG_FATAL LOG(FATAL) << SERVER_DOMAIN_NAME

#define SERVER_ERROR(error)                             \
    ({                                                  \
        SERVER_LOG_ERROR << SERVER_ERROR_TEXT << error; \
        (error);                                        \
    })

#define SERVER_CHECK(func)                                  \
    {                                                       \
        zilliz::milvus::server::ServerError error = func;  \
        if (error != zilliz::milvus::server::SERVER_SUCCESS) { \
            return SERVER_ERROR(error);                     \
        }                                                   \
    }                                                       \

} // namespace sql
} // namespace zilliz
} // namespace server
