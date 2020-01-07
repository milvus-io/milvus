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

#include "easyloggingpp/easylogging++.h"

namespace milvus {

/////////////////////////////////////////////////////////////////////////////////////////////////
#define SERVER_DOMAIN_NAME "[SERVER] "

#define SERVER_LOG_TRACE LOG(TRACE) << SERVER_DOMAIN_NAME
#define SERVER_LOG_DEBUG LOG(DEBUG) << SERVER_DOMAIN_NAME
#define SERVER_LOG_INFO LOG(INFO) << SERVER_DOMAIN_NAME
#define SERVER_LOG_WARNING LOG(WARNING) << SERVER_DOMAIN_NAME
#define SERVER_LOG_ERROR LOG(ERROR) << SERVER_DOMAIN_NAME
#define SERVER_LOG_FATAL LOG(FATAL) << SERVER_DOMAIN_NAME

/////////////////////////////////////////////////////////////////////////////////////////////////
#define ENGINE_DOMAIN_NAME "[ENGINE] "

#define ENGINE_LOG_TRACE LOG(TRACE) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_DEBUG LOG(DEBUG) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_INFO LOG(INFO) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_WARNING LOG(WARNING) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_ERROR LOG(ERROR) << ENGINE_DOMAIN_NAME
#define ENGINE_LOG_FATAL LOG(FATAL) << ENGINE_DOMAIN_NAME

/////////////////////////////////////////////////////////////////////////////////////////////////
#define WRAPPER_DOMAIN_NAME "[WRAPPER] "

#define WRAPPER_LOG_TRACE LOG(TRACE) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_DEBUG LOG(DEBUG) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_INFO LOG(INFO) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_WARNING LOG(WARNING) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_ERROR LOG(ERROR) << WRAPPER_DOMAIN_NAME
#define WRAPPER_LOG_FATAL LOG(FATAL) << WRAPPER_DOMAIN_NAME

/////////////////////////////////////////////////////////////////////////////////////////////////
#define STORAGE_DOMAIN_NAME "[STORAGE] "

#define STORAGE_LOG_TRACE LOG(TRACE) << STORAGE_DOMAIN_NAME
#define STORAGE_LOG_DEBUG LOG(DEBUG) << STORAGE_DOMAIN_NAME
#define STORAGE_LOG_INFO LOG(INFO) << STORAGE_DOMAIN_NAME
#define STORAGE_LOG_WARNING LOG(WARNING) << STORAGE_DOMAIN_NAME
#define STORAGE_LOG_ERROR LOG(ERROR) << STORAGE_DOMAIN_NAME
#define STORAGE_LOG_FATAL LOG(FATAL) << STORAGE_DOMAIN_NAME

}  // namespace milvus
