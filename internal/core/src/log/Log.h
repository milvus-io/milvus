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
#include <sys/types.h>
#include <unistd.h>
#include "glog/logging.h"

// namespace milvus {

#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif

// Log message format: %timestamp | %request_id | %level | %collection_name | %client_id | %client_tag | %client_ipport
// | %thread_id | %thread_start_timestamp | %command_tag | %module | %error_code | %message

#define VAR_REQUEST_ID (context->request_id())
#define VAR_COLLECTION_NAME (context->collection_name())
#define VAR_CLIENT_ID ("")
#define VAR_CLIENT_TAG (context->client_tag())
#define VAR_CLIENT_IPPORT (context->client_ipport())
#define VAR_THREAD_ID (gettid())
#define VAR_COMMAND_TAG (context->command_tag())

/////////////////////////////////////////////////////////////////////////////////////////////////
#define SEGCORE_MODULE_NAME "SEGCORE"
#define SEGCORE_MODULE_CLASS_FUNCTION \
    LogOut("[%s][%s::%s][%s] ",       \
           SEGCORE_MODULE_NAME,       \
           (typeid(*this).name()),    \
           __FUNCTION__,              \
           GetThreadName().c_str())
#define SEGCORE_MODULE_FUNCTION \
    LogOut("[%s][%s][%s] ",     \
           SEGCORE_MODULE_NAME, \
           __FUNCTION__,        \
           GetThreadName().c_str())

#define LOG_SEGCORE_TRACE_ DLOG(INFO) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_DEBUG_ DLOG(INFO) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_INFO_ LOG(INFO) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_WARNING_ LOG(WARNING) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_ERROR_ LOG(ERROR) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_FATAL_ LOG(FATAL) << SEGCORE_MODULE_FUNCTION

/////////////////////////////////////////////////////////////////////////////////////////////////
#define SERVER_MODULE_NAME "SERVER"
#define SERVER_MODULE_CLASS_FUNCTION \
    LogOut("[%s][%s::%s][%s] ",      \
           SERVER_MODULE_NAME,       \
           (typeid(*this).name()),   \
           __FUNCTION__,             \
           GetThreadName().c_str())
#define SERVER_MODULE_FUNCTION \
    LogOut("[%s][%s][%s] ",    \
           SERVER_MODULE_NAME, \
           __FUNCTION__,       \
           GetThreadName().c_str())

#define LOG_SERVER_TRACE_ DLOG(INFO) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_DEBUG_ DLOG(INFO) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_INFO_ LOG(INFO) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_WARNING_ LOG(WARNING) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_ERROR_ LOG(ERROR) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_FATAL_ LOG(FATAL) << SERVER_MODULE_FUNCTION

/////////////////////////////////////////////////////////////////////////////////////////////////

std::string
LogOut(const char* pattern, ...);

void
SetThreadName(const std::string_view name);

std::string
GetThreadName();

// }  // namespace milvus
