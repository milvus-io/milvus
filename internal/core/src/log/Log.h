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

#include <string>
#include <sys/types.h>
#include <unistd.h>
#include "easyloggingpp/easylogging++.h"

namespace milvus {

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
#define VAR_THREAD_START_TIMESTAMP (get_thread_start_timestamp())
#define VAR_COMMAND_TAG (context->command_tag())

// Use this macro whenever possible
// Depends variables: context Context
#define MLOG(level, module, error_code)                                                                                \
    LOG(level) << " | " << VAR_REQUEST_ID << " | " << #level << " | " << VAR_COLLECTION_NAME << " | " << VAR_CLIENT_ID \
               << " | " << VAR_CLIENT_TAG << " | " << VAR_CLIENT_IPPORT << " | " << VAR_THREAD_ID << " | "             \
               << VAR_THREAD_START_TIMESTAMP << " | " << VAR_COMMAND_TAG << " | " << #module << " | " << error_code    \
               << " | "

// Use in some background process only
#define MLOG_(level, module, error_code)                                                 \
    LOG(level) << " | "                                                                  \
               << ""                                                                     \
               << " | " << #level << " | "                                               \
               << ""                                                                     \
               << " | "                                                                  \
               << ""                                                                     \
               << " | "                                                                  \
               << ""                                                                     \
               << " | "                                                                  \
               << ""                                                                     \
               << " | " << VAR_THREAD_ID << " | " << VAR_THREAD_START_TIMESTAMP << " | " \
               << ""                                                                     \
               << " | " << #module << " | " << error_code << " | "

/////////////////////////////////////////////////////////////////////////////////////////////////
#define SEGCORE_MODULE_NAME "SEGCORE"
#define SEGCORE_MODULE_CLASS_FUNCTION \
    LogOut("[%s][%s::%s][%s] ", SEGCORE_MODULE_NAME, (typeid(*this).name()), __FUNCTION__, GetThreadName().c_str())
#define SEGCORE_MODULE_FUNCTION LogOut("[%s][%s][%s] ", SEGCORE_MODULE_NAME, __FUNCTION__, GetThreadName().c_str())

#define LOG_SEGCORE_TRACE_C LOG(TRACE) << SEGCORE_MODULE_CLASS_FUNCTION
#define LOG_SEGCORE_DEBUG_C LOG(DEBUG) << SEGCORE_MODULE_CLASS_FUNCTION
#define LOG_SEGCORE_INFO_C LOG(INFO) << SEGCORE_MODULE_CLASS_FUNCTION
#define LOG_SEGCORE_WARNING_C LOG(WARNING) << SEGCORE_MODULE_CLASS_FUNCTION
#define LOG_SEGCORE_ERROR_C LOG(ERROR) << SEGCORE_MODULE_CLASS_FUNCTION
#define LOG_SEGCORE_FATAL_C LOG(FATAL) << SEGCORE_MODULE_CLASS_FUNCTION

#define LOG_SEGCORE_TRACE_ LOG(TRACE) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_DEBUG_ LOG(DEBUG) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_INFO_ LOG(INFO) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_WARNING_ LOG(WARNING) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_ERROR_ LOG(ERROR) << SEGCORE_MODULE_FUNCTION
#define LOG_SEGCORE_FATAL_ LOG(FATAL) << SEGCORE_MODULE_FUNCTION

/////////////////////////////////////////////////////////////////////////////////////////////////
#define KNOWHERE_MODULE_NAME "KNOWHERE"
#define KNOWHERE_MODULE_CLASS_FUNCTION \
    LogOut("[%s][%s::%s][%s] ", KNOWHERE_MODULE_NAME, (typeid(*this).name()), __FUNCTION__, GetThreadName().c_str())
#define KNOWHERE_MODULE_FUNCTION LogOut("[%s][%s][%s] ", KNOWHERE_MODULE_NAME, __FUNCTION__, GetThreadName().c_str())

#define LOG_KNOWHERE_TRACE_C LOG(TRACE) << KNOWHERE_MODULE_CLASS_FUNCTION
#define LOG_KNOWHERE_DEBUG_C LOG(DEBUG) << KNOWHERE_MODULE_CLASS_FUNCTION
#define LOG_KNOWHERE_INFO_C LOG(INFO) << KNOWHERE_MODULE_CLASS_FUNCTION
#define LOG_KNOWHERE_WARNING_C LOG(WARNING) << KNOWHERE_MODULE_CLASS_FUNCTION
#define LOG_KNOWHERE_ERROR_C LOG(ERROR) << KNOWHERE_MODULE_CLASS_FUNCTION
#define LOG_KNOWHERE_FATAL_C LOG(FATAL) << KNOWHERE_MODULE_CLASS_FUNCTION

#define LOG_KNOWHERE_TRACE_ LOG(TRACE) << KNOWHERE_MODULE_FUNCTION
#define LOG_KNOWHERE_DEBUG_ LOG(DEBUG) << KNOWHERE_MODULE_FUNCTION
#define LOG_KNOWHERE_INFO_ LOG(INFO) << KNOWHERE_MODULE_FUNCTION
#define LOG_KNOWHERE_WARNING_ LOG(WARNING) << KNOWHERE_MODULE_FUNCTION
#define LOG_KNOWHERE_ERROR_ LOG(ERROR) << KNOWHERE_MODULE_FUNCTION
#define LOG_KNOWHERE_FATAL_ LOG(FATAL) << KNOWHERE_MODULE_FUNCTION

/////////////////////////////////////////////////////////////////////////////////////////////////
#define SERVER_MODULE_NAME "SERVER"
#define SERVER_MODULE_CLASS_FUNCTION \
    LogOut("[%s][%s::%s][%s] ", SERVER_MODULE_NAME, (typeid(*this).name()), __FUNCTION__, GetThreadName().c_str())
#define SERVER_MODULE_FUNCTION LogOut("[%s][%s][%s] ", SERVER_MODULE_NAME, __FUNCTION__, GetThreadName().c_str())

#define LOG_SERVER_TRACE_C LOG(TRACE) << SERVER_MODULE_CLASS_FUNCTION
#define LOG_SERVER_DEBUG_C LOG(DEBUG) << SERVER_MODULE_CLASS_FUNCTION
#define LOG_SERVER_INFO_C LOG(INFO) << SERVER_MODULE_CLASS_FUNCTION
#define LOG_SERVER_WARNING_C LOG(WARNING) << SERVER_MODULE_CLASS_FUNCTION
#define LOG_SERVER_ERROR_C LOG(ERROR) << SERVER_MODULE_CLASS_FUNCTION
#define LOG_SERVER_FATAL_C LOG(FATAL) << SERVER_MODULE_CLASS_FUNCTION

#define LOG_SERVER_TRACE_ LOG(TRACE) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_DEBUG_ LOG(DEBUG) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_INFO_ LOG(INFO) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_WARNING_ LOG(WARNING) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_ERROR_ LOG(ERROR) << SERVER_MODULE_FUNCTION
#define LOG_SERVER_FATAL_ LOG(FATAL) << SERVER_MODULE_FUNCTION

/////////////////////////////////////////////////////////////////////////////////////////////////

std::string
LogOut(const char* pattern, ...);

void
SetThreadName(const std::string& name);

std::string
GetThreadName();

int64_t
get_thread_start_timestamp();

}  // namespace milvus
