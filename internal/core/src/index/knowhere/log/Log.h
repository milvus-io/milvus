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

#include "log/DeprecatedLog.h"

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

std::string
LogOut(const char* pattern, ...);

void
SetThreadName(const std::string& name);

std::string
GetThreadName();

int64_t
get_thread_start_timestamp();

}  // namespace milvus
