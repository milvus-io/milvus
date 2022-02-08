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

#include "easyloggingpp/easylogging++.h"

namespace milvus {
namespace knowhere {

std::string
LogOut(const char* pattern, ...);

void
SetThreadName(const std::string& name);

std::string
GetThreadName();

void
log_trace_(const std::string&);

void
log_debug_(const std::string&);

void
log_info_(const std::string&);

void
log_warning_(const std::string&);

void
log_error_(const std::string&);

void
log_fatal_(const std::string&);

/*
 * Please use LOG_MODULE_LEVEL_C macro in member function of class
 * and LOG_MODULE_LEVEL_ macro in other functions.
 */

/////////////////////////////////////////////////////////////////////////////////////////////////
#define KNOWHERE_MODULE_NAME "KNOWHERE"
#define KNOWHERE_MODULE_CLASS_FUNCTION                                                                        \
    milvus::knowhere::LogOut("[%s][%s::%s][%s] ", KNOWHERE_MODULE_NAME, (typeid(*this).name()), __FUNCTION__, \
                             milvus::knowhere::GetThreadName().c_str())
#define KNOWHERE_MODULE_FUNCTION                                                  \
    milvus::knowhere::LogOut("[%s][%s][%s] ", KNOWHERE_MODULE_NAME, __FUNCTION__, \
                             milvus::knowhere::GetThreadName().c_str())

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

}  // namespace knowhere
}  // namespace milvus
