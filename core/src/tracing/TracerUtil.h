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

namespace milvus {
namespace tracing {

extern const char* TRACER_LIBRARY_CONFIG_NAME;
extern const char* TRACER_CONFIGURATION_CONFIG_NAME;
extern const char* TRACE_CONTEXT_HEADER_CONFIG_NAME;

class TracerUtil {
 public:
    static void
    InitGlobal(const std::string& config_path = "");

    static std::string
    GetTraceContextHeaderName();

 private:
    static void
    LoadConfig(const std::string& config_path);

    static const char* tracer_context_header_name_;
};

}  // namespace tracing
}  // namespace milvus
