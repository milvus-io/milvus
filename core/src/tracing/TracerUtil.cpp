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

#include "tracing/TracerUtil.h"

#include <opentracing/dynamic_load.h>
#include <opentracing/tracer.h>

#include <fstream>
#include <iostream>

#include "thirdparty/nlohmann/json.hpp"

namespace milvus {
namespace tracing {

const char* TracerUtil::tracer_context_header_name_;

void
TracerUtil::InitGlobal(const std::string& config_path) {
    if (!config_path.empty()) {
        LoadConfig(config_path);
    } else {
        tracer_context_header_name_ = "";
    }
}

void
TracerUtil::LoadConfig(const std::string& config_path) {
    // Parse JSON config
    std::ifstream tracer_config(config_path);
    if (!tracer_config.good()) {
        std::cerr << "Failed to open tracer config file " << config_path << ": " << std::strerror(errno) << std::endl;
        return;
    }
    using json = nlohmann::json;
    json tracer_config_json;
    tracer_config >> tracer_config_json;
    std::string tracing_shared_lib = tracer_config_json[TRACER_LIBRARY_CONFIG_NAME];
    std::string tracer_config_str = tracer_config_json[TRACER_CONFIGURATION_CONFIG_NAME].dump();
    tracer_context_header_name_ = tracer_config_json[TRACE_CONTEXT_HEADER_CONFIG_NAME].dump().c_str();

    // Load the tracer library.
    std::string error_message;
    auto handle_maybe = opentracing::DynamicallyLoadTracingLibrary(tracing_shared_lib.c_str(), error_message);
    if (!handle_maybe) {
        std::cerr << "Failed to load tracer library: " << error_message << std::endl;
        return;
    }

    // Construct a tracer.
    auto& tracer_factory = handle_maybe->tracer_factory();
    auto tracer_maybe = tracer_factory.MakeTracer(tracer_config_str.c_str(), error_message);
    if (!tracer_maybe) {
        std::cerr << "Failed to create tracer: " << error_message << std::endl;
        return;
    }
    auto& tracer = *tracer_maybe;

    opentracing::Tracer::InitGlobal(tracer);
}

std::string
TracerUtil::GetTraceContextHeaderName() {
    return tracer_context_header_name_;
}

}  // namespace tracing
}  // namespace milvus
