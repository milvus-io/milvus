#pragma once

#include <string>

static const char* TRACER_LIBRARY_CONFIG_NAME = "tracer_library";
static const char* TRACER_CONFIGURATION_CONFIG_NAME = "tracer_configuration";
static const char* TRACE_CONTEXT_HEADER_CONFIG_NAME = "TraceContextHeaderName";

class TracerUtil {

 public:

    static void InitGlobal(const std::string& config_path = "");

    static const std::string& GetTraceContextHeaderName();

 private:

    static void LoadConfig(const std::string& config_path);

    static std::string tracer_context_header_name_;

};