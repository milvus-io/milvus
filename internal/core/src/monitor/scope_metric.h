#pragma once

#include <chrono>
#include <string>

#define SCOPE_CGO_CALL_METRIC() \
    ::milvus::monitor::FuncScopeMetric _scope_metric(__func__)

namespace milvus::monitor {

class FuncScopeMetric {
 public:
    FuncScopeMetric(const char* f);

    ~FuncScopeMetric();

 private:
    std::string func_;
    std::chrono::high_resolution_clock::time_point start_;
};

}  // namespace milvus::monitor