#include <chrono>
#include <string>
#include <iostream>
#include "log/Log.h"
#include "Monitor.h"
#include "scope_metric.h"

namespace milvus::monitor {

const prometheus::Histogram::BucketBoundaries cgoCallDurationbuckets = {
    std::chrono::duration<float>(std::chrono::microseconds(10)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(50)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(100)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(500)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(1)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(5)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(10)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(50)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(100)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(500)).count(),
    std::chrono::duration<float>(std::chrono::seconds(1)).count(),
    std::chrono::duration<float>(std::chrono::seconds(2)).count(),
    std::chrono::duration<float>(std::chrono::seconds(5)).count(),
    std::chrono::duration<float>(std::chrono::seconds(10)).count(),
};

// One histogram per function name (label)
static inline prometheus::Histogram&
GetHistogram(std::string&& func) {
    static auto& hist_family =
        prometheus::BuildHistogram()
            .Name("milvus_cgocall_duration_seconds")
            .Help("Duration of cgo-exposed functions")
            .Register(getPrometheusClient().GetRegistry());

    // default buckets: [0.005, 0.01, ..., 1.0]
    return hist_family.Add({{"func", func}}, cgoCallDurationbuckets);
}

FuncScopeMetric::FuncScopeMetric(const char* f)
    : func_(f), start_(std::chrono::high_resolution_clock::now()) {
}

FuncScopeMetric::~FuncScopeMetric() {
    auto end = std::chrono::high_resolution_clock::now();
    double duration_sec = std::chrono::duration<double>(end - start_).count();

    if (duration_sec > 1.0) {
        LOG_INFO("[CGO Call] slow function {} done with duration {}s",
                 func_,
                 duration_sec);
    }
    // record prometheus metric
    auto& hist = GetHistogram(std::move(func_));
    hist.Observe(duration_sec);
}
}  // namespace milvus::monitor