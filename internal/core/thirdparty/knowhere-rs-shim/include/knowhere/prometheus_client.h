#pragma once

#include <memory>
#include <string>
#include <utility>

namespace prometheus {

class Registry {};

}  // namespace prometheus

namespace knowhere {

class PrometheusClient {
 public:
    PrometheusClient() = default;
    PrometheusClient(const PrometheusClient&) = delete;
    PrometheusClient&
    operator=(const PrometheusClient&) = delete;

    prometheus::Registry&
    GetRegistry() {
        return registry_;
    }

    std::string
    GetMetrics() const {
        return {};
    }

 private:
    prometheus::Registry registry_;
};

inline std::unique_ptr<PrometheusClient> prometheusClient =
    std::make_unique<PrometheusClient>();

}  // namespace knowhere
