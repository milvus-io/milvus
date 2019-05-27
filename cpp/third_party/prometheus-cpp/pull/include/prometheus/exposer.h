#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "prometheus/collectable.h"
#include "prometheus/registry.h"

class CivetServer;

namespace prometheus {

namespace detail {
class MetricsHandler;
}  // namespace detail

class Exposer {
 public:
  explicit Exposer(const std::string& bind_address,
                   const std::string& uri = std::string("/metrics"),
                   const std::size_t num_threads = 2);
  ~Exposer();
  void RegisterCollectable(const std::weak_ptr<Collectable>& collectable);

 private:
  std::unique_ptr<CivetServer> server_;
  std::vector<std::weak_ptr<Collectable>> collectables_;
  std::shared_ptr<Registry> exposer_registry_;
  std::unique_ptr<detail::MetricsHandler> metrics_handler_;
  std::string uri_;
};

}  // namespace prometheus
