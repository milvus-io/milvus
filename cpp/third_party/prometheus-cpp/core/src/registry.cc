#include "prometheus/registry.h"

namespace prometheus {

std::vector<MetricFamily> Registry::Collect() {
  std::lock_guard<std::mutex> lock{mutex_};
  auto results = std::vector<MetricFamily>{};
  for (auto&& collectable : collectables_) {
    auto metrics = collectable->Collect();
    results.insert(results.end(), metrics.begin(), metrics.end());
  }

  return results;
}

}  // namespace prometheus
