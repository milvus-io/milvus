#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "prometheus/collectable.h"
#include "prometheus/counter.h"
#include "prometheus/detail/counter_builder.h"
#include "prometheus/detail/future_std.h"
#include "prometheus/detail/gauge_builder.h"
#include "prometheus/detail/histogram_builder.h"
#include "prometheus/detail/summary_builder.h"
#include "prometheus/family.h"
#include "prometheus/gauge.h"
#include "prometheus/histogram.h"
#include "prometheus/metric_family.h"
#include "prometheus/summary.h"

namespace prometheus {

/// \brief Manages the collection of a number of metrics.
///
/// The Registry is responsible to expose data to a class/method/function
/// "bridge", which returns the metrics in a format Prometheus supports.
///
/// The key class is the Collectable. This has a method - called Collect() -
/// that returns zero or more metrics and their samples. The metrics are
/// represented by the class Family<>, which implements the Collectable
/// interface. A new metric is registered with BuildCounter(), BuildGauge(),
/// BuildHistogram() or BuildSummary().
///
/// The class is thread-safe. No concurrent call to any API of this type causes
/// a data race.
class Registry : public Collectable {
 public:
  /// \brief Returns a list of metrics and their samples.
  ///
  /// Every time the Registry is scraped it calls each of the metrics Collect
  /// function.
  ///
  /// \return Zero or more metrics and their samples.
  std::vector<MetricFamily> Collect() override;

 private:
  friend class detail::CounterBuilder;
  friend class detail::GaugeBuilder;
  friend class detail::HistogramBuilder;
  friend class detail::SummaryBuilder;

  template <typename T>
  Family<T>& Add(const std::string& name, const std::string& help,
                 const std::map<std::string, std::string>& labels);

  std::vector<std::unique_ptr<Collectable>> collectables_;
  std::mutex mutex_;
};

template <typename T>
Family<T>& Registry::Add(const std::string& name, const std::string& help,
                         const std::map<std::string, std::string>& labels) {
  std::lock_guard<std::mutex> lock{mutex_};
  auto family = detail::make_unique<Family<T>>(name, help, labels);
  auto& ref = *family;
  collectables_.push_back(std::move(family));
  return ref;
}

}  // namespace prometheus
