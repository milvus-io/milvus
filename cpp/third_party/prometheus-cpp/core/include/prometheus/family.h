#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "prometheus/check_names.h"
#include "prometheus/client_metric.h"
#include "prometheus/collectable.h"
#include "prometheus/detail/future_std.h"
#include "prometheus/detail/utils.h"
#include "prometheus/metric_family.h"

namespace prometheus {

/// \brief A metric of type T with a set of labeled dimensions.
///
/// One of Prometheus main feature is a multi-dimensional data model with time
/// series data identified by metric name and key/value pairs, also known as
/// labels. A time series is a series of data points indexed (or listed or
/// graphed) in time order (https://en.wikipedia.org/wiki/Time_series).
///
/// An instance of this class is exposed as multiple time series during
/// scrape, i.e., one time series for each set of labels provided to Add().
///
/// For example it is possible to collect data for a metric
/// `http_requests_total`, with two time series:
///
/// - all HTTP requests that used the method POST
/// - all HTTP requests that used the method GET
///
/// The metric name specifies the general feature of a system that is
/// measured, e.g., `http_requests_total`. Labels enable Prometheus's
/// dimensional data model: any given combination of labels for the same
/// metric name identifies a particular dimensional instantiation of that
/// metric. For example a label for 'all HTTP requests that used the method
/// POST' can be assigned with `method= "POST"`.
///
/// Given a metric name and a set of labels, time series are frequently
/// identified using this notation:
///
///     <metric name> { < label name >= <label value>, ... }
///
/// It is required to follow the syntax of metric names and labels given by:
/// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
///
/// The following metric and label conventions are not required for using
/// Prometheus, but can serve as both a style-guide and a collection of best
/// practices: https://prometheus.io/docs/practices/naming/
///
/// \tparam T One of the metric types Counter, Gauge, Histogram or Summary.
template <typename T>
class Family : public Collectable {
 public:
  /// \brief Create a new metric.
  ///
  /// Every metric is uniquely identified by its name and a set of key-value
  /// pairs, also known as labels. Prometheus's query language allows filtering
  /// and aggregation based on metric name and these labels.
  ///
  /// This example selects all time series that have the `http_requests_total`
  /// metric name:
  ///
  ///     http_requests_total
  ///
  /// It is possible to assign labels to the metric name. These labels are
  /// propagated to each dimensional data added with Add(). For example if a
  /// label `job= "prometheus"` is provided to this constructor, it is possible
  /// to filter this time series with Prometheus's query language by appending
  /// a set of labels to match in curly braces ({})
  ///
  ///     http_requests_total{job= "prometheus"}
  ///
  /// For further information see: [Quering Basics]
  /// (https://prometheus.io/docs/prometheus/latest/querying/basics/)
  ///
  /// \param name Set the metric name.
  /// \param help Set an additional description.
  /// \param constant_labels Assign a set of key-value pairs (= labels) to the
  /// metric. All these labels are propagated to each time series within the
  /// metric.
  Family(const std::string& name, const std::string& help,
         const std::map<std::string, std::string>& constant_labels);

  /// \brief Add a new dimensional data.
  ///
  /// Each new set of labels adds a new dimensional data and is exposed in
  /// Prometheus as a time series. It is possible to filter the time series
  /// with Prometheus's query language by appending a set of labels to match in
  /// curly braces ({})
  ///
  ///     http_requests_total{job= "prometheus",method= "POST"}
  ///
  /// \param labels Assign a set of key-value pairs (= labels) to the
  /// dimensional data. The function does nothing, if the same set of lables
  /// already exists.
  /// \param args Arguments are passed to the constructor of metric type T. See
  /// Counter, Gauge, Histogram or Summary for required constructor arguments.
  /// \return Return the newly created dimensional data or - if a same set of
  /// lables already exists - the already existing dimensional data.
  template <typename... Args>
  T& Add(const std::map<std::string, std::string>& labels, Args&&... args);

  /// \brief Remove the given dimensional data.
  ///
  /// \param metric Dimensional data to be removed. The function does nothing,
  /// if the given metric was not returned by Add().
  void Remove(T* metric);

  /// \brief Returns the current value of each dimensional data.
  ///
  /// Collect is called by the Registry when collecting metrics.
  ///
  /// \return Zero or more samples for each dimensional data.
  std::vector<MetricFamily> Collect() override;

 private:
  std::unordered_map<std::size_t, std::unique_ptr<T>> metrics_;
  std::unordered_map<std::size_t, std::map<std::string, std::string>> labels_;
  std::unordered_map<T*, std::size_t> labels_reverse_lookup_;

  const std::string name_;
  const std::string help_;
  const std::map<std::string, std::string> constant_labels_;
  std::mutex mutex_;

  ClientMetric CollectMetric(std::size_t hash, T* metric);
};

template <typename T>
Family<T>::Family(const std::string& name, const std::string& help,
                  const std::map<std::string, std::string>& constant_labels)
    : name_(name), help_(help), constant_labels_(constant_labels) {
  assert(CheckMetricName(name_));
}

template <typename T>
template <typename... Args>
T& Family<T>::Add(const std::map<std::string, std::string>& labels,
                  Args&&... args) {
  auto hash = detail::hash_labels(labels);
  std::lock_guard<std::mutex> lock{mutex_};
  auto metrics_iter = metrics_.find(hash);

  if (metrics_iter != metrics_.end()) {
#ifndef NDEBUG
    auto labels_iter = labels_.find(hash);
    assert(labels_iter != labels_.end());
    const auto& old_labels = labels_iter->second;
    assert(labels == old_labels);
#endif
    return *metrics_iter->second;
  } else {
#ifndef NDEBUG
    for (auto& label_pair : labels) {
      auto& label_name = label_pair.first;
      assert(CheckLabelName(label_name));
    }
#endif

    auto metric =
        metrics_.insert(std::make_pair(hash, detail::make_unique<T>(args...)));
    assert(metric.second);
    labels_.insert({hash, labels});
    labels_reverse_lookup_.insert({metric.first->second.get(), hash});
    return *(metric.first->second);
  }
}

template <typename T>
void Family<T>::Remove(T* metric) {
  std::lock_guard<std::mutex> lock{mutex_};
  if (labels_reverse_lookup_.count(metric) == 0) {
    return;
  }

  auto hash = labels_reverse_lookup_.at(metric);
  metrics_.erase(hash);
  labels_.erase(hash);
  labels_reverse_lookup_.erase(metric);
}

template <typename T>
std::vector<MetricFamily> Family<T>::Collect() {
  std::lock_guard<std::mutex> lock{mutex_};
  auto family = MetricFamily{};
  family.name = name_;
  family.help = help_;
  family.type = T::metric_type;
  for (const auto& m : metrics_) {
    family.metric.push_back(std::move(CollectMetric(m.first, m.second.get())));
  }
  return {family};
}

template <typename T>
ClientMetric Family<T>::CollectMetric(std::size_t hash, T* metric) {
  auto collected = metric->Collect();
  auto add_label =
      [&collected](const std::pair<std::string, std::string>& label_pair) {
        auto label = ClientMetric::Label{};
        label.name = label_pair.first;
        label.value = label_pair.second;
        collected.label.push_back(std::move(label));
      };
  std::for_each(constant_labels_.cbegin(), constant_labels_.cend(), add_label);
  const auto& metric_labels = labels_.at(hash);
  std::for_each(metric_labels.cbegin(), metric_labels.cend(), add_label);
  return collected;
}

}  // namespace prometheus
