#pragma once

namespace prometheus {

enum class MetricType {
  Counter,
  Gauge,
  Summary,
  Untyped,
  Histogram,
};

}  // namespace prometheus
