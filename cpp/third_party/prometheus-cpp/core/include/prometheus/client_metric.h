#pragma once

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

namespace prometheus {

struct ClientMetric {
  // Label

  struct Label {
    std::string name;
    std::string value;

    bool operator<(const Label& rhs) const {
      return std::tie(name, value) < std::tie(rhs.name, rhs.value);
    }

    bool operator==(const Label& rhs) const {
      return std::tie(name, value) == std::tie(rhs.name, rhs.value);
    }
  };
  std::vector<Label> label;

  // Counter

  struct Counter {
    double value = 0.0;
  };
  Counter counter;

  // Gauge

  struct Gauge {
    double value = 0.0;
  };
  Gauge gauge;

  // Summary

  struct Quantile {
    double quantile = 0.0;
    double value = 0.0;
  };

  struct Summary {
    std::uint64_t sample_count = 0;
    double sample_sum = 0.0;
    std::vector<Quantile> quantile;
  };
  Summary summary;

  // Histogram

  struct Bucket {
    std::uint64_t cumulative_count = 0;
    double upper_bound = 0.0;
  };

  struct Histogram {
    std::uint64_t sample_count = 0;
    double sample_sum = 0.0;
    std::vector<Bucket> bucket;
  };
  Histogram histogram;

  // Untyped

  struct Untyped {
    double value = 0;
  };
  Untyped untyped;

  // Timestamp

  std::int64_t timestamp_ms = 0;
};

}  // namespace prometheus
