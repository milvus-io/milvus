/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace faiss { namespace gpu {

/// List of supported metrics
inline bool isMetricSupported(MetricType mt) {
  switch (mt) {
    case MetricType::METRIC_INNER_PRODUCT:
    case MetricType::METRIC_L2:
      return true;
    default:
      return false;
  }
}

/// Sort direction per each metric
inline bool metricToSortDirection(MetricType mt) {
  switch (mt) {
    case MetricType::METRIC_INNER_PRODUCT:
      // highest
      return true;
    case MetricType::METRIC_L2:
      // lowest
      return false;
    default:
      // unhandled metric
      FAISS_ASSERT(false);
      return false;
  }
}

struct L2Metric {
  static inline __device__ float distance(float a, float b) {
    float d = a - b;
    return d * d;
  }
};

struct IPMetric {
  static inline __device__ float distance(float a, float b) {
    return a * b;
  }
};

} } // namespace
