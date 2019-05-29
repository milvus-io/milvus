#pragma once

#include <chrono>
#include <cstddef>
#include <vector>

#include "prometheus/detail/ckms_quantiles.h"

namespace prometheus {
namespace detail {

class TimeWindowQuantiles {
  using Clock = std::chrono::steady_clock;

 public:
  TimeWindowQuantiles(const std::vector<CKMSQuantiles::Quantile>& quantiles,
                      Clock::duration max_age_seconds, int age_buckets);

  double get(double q);
  void insert(double value);

 private:
  CKMSQuantiles& rotate();

  const std::vector<CKMSQuantiles::Quantile>& quantiles_;
  std::vector<CKMSQuantiles> ckms_quantiles_;
  std::size_t current_bucket_;

  Clock::time_point last_rotation_;
  const Clock::duration rotation_interval_;
};

}  // namespace detail
}  // namespace prometheus
