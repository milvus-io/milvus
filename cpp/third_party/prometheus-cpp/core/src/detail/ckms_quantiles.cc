#include "prometheus/detail/ckms_quantiles.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace prometheus {
namespace detail {

CKMSQuantiles::Quantile::Quantile(double quantile, double error)
    : quantile(quantile),
      error(error),
      u(2.0 * error / (1.0 - quantile)),
      v(2.0 * error / quantile) {}

CKMSQuantiles::Item::Item(double value, int lower_delta, int delta)
    : value(value), g(lower_delta), delta(delta) {}

CKMSQuantiles::CKMSQuantiles(const std::vector<Quantile>& quantiles)
    : quantiles_(quantiles), count_(0), buffer_{}, buffer_count_(0) {}

void CKMSQuantiles::insert(double value) {
  buffer_[buffer_count_] = value;
  ++buffer_count_;

  if (buffer_count_ == buffer_.size()) {
    insertBatch();
    compress();
  }
}

double CKMSQuantiles::get(double q) {
  insertBatch();
  compress();

  if (sample_.empty()) {
    return std::numeric_limits<double>::quiet_NaN();
  }

  int rankMin = 0;
  const auto desired = static_cast<int>(q * count_);
  const auto bound = desired + (allowableError(desired) / 2);

  auto it = sample_.begin();
  decltype(it) prev;
  auto cur = it++;

  while (it != sample_.end()) {
    prev = cur;
    cur = it++;

    rankMin += prev->g;

    if (rankMin + cur->g + cur->delta > bound) {
      return prev->value;
    }
  }

  return sample_.back().value;
}

void CKMSQuantiles::reset() {
  count_ = 0;
  sample_.clear();
  buffer_count_ = 0;
}

double CKMSQuantiles::allowableError(int rank) {
  auto size = sample_.size();
  double minError = size + 1;

  for (const auto& q : quantiles_.get()) {
    double error;
    if (rank <= q.quantile * size) {
      error = q.u * (size - rank);
    } else {
      error = q.v * rank;
    }
    if (error < minError) {
      minError = error;
    }
  }

  return minError;
}

bool CKMSQuantiles::insertBatch() {
  if (buffer_count_ == 0) {
    return false;
  }

  std::sort(buffer_.begin(), buffer_.begin() + buffer_count_);

  std::size_t start = 0;
  if (sample_.empty()) {
    sample_.emplace_back(buffer_[0], 1, 0);
    ++start;
    ++count_;
  }

  std::size_t idx = 0;
  std::size_t item = idx++;

  for (std::size_t i = start; i < buffer_count_; ++i) {
    double v = buffer_[i];
    while (idx < sample_.size() && sample_[item].value < v) {
      item = idx++;
    }

    if (sample_[item].value > v) {
      --idx;
    }

    int delta;
    if (idx - 1 == 0 || idx + 1 == sample_.size()) {
      delta = 0;
    } else {
      delta = static_cast<int>(std::floor(allowableError(idx + 1))) + 1;
    }

    sample_.emplace(sample_.begin() + idx, v, 1, delta);
    count_++;
    item = idx++;
  }

  buffer_count_ = 0;
  return true;
}

void CKMSQuantiles::compress() {
  if (sample_.size() < 2) {
    return;
  }

  std::size_t idx = 0;
  std::size_t prev;
  std::size_t next = idx++;

  while (idx < sample_.size()) {
    prev = next;
    next = idx++;

    if (sample_[prev].g + sample_[next].g + sample_[next].delta <=
        allowableError(idx - 1)) {
      sample_[next].g += sample_[prev].g;
      sample_.erase(sample_.begin() + prev);
    }
  }
}

}  // namespace detail
}  // namespace prometheus
