#pragma once

#include <array>
#include <cstddef>
#include <functional>
#include <vector>

namespace prometheus {
namespace detail {

class CKMSQuantiles {
 public:
  struct Quantile {
    const double quantile;
    const double error;
    const double u;
    const double v;

    Quantile(double quantile, double error);
  };

 private:
  struct Item {
    /*const*/ double value;
    int g;
    /*const*/ int delta;

    explicit Item(double value, int lower_delta, int delta);
  };

 public:
  explicit CKMSQuantiles(const std::vector<Quantile>& quantiles);

  void insert(double value);
  double get(double q);
  void reset();

 private:
  double allowableError(int rank);
  bool insertBatch();
  void compress();

 private:
  const std::reference_wrapper<const std::vector<Quantile>> quantiles_;

  std::size_t count_;
  std::vector<Item> sample_;
  std::array<double, 500> buffer_;
  std::size_t buffer_count_;
};

}  // namespace detail
}  // namespace prometheus
