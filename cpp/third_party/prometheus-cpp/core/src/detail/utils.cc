#include "prometheus/detail/utils.h"
#include "hash.h"

#include <numeric>

namespace prometheus {

namespace detail {

std::size_t hash_labels(const std::map<std::string, std::string>& labels) {
  size_t seed = 0;
  for (auto& label : labels) {
    hash_combine(&seed, label.first, label.second);
  }

  return seed;
}

}  // namespace detail

}  // namespace prometheus
