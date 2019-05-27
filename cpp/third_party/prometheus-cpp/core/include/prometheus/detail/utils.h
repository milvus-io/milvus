#pragma once

#include <cstddef>
#include <map>
#include <string>

namespace prometheus {

namespace detail {

/// \brief Compute the hash value of a map of labels.
///
/// \param labels The map that will be computed the hash value.
///
/// \returns The hash value of the given labels.
std::size_t hash_labels(const std::map<std::string, std::string>& labels);

}  // namespace detail

}  // namespace prometheus
