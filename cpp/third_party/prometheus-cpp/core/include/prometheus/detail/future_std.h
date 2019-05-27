#pragma once

#include <memory>
#include <utility>

namespace prometheus {
namespace detail {

// Remove as soon C++14 can be used.
template <typename T, typename... Args>
std::unique_ptr<T> make_unique(Args&&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

}  // namespace detail
}  // namespace prometheus
