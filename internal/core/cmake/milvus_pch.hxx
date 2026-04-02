// Milvus precompiled header - STL + commonly-used third-party headers.
// This file is auto-included via CMake's target_precompile_headers().
// Adding a header here speeds up compilation for ALL 350+ .cpp files.
// NOTE: Avoid adding very large headers (protobuf .pb.h) here — the per-target
// PCH compilation cost outweighs the benefit at low parallelism (j8).

// ---- STL headers (included in 50+ .cpp files) ----
#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <map>
#include <unordered_map>
#include <optional>
#include <functional>
#include <chrono>
#include <string_view>
#include <type_traits>
#include <exception>
#include <cstddef>
#include <limits>
#include <tuple>
#include <set>
#include <numeric>
#include <mutex>
#include <atomic>
#include <stdexcept>
#include <variant>

// ---- Third-party headers (included in 40+ .cpp files, small parse cost) ----
#include "fmt/core.h"
#include "glog/logging.h"
