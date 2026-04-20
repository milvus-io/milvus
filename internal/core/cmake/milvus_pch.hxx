// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
