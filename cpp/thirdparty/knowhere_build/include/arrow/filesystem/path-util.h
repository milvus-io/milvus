// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "arrow/status.h"

namespace arrow {
namespace fs {
namespace internal {

constexpr char kSep = '/';

// Computations on abstract paths (not local paths with system-dependent behaviour).
// Abstract paths are typically used in URIs.

// Split an abstract path into its individual components.
ARROW_EXPORT
std::vector<std::string> SplitAbstractPath(const std::string& s);

// Return the parent directory and basename of an abstract path.  Both values may be
// empty.
ARROW_EXPORT
std::pair<std::string, std::string> GetAbstractPathParent(const std::string& s);

// Validate the components of an abstract path.
ARROW_EXPORT
Status ValidateAbstractPathParts(const std::vector<std::string>& parts);

// Append a non-empty stem to an abstract path.
ARROW_EXPORT
std::string ConcatAbstractPath(const std::string& base, const std::string& stem);

ARROW_EXPORT
std::string EnsureTrailingSlash(const std::string& s);

// Join the components of an abstract path.
template <class StringIt>
std::string JoinAbstractPath(StringIt it, StringIt end) {
  std::string path;
  for (; it != end; ++it) {
    if (!path.empty()) {
      path += kSep;
    }
    path += *it;
  }
  return path;
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow
