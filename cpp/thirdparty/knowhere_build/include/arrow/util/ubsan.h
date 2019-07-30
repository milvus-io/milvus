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

// Contains utilities for making UBSan happy.

#pragma once

#include <memory>

#include "arrow/util/macros.h"

namespace arrow {
namespace util {

namespace internal {

static uint8_t non_null_filler;

}  // namespace internal

/// \brief Returns maybe_null if not null or a non-null pointer to an arbitrary memory
/// that shouldn't be dereferenced.
///
/// Memset/Memcpy are undefinfed when a nullptr is passed as an argument use this utility
/// method to wrap locations where this could happen.
///
/// Note: Flatbuffers has UBSan warnings if a zero length vector is passed.
/// https://github.com/google/flatbuffers/pull/5355 is trying to resolve them.
template <typename T>
inline T* MakeNonNull(T* maybe_null) {
  if (ARROW_PREDICT_TRUE(maybe_null != NULLPTR)) {
    return maybe_null;
  }

  return reinterpret_cast<T*>(&internal::non_null_filler);
}

}  // namespace util
}  // namespace arrow
