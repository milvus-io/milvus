// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>

#include "common/EasyAssert.h"

namespace milvus::index::vector_disk_ann {

inline constexpr uint32_t kMinBeamwidth = 1;
inline constexpr uint32_t kMaxBeamwidth = 128;

inline void
ValidateBeamwidth(uint32_t beamwidth) {
    if (beamwidth < kMinBeamwidth || beamwidth > kMaxBeamwidth) {
        ThrowInfo(ConfigInvalid,
                  "DiskANN beamwidth {} is outside [{}, {}]",
                  beamwidth,
                  kMinBeamwidth,
                  kMaxBeamwidth);
    }
}

}  // namespace milvus::index::vector_disk_ann
