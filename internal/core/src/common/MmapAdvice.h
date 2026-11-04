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

#pragma once

#include <sys/mman.h>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <optional>
#include <string>

namespace milvus {

// Keep the policy names and best-effort semantics consistent with Cardinal.
enum class MmapAdvice { None, Normal, Random, Sequential, WillNeed };

inline const char*
MmapAdviceName(MmapAdvice advice) {
    switch (advice) {
        case MmapAdvice::None:
            return "none";
        case MmapAdvice::Normal:
            return "normal";
        case MmapAdvice::Random:
            return "random";
        case MmapAdvice::Sequential:
            return "sequential";
        case MmapAdvice::WillNeed:
            return "willneed";
    }
    return "unknown";
}

inline std::optional<MmapAdvice>
ParseMmapAdvice(std::string advice) {
    std::transform(
        advice.begin(), advice.end(), advice.begin(), [](unsigned char ch) {
            return std::tolower(ch);
        });
    if (advice.empty() || advice == "none") {
        return MmapAdvice::None;
    }
    if (advice == "normal") {
        return MmapAdvice::Normal;
    }
    if (advice == "random") {
        return MmapAdvice::Random;
    }
    if (advice == "sequential") {
        return MmapAdvice::Sequential;
    }
    if (advice == "willneed") {
        return MmapAdvice::WillNeed;
    }
    return std::nullopt;
}

inline std::optional<int>
NativeMmapAdvice(MmapAdvice advice) {
    switch (advice) {
        case MmapAdvice::None:
            return std::nullopt;
#ifdef MADV_NORMAL
        case MmapAdvice::Normal:
            return MADV_NORMAL;
#endif
#ifdef MADV_RANDOM
        case MmapAdvice::Random:
            return MADV_RANDOM;
#endif
#ifdef MADV_SEQUENTIAL
        case MmapAdvice::Sequential:
            return MADV_SEQUENTIAL;
#endif
#ifdef MADV_WILLNEED
        case MmapAdvice::WillNeed:
            return MADV_WILLNEED;
#endif
        default:
            return std::nullopt;
    }
}

// The caller owns the mapping and supplies a page-aligned address. Advice is
// a hint: unsupported policies or syscall failures must not fail a data load.
void
ApplyMmapAdvice(void* data, size_t size, MmapAdvice advice);

}  // namespace milvus
