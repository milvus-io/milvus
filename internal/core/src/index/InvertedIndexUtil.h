// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

namespace milvus::index {
inline void
apply_hits_with_filter(milvus::TargetBitmap& bitset,
                       const std::function<bool(size_t /* offset */)>& filter) {
    std::optional<size_t> result = bitset.find_first();
    while (result.has_value()) {
        size_t offset = result.value();
        bitset[offset] = filter(offset);
        result = bitset.find_next(offset);
    }
}

inline void
apply_hits_with_callback(
    milvus::TargetBitmap& bitset,
    const std::function<void(size_t /* offset */)>& callback) {
    std::optional<size_t> result = bitset.find_first();
    while (result.has_value()) {
        size_t offset = result.value();
        callback(offset);
        result = bitset.find_next(offset);
    }
}
}  // namespace milvus::index
