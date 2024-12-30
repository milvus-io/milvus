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
apply_hits(milvus::TargetBitmap& bitset,
           const milvus::index::RustArrayWrapper& w,
           bool v) {
    for (size_t j = 0; j < w.array_.len; j++) {
        bitset[w.array_.array[j]] = v;
    }
}

inline size_t
should_allocate_bitset_size(const milvus::index::RustArrayWrapper& w) {
    if (w.array_.len == 0) {
        return 0;
    }
    size_t cnt = 0;
    for (size_t i = 0; i < w.array_.len; i++) {
        cnt = std::max(cnt, static_cast<size_t>(w.array_.array[i]));
    }
    return cnt + 1;
}

inline void
apply_hits_with_filter(milvus::TargetBitmap& bitset,
                       const milvus::index::RustArrayWrapper& w,
                       const std::function<bool(size_t /* offset */)>& filter) {
    for (size_t j = 0; j < w.array_.len; j++) {
        auto the_offset = w.array_.array[j];
        bitset[the_offset] = filter(the_offset);
    }
}

inline void
apply_hits_with_callback(
    const milvus::index::RustArrayWrapper& w,
    const std::function<void(size_t /* offset */)>& callback) {
    for (size_t j = 0; j < w.array_.len; j++) {
        callback(w.array_.array[j]);
    }
}
}  // namespace milvus::index
