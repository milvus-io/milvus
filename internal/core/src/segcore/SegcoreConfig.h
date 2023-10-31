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

#include <map>
#include <string>

#include "common/Types.h"
#include "common/Json.h"
#include "index/Utils.h"
#include "common/EasyAssert.h"

namespace milvus::segcore {

class SegcoreConfig {
 public:
    static SegcoreConfig&
    default_config() {
        // TODO: remove this when go side is ready
        static SegcoreConfig config;
        return config;
    }

    void
    parse_from(const std::string& string_path);

    int64_t
    get_chunk_rows() const {
        return chunk_rows_;
    }

    void
    set_chunk_rows(int64_t chunk_rows) {
        chunk_rows_ = chunk_rows;
    }

    int64_t
    get_nlist() const {
        return nlist_;
    }

    int64_t
    get_nprobe() const {
        return nprobe_;
    }

    void
    set_nlist(int64_t nlist) {
        nlist_ = nlist;
    }

    void
    set_nprobe(int64_t nprobe) {
        nprobe_ = nprobe;
    }

    void
    set_enable_interim_segment_index(bool enable_interim_segment_index) {
        this->enable_interim_segment_index_ = enable_interim_segment_index;
    }

    bool
    get_enable_interim_segment_index() const {
        return enable_interim_segment_index_;
    }

 private:
    inline static bool enable_interim_segment_index_ = false;
    inline static int64_t chunk_rows_ = 32 * 1024;
    inline static int64_t nlist_ = 100;
    inline static int64_t nprobe_ = 4;
};

}  // namespace milvus::segcore
