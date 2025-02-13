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

    void
    set_sub_dim(int64_t sub_dim) {
        sub_dim_ = sub_dim;
    }

    int64_t
    get_sub_dim() const {
        return sub_dim_;
    }

    void
    set_refine_ratio(float refine_ratio) {
        refine_ratio_ = refine_ratio;
    }

    int64_t
    get_refine_ratio() const {
        return refine_ratio_;
    }

    void
    set_dense_vector_intermin_index_type(const std::string index_type) {
        AssertInfo(valid_dense_vector_index_type.find(index_type) !=
                       valid_dense_vector_index_type.end(),
                   "fail to set dense vector index type.");
        dense_index_type_ = index_type;
    }

    std::string
    get_dense_vector_intermin_index_type() const {
        return dense_index_type_;
    }

 private:
    inline static const std::unordered_set<std::string>
        valid_dense_vector_index_type = {
            knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC,
            knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
    };
    inline static bool enable_interim_segment_index_ = false;
    inline static int64_t chunk_rows_ = 32 * 1024;
    inline static int64_t nlist_ = 100;
    inline static int64_t nprobe_ = 4;
    inline static int64_t sub_dim_ = 2;
    inline static float refine_ratio_ = 3.0;
    inline static std::string dense_index_type_ =
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
};

}  // namespace milvus::segcore
