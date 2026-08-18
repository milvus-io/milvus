// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
// the specific language governing permissions and limitations under the License.

#pragma once

#include <optional>
#include <string>

#include "knowhere/index/index_factory.h"
#include "segcore/SegcoreConfig.h"

namespace milvus::segcore {

inline std::string
RefineTypeToConfigStringForTest(knowhere::RefineType refine_type) {
    if (refine_type == knowhere::RefineType::DATA_VIEW) {
        return "NONE";
    }
    if (refine_type == knowhere::RefineType::BFLOAT16_QUANT) {
        return "BFLOAT16";
    }
    if (refine_type == knowhere::RefineType::FLOAT16_QUANT) {
        return "FLOAT16";
    }
    if (refine_type == knowhere::RefineType::UINT8_QUANT) {
        return "UINT8";
    }
    ThrowInfo(Unsupported, "unsupported refine type for test config restore");
}

class ScopedSegcoreConfigRestore {
 public:
    explicit ScopedSegcoreConfigRestore(
        SegcoreConfig& config = SegcoreConfig::default_config())
        : config_(config),
          chunk_rows_(config.get_chunk_rows()),
          nlist_(config.get_nlist()),
          nprobe_(config.get_nprobe()),
          enable_interim_segment_index_(
              config.get_enable_interim_segment_index()),
          storage_v3_enabled_(config.get_storage_v3_enabled()),
          enable_growing_source_flush_(
              config.get_enable_growing_source_flush()),
          sub_dim_(config.get_sub_dim()),
          refine_ratio_(config.get_refine_ratio()),
          dense_vector_interim_index_type_(
              config.get_dense_vector_intermin_index_type()),
          refine_quant_type_(
              RefineTypeToConfigStringForTest(config.get_refine_quant_type())),
          refine_with_quant_flag_(config.get_refine_with_quant_flag()) {
    }

    ~ScopedSegcoreConfigRestore() {
        config_.set_chunk_rows(chunk_rows_);
        config_.set_nlist(nlist_);
        config_.set_nprobe(nprobe_);
        config_.set_enable_interim_segment_index(enable_interim_segment_index_);
        config_.set_storage_v3_enabled(storage_v3_enabled_);
        config_.set_enable_growing_source_flush(enable_growing_source_flush_);
        config_.set_sub_dim(sub_dim_);
        config_.set_refine_ratio(refine_ratio_);
        config_.set_dense_vector_intermin_index_type(
            dense_vector_interim_index_type_);
        config_.set_refine_quant_type(refine_quant_type_);
        config_.set_refine_with_quant_flag(refine_with_quant_flag_);
    }

    ScopedSegcoreConfigRestore(const ScopedSegcoreConfigRestore&) = delete;
    ScopedSegcoreConfigRestore&
    operator=(const ScopedSegcoreConfigRestore&) = delete;

 private:
    SegcoreConfig& config_;
    int64_t chunk_rows_;
    int64_t nlist_;
    int64_t nprobe_;
    bool enable_interim_segment_index_;
    bool storage_v3_enabled_;
    bool enable_growing_source_flush_;
    int64_t sub_dim_;
    float refine_ratio_;
    std::string dense_vector_interim_index_type_;
    std::string refine_quant_type_;
    bool refine_with_quant_flag_;
};

struct InterimIndexConfigForTest {
    bool enable_interim_segment_index = true;
    int64_t chunk_rows = 32 * 1024;
    int64_t nlist = 100;
    int64_t nprobe = 4;
    std::optional<std::string> dense_vector_interim_index_type = std::nullopt;
    int64_t sub_dim = 2;
    float refine_ratio = 3.0F;
    std::string refine_quant_type = "NONE";
    bool refine_with_quant_flag = false;
};

inline void
ApplyInterimIndexConfigForTest(
    const InterimIndexConfigForTest& options,
    SegcoreConfig& config = SegcoreConfig::default_config()) {
    config.set_chunk_rows(options.chunk_rows);
    config.set_enable_interim_segment_index(
        options.enable_interim_segment_index);
    config.set_nlist(options.nlist);
    config.set_nprobe(options.nprobe);
    if (options.dense_vector_interim_index_type.has_value()) {
        config.set_dense_vector_intermin_index_type(
            options.dense_vector_interim_index_type.value());
    }

    if (options.dense_vector_interim_index_type.has_value() &&
        options.dense_vector_interim_index_type.value() ==
            knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR) {
        config.set_sub_dim(options.sub_dim);
        config.set_refine_ratio(options.refine_ratio);
        config.set_refine_quant_type(options.refine_quant_type);
        config.set_refine_with_quant_flag(options.refine_with_quant_flag);
    }
}

}  // namespace milvus::segcore
