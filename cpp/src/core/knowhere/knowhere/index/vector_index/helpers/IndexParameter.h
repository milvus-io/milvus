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

#include "knowhere/common/Config.h"
#include <faiss/Index.h>


namespace zilliz {
namespace knowhere {

extern faiss::MetricType GetMetricType(METRICTYPE &type);

// IVF Config
constexpr int64_t DEFAULT_NLIST = INVALID_VALUE;
constexpr int64_t DEFAULT_NPROBE = INVALID_VALUE;
constexpr int64_t DEFAULT_NSUBVECTORS = INVALID_VALUE;
constexpr int64_t DEFAULT_NBITS = INVALID_VALUE;
constexpr int64_t DEFAULT_SCAN_TABLE_THREHOLD = INVALID_VALUE;
constexpr int64_t DEFAULT_POLYSEMOUS_HT = INVALID_VALUE;
constexpr int64_t DEFAULT_MAX_CODES = INVALID_VALUE;

// NSG Config
constexpr int64_t DEFAULT_SEARCH_LENGTH = INVALID_VALUE;
constexpr int64_t DEFAULT_OUT_DEGREE = INVALID_VALUE;
constexpr int64_t DEFAULT_CANDIDATE_SISE = INVALID_VALUE;
constexpr int64_t DEFAULT_NNG_K = INVALID_VALUE;

struct IVFCfg : public Cfg {
    int64_t nlist = DEFAULT_NLIST;
    int64_t nprobe = DEFAULT_NPROBE;

    IVFCfg(const int64_t &dim,
           const int64_t &k,
           const int64_t &gpu_id,
           const int64_t &nlist,
           const int64_t &nprobe,
           METRICTYPE type)
        : Cfg(dim, k, gpu_id, type), nlist(nlist), nprobe(nprobe) {
    }

    IVFCfg() = default;

    bool
    CheckValid() override {
        return true;
    };
};
using IVFConfig = std::shared_ptr<IVFCfg>;

struct IVFSQCfg : public IVFCfg {
    // TODO(linxj): cpu only support SQ4 SQ6 SQ8 SQ16, gpu only support SQ4, SQ8, SQ16
    int64_t nbits = DEFAULT_NBITS;

    IVFSQCfg(const int64_t &dim,
             const int64_t &k,
             const int64_t &gpu_id,
             const int64_t &nlist,
             const int64_t &nprobe,
             const int64_t &nbits,
             METRICTYPE type)
        : IVFCfg(dim, k, gpu_id, nlist, nprobe, type), nbits(nbits) {
    }

    IVFSQCfg() = default;

    bool
    CheckValid() override {
        return true;
    };
};
using IVFSQConfig = std::shared_ptr<IVFSQCfg>;

struct IVFPQCfg : public IVFCfg {
    int64_t m = DEFAULT_NSUBVECTORS; // number of subquantizers(subvector)
    int64_t nbits = DEFAULT_NBITS;  // number of bit per subvector index

    // TODO(linxj): not use yet
    int64_t scan_table_threhold = DEFAULT_SCAN_TABLE_THREHOLD;
    int64_t polysemous_ht = DEFAULT_POLYSEMOUS_HT;
    int64_t max_codes = DEFAULT_MAX_CODES;

    IVFPQCfg(const int64_t &dim,
             const int64_t &k,
             const int64_t &gpu_id,
             const int64_t &nlist,
             const int64_t &nprobe,
             const int64_t &nbits,
             const int64_t &m,
             METRICTYPE type)
        : IVFCfg(dim, k, gpu_id, nlist, nprobe, type), m(m), nbits(nbits) {
    }

    IVFPQCfg() = default;

    bool
    CheckValid() override {
        return true;
    };
};
using IVFPQConfig = std::shared_ptr<IVFPQCfg>;

struct NSGCfg : public IVFCfg {
    int64_t knng = DEFAULT_NNG_K;
    int64_t search_length = DEFAULT_SEARCH_LENGTH;
    int64_t out_degree = DEFAULT_OUT_DEGREE;
    int64_t candidate_pool_size = DEFAULT_CANDIDATE_SISE;

    NSGCfg(const int64_t &dim,
           const int64_t &k,
           const int64_t &gpu_id,
           const int64_t &nlist,
           const int64_t &nprobe,
           const int64_t &knng,
           const int64_t &search_length,
           const int64_t &out_degree,
           const int64_t &candidate_size,
           METRICTYPE type)
        : IVFCfg(dim, k, gpu_id, nlist, nprobe, type),
          knng(knng), search_length(search_length),
          out_degree(out_degree), candidate_pool_size(candidate_size) {
    }

    NSGCfg() = default;

    bool
    CheckValid() override {
        return true;
    };
};
using NSGConfig = std::shared_ptr<NSGCfg>;

struct KDTCfg : public Cfg {
    int64_t tptnubmber = -1;
};

} // knowhere
} // zilliz

