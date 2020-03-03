// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <faiss/Index.h>
#include <memory>

#include "knowhere/common/Config.h"

namespace knowhere {

extern faiss::MetricType
GetMetricType(METRICTYPE& type);

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

// SPTAG Config
constexpr int64_t DEFAULT_SAMPLES = INVALID_VALUE;
constexpr int64_t DEFAULT_TPTNUMBER = INVALID_VALUE;
constexpr int64_t DEFAULT_TPTLEAFSIZE = INVALID_VALUE;
constexpr int64_t DEFAULT_NUMTOPDIMENSIONTPTSPLIT = INVALID_VALUE;
constexpr int64_t DEFAULT_NEIGHBORHOODSIZE = INVALID_VALUE;
constexpr int64_t DEFAULT_GRAPHNEIGHBORHOODSCALE = INVALID_VALUE;
constexpr int64_t DEFAULT_GRAPHCEFSCALE = INVALID_VALUE;
constexpr int64_t DEFAULT_REFINEITERATIONS = INVALID_VALUE;
constexpr int64_t DEFAULT_CEF = INVALID_VALUE;
constexpr int64_t DEFAULT_MAXCHECKFORREFINEGRAPH = INVALID_VALUE;
constexpr int64_t DEFAULT_NUMOFTHREADS = INVALID_VALUE;
constexpr int64_t DEFAULT_MAXCHECK = INVALID_VALUE;
constexpr int64_t DEFAULT_THRESHOLDOFNUMBEROFCONTINUOUSNOBETTERPROPAGATION = INVALID_VALUE;
constexpr int64_t DEFAULT_NUMBEROFINITIALDYNAMICPIVOTS = INVALID_VALUE;
constexpr int64_t DEFAULT_NUMBEROFOTHERDYNAMICPIVOTS = INVALID_VALUE;

// KDT Config
constexpr int64_t DEFAULT_KDTNUMBER = INVALID_VALUE;
constexpr int64_t DEFAULT_NUMTOPDIMENSIONKDTSPLIT = INVALID_VALUE;

// BKT Config
constexpr int64_t DEFAULT_BKTNUMBER = INVALID_VALUE;
constexpr int64_t DEFAULT_BKTKMEANSK = INVALID_VALUE;
constexpr int64_t DEFAULT_BKTLEAFSIZE = INVALID_VALUE;

// HNSW Config
constexpr int64_t DEFAULT_M = INVALID_VALUE;
constexpr int64_t DEFAULT_EF = INVALID_VALUE;

struct IVFCfg : public Cfg {
    int64_t nlist = DEFAULT_NLIST;
    int64_t nprobe = DEFAULT_NPROBE;

    IVFCfg(const int64_t& dim, const int64_t& k, const int64_t& gpu_id, const int64_t& nlist, const int64_t& nprobe,
           METRICTYPE type)
        : Cfg(dim, k, gpu_id, type), nlist(nlist), nprobe(nprobe) {
    }

    IVFCfg() = default;

    std::stringstream
    DumpImpl() override;

    //    bool
    //    CheckValid() override {
    //        return true;
    //    };
};
using IVFConfig = std::shared_ptr<IVFCfg>;

struct IVFBinCfg : public IVFCfg {
    bool
    CheckValid() override {
        if (metric_type == METRICTYPE::HAMMING || metric_type == METRICTYPE::TANIMOTO ||
            metric_type == METRICTYPE::JACCARD) {
            return true;
        }
        std::stringstream ss;
        ss << "MetricType: " << int(metric_type) << " not support!";
        KNOWHERE_THROW_MSG(ss.str());
        return false;
    }
};

struct IVFSQCfg : public IVFCfg {
    // TODO(linxj): cpu only support SQ4 SQ6 SQ8 SQ16, gpu only support SQ4, SQ8, SQ16
    int64_t nbits = DEFAULT_NBITS;

    IVFSQCfg(const int64_t& dim, const int64_t& k, const int64_t& gpu_id, const int64_t& nlist, const int64_t& nprobe,
             const int64_t& nbits, METRICTYPE type)
        : IVFCfg(dim, k, gpu_id, nlist, nprobe, type), nbits(nbits) {
    }

    std::stringstream
    DumpImpl() override;

    IVFSQCfg() = default;

    //    bool
    //    CheckValid() override {
    //        return true;
    //    };
};
using IVFSQConfig = std::shared_ptr<IVFSQCfg>;

struct IVFPQCfg : public IVFCfg {
    int64_t m = DEFAULT_NSUBVECTORS;  // number of subquantizers(subvector)
    int64_t nbits = DEFAULT_NBITS;    // number of bit per subvector index

    // TODO(linxj): not use yet
    int64_t scan_table_threhold = DEFAULT_SCAN_TABLE_THREHOLD;
    int64_t polysemous_ht = DEFAULT_POLYSEMOUS_HT;
    int64_t max_codes = DEFAULT_MAX_CODES;

    IVFPQCfg(const int64_t& dim, const int64_t& k, const int64_t& gpu_id, const int64_t& nlist, const int64_t& nprobe,
             const int64_t& nbits, const int64_t& m, METRICTYPE type)
        : IVFCfg(dim, k, gpu_id, nlist, nprobe, type), m(m), nbits(nbits) {
    }

    IVFPQCfg() = default;

    //    bool
    //    CheckValid() override {
    //        return true;
    //    };
};
using IVFPQConfig = std::shared_ptr<IVFPQCfg>;

struct NSGCfg : public IVFCfg {
    int64_t knng = DEFAULT_NNG_K;
    int64_t search_length = DEFAULT_SEARCH_LENGTH;
    int64_t out_degree = DEFAULT_OUT_DEGREE;
    int64_t candidate_pool_size = DEFAULT_CANDIDATE_SISE;

    NSGCfg(const int64_t& dim, const int64_t& k, const int64_t& gpu_id, const int64_t& nlist, const int64_t& nprobe,
           const int64_t& knng, const int64_t& search_length, const int64_t& out_degree, const int64_t& candidate_size,
           METRICTYPE type)
        : IVFCfg(dim, k, gpu_id, nlist, nprobe, type),
          knng(knng),
          search_length(search_length),
          out_degree(out_degree),
          candidate_pool_size(candidate_size) {
    }

    NSGCfg() = default;

    std::stringstream
    DumpImpl() override;

    //    bool
    //    CheckValid() override {
    //        return true;
    //    };
};
using NSGConfig = std::shared_ptr<NSGCfg>;

struct SPTAGCfg : public Cfg {
    int64_t samples = DEFAULT_SAMPLES;
    int64_t tptnumber = DEFAULT_TPTNUMBER;
    int64_t tptleafsize = DEFAULT_TPTLEAFSIZE;
    int64_t numtopdimensiontptsplit = DEFAULT_NUMTOPDIMENSIONTPTSPLIT;
    int64_t neighborhoodsize = DEFAULT_NEIGHBORHOODSIZE;
    int64_t graphneighborhoodscale = DEFAULT_GRAPHNEIGHBORHOODSCALE;
    int64_t graphcefscale = DEFAULT_GRAPHCEFSCALE;
    int64_t refineiterations = DEFAULT_REFINEITERATIONS;
    int64_t cef = DEFAULT_CEF;
    int64_t maxcheckforrefinegraph = DEFAULT_MAXCHECKFORREFINEGRAPH;
    int64_t numofthreads = DEFAULT_NUMOFTHREADS;
    int64_t maxcheck = DEFAULT_MAXCHECK;
    int64_t thresholdofnumberofcontinuousnobetterpropagation = DEFAULT_THRESHOLDOFNUMBEROFCONTINUOUSNOBETTERPROPAGATION;
    int64_t numberofinitialdynamicpivots = DEFAULT_NUMBEROFINITIALDYNAMICPIVOTS;
    int64_t numberofotherdynamicpivots = DEFAULT_NUMBEROFOTHERDYNAMICPIVOTS;

    SPTAGCfg() = default;

    //    bool
    //    CheckValid() override {
    //        return true;
    //    };
};
using SPTAGConfig = std::shared_ptr<SPTAGCfg>;

struct KDTCfg : public SPTAGCfg {
    int64_t kdtnumber = DEFAULT_KDTNUMBER;
    int64_t numtopdimensionkdtsplit = DEFAULT_NUMTOPDIMENSIONKDTSPLIT;

    KDTCfg() = default;

    //    bool
    //    CheckValid() override {
    //        return true;
    //    };
};
using KDTConfig = std::shared_ptr<KDTCfg>;

struct BKTCfg : public SPTAGCfg {
    int64_t bktnumber = DEFAULT_BKTNUMBER;
    int64_t bktkmeansk = DEFAULT_BKTKMEANSK;
    int64_t bktleafsize = DEFAULT_BKTLEAFSIZE;

    BKTCfg() = default;

    //    bool
    //    CheckValid() override {
    //        return true;
    //    };
};
using BKTConfig = std::shared_ptr<BKTCfg>;

struct BinIDMAPCfg : public Cfg {
    bool
    CheckValid() override {
        if (metric_type == METRICTYPE::HAMMING || metric_type == METRICTYPE::TANIMOTO ||
            metric_type == METRICTYPE::JACCARD) {
            return true;
        }
        std::stringstream ss;
        ss << "MetricType: " << int(metric_type) << " not support!";
        KNOWHERE_THROW_MSG(ss.str());
        return false;
    }
};

struct HNSWCfg : public Cfg {
    int64_t M = DEFAULT_M;
    int64_t ef = DEFAULT_EF;

    HNSWCfg() = default;
};
using HNSWConfig = std::shared_ptr<HNSWCfg>;

}  // namespace knowhere
