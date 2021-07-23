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

#ifdef MILVUS_APU_VERSION
#ifndef APU_H
#define APU_H

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>

extern "C" {
#include <gsi/libgdl.h>
#include <gsi/libgsl.h>
#include <gsi/libgsl_flat_tanimoto.h>
#include <gsi/libgsl_matrix.h>
}

#define NUM_QUERIES (100)
#define CHAR_BITS (8)
#define INIT_K (50)
#define GSI_SUCCESS (0)

namespace Fpga {

enum class APU_CLEAN_TYPE {
    SESSION_HDL = 0,
    BDBH = 1,
    FULL,
    NEW_DB,
};

enum class APU_METRIC_TYPE {
    HAMMING = 0,
    TANIMOTO = 1,
};

class ApuInterface {
 public:
    ApuInterface();

    ~ApuInterface();

    void
    PopulateApuParams(uint32_t dimention, uint32_t row_count, const std::string location);

    void
    createBdb();

    void
    loadSeesionToApu(APU_METRIC_TYPE type);

    void
    Query(gsl_matrix_u32& indices, gsl_matrix_f32& distances, gsl_matrix_u1& queries, APU_METRIC_TYPE type);

    bool
    isLoadNeeded(std::string location_);

    void
    cleanApuResources(APU_CLEAN_TYPE type);

    uint32_t
    getTopK() const;

    void
    setTopK(uint32_t topK);

 private:
    gsl_bdb_hdl bdbh_ = NULL;

    void
    InitApu();

    void
    loadHammingSessionToApu();

    void
    loadTanimotoSessionToApu();

    std::string location_ = "";

    uint32_t num_records_ = 0;

    uint32_t num_bfeatures_ = 0;

    uint32_t num_bytes_in_rec_ = 0;

    gsl_context gsl_ctx_;

    struct gsl_matrix_u1 bdb_;

    gsl_search_session_hdl session_hdl_;

    gsl_flat_hamming_desc hamming_desc_;

    gsl_flat_tanimoto_desc tanimoto_desc_;

    uint32_t topK_ = INIT_K;
};  // namespace ApuInterface
using ApuInterfacePtr = std::shared_ptr<ApuInterface>;
}  // namespace Fpga
#endif  // MILVUS_ENGINE_APU_H
#endif
