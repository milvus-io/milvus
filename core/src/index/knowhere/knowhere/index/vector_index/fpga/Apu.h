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
#include <mutex>
#include <string>
#include <vector>

#include <db/Types.h>
#include <knowhere/common/Typedef.h>
#include <utils/Status.h>
#include "knowhere/index/vector_index/fpga/ApuUtils.h"
#include "segment/DeletedDocs.h"

extern "C" {
#include <gsi/libgdl.h>
#include <gsi/libgsl.h>
#include <gsi/libgsl_flat_hamming.h>
#include <gsi/libgsl_flat_tanimoto.h>
#include <gsi/libgsl_matrix.h>
}

#define NUM_QUERIES 100
#define CHAR_BITS 8
#define INIT_K 50
#define GSI_SUCCESS 0
#define MAX_ACTIVITIES_BEFORE_RELOAD 4000
#define INDEX_SENTINEL_VALUE 4294967295

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
    load(uint32_t dimention, uint32_t row_count, const std::string location, APU_METRIC_TYPE type,
         const std::string& collection_name);

    void
    Query(gsl_matrix_u32& indices, gsl_matrix_f32& distances, gsl_matrix_u1& queries, APU_METRIC_TYPE type,
          uint32_t topK);

    milvus::Status
    insertVectors(milvus::engine::VectorsData& vectors, std::string collection_name);

    milvus::Status
    deleteVectors(milvus::engine::IDNumbers vector_ids, std::string collection_name);

    bool
    getApuLoadStatus(std::string collection_name);

    void
    setIndex(const milvus::knowhere::VecIndexPtr& index);

    const milvus::knowhere::VecIndexPtr&
    getIndex() const;

    milvus::Status
    dropCollection(std::string collection_name);

 private:
    void
    InitApu();

    void
    cleanApuResources(APU_CLEAN_TYPE type);

    void
    loadHammingSessionToApu();

    void
    loadTanimotoSessionToApu();

    void
    PopulateApuParams(uint32_t dimention, uint32_t row_count, const std::string location);

    void
    createBdb();

    void
    loadSessionToApu(APU_METRIC_TYPE type);

    void
    removeDeleteDocsFromApu();

    void
    alignApuUids(gsl_matrix_u32& moved_items, int record_count) const;

    void
    getDeleteObjects(uint32_t num_of_offsets, gsl_matrix_u32& offsets_to_remove, gsl_matrix_u32& moved_items,
                     uint32_t* data) const;

    std::string location_ = "";

    int32_t metric_type_;

    std::string loaded_collection_name_ = "";

    int activities_counter_ = 0;

    uint32_t num_records_ = 0;

    uint32_t num_bfeatures_ = 0;

    uint32_t num_bytes_in_rec_ = 0;

    gsl_context gsl_ctx_;

    gsl_matrix_u1 bdb_;

    gsl_bdb_hdl bdbh_ = NULL;

    gsl_search_session_hdl session_hdl_ = NULL;

    gsl_flat_hamming_desc hamming_desc_;

    gsl_flat_tanimoto_desc tanimoto_desc_;

    uint32_t topK_ = INIT_K;

    milvus::knowhere::VecIndexPtr index_ = nullptr;

    std::vector<milvus::segment::offset_t> delete_docs_;

    std::mutex operation_mutex_;

    std::mutex cleanup_mutex_;

    bool tanimoto_reject_ind = false;
};  // namespace ApuInterface
using ApuInterfacePtr = std::shared_ptr<ApuInterface>;
}  // namespace Fpga
#endif  // MILVUS_ENGINE_APU_H
#endif
