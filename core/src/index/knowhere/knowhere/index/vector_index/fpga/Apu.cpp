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

#include "knowhere/index/vector_index/fpga/Apu.h"

#include <chrono>
#include <fstream>
#include <string>

#include "knowhere/common/Exception.h"
#include "utils/Log.h"

using milvus::GetThreadName;
using milvus::LogOut;
using milvus::knowhere::KnowhereException;

namespace Fpga {

ApuInterface::ApuInterface() {
    InitApu();
}

void
ApuInterface::InitApu() {
    unsigned int num_existing_cards = 0;
    unsigned int num_req_cards = 8;

    gsi_prod_status_t status = gdl_init();
    if (status != GSI_SUCCESS) {
        KNOWHERE_THROW_MSG("Apu init failed. error code : " + std::to_string(status));
    }

    status = gdl_context_count_get(&num_existing_cards);
    if (status != GSI_SUCCESS) {
        KNOWHERE_THROW_MSG("Apu failed to get number of cards. error code : " + std::to_string(status));
    }

    struct gdl_context_desc cards_desc[GDL_MAX_NUM_CONTEXTS] = {0};
    status = gdl_context_desc_get(cards_desc, num_req_cards);
    if (status != GSI_SUCCESS) {
        KNOWHERE_THROW_MSG("Apu failed to get cards description. error code : " + std::to_string(status));
    }

    unsigned int num_ready_cards = 0;
    gdl_context_handle_t cards_ids[GDL_MAX_NUM_CONTEXTS] = {0};
    for (unsigned int i = 0; i < num_existing_cards; ++i) {
        if (cards_desc[i].status == GDL_CONTEXT_READY) {
            cards_ids[num_ready_cards] = cards_desc[i].ctx_id;
            ++num_ready_cards;
        }
    }
    num_req_cards = (num_req_cards > num_ready_cards) ? num_ready_cards : num_req_cards;
    LOG_ENGINE_DEBUG_ << "Apu available cards: " << num_req_cards;

    gsl_ctx_ = NULL;
    unsigned int idx_first_occupied_card = 0;
    uint32_t max_num_threads = 0;
    while ((idx_first_occupied_card + num_req_cards) <= num_ready_cards) {
        status = gsl_create_context(&gsl_ctx_, &cards_ids[idx_first_occupied_card], num_req_cards, max_num_threads);
        if (0 == status) {
            break;
        } else if (ENOSYS == status) {
            break;
        }
        ++idx_first_occupied_card;
    }

    if (status != GSI_SUCCESS && status != ENOSYS) {
        KNOWHERE_THROW_MSG("Apu failed to create context. error code : " + std::to_string(status));
    }
}

void
ApuInterface::PopulateApuParams(uint32_t dimention, uint32_t row_count, const std::string location) {
    num_bfeatures_ = dimention;
    num_records_ = row_count;
    location_ = location;
    num_bytes_in_rec_ = num_bfeatures_ / CHAR_BITS;
}

void
ApuInterface::createBdb() {
    size_t db_size = num_bytes_in_rec_ * num_records_;
    bdb_ = {.row_size = num_bfeatures_,
            .row_stride = num_bytes_in_rec_,
            .num_rows = num_records_,
            .rows_u1 = malloc(db_size)};

    std::string records_file_name = location_ + ".rv";
    char* rec_file_name = &records_file_name[0];
    char* records_file_c = &(rec_file_name)[0];

    size_t file_size = 0;
    std::ifstream fin(records_file_c, std::ifstream::in | std::ifstream::binary);
    if (fin.is_open()) {
        fin.seekg(8, std::ios::beg);
        file_size = fin.tellg();
        fin.read((char*)bdb_.rows_u1, db_size);
        fin.close();
    }

    int status = 0;
    bdbh_ = 0;
    status = gsl_create_bdb(gsl_ctx_, &bdbh_, &bdb_);
    if (status != GSI_SUCCESS) {
        KNOWHERE_THROW_MSG("Apu failed create Bdb. error code : " + std::to_string(status));
    }
}

void
ApuInterface::loadHammingSessionToApu() {
    session_hdl_ = NULL;
    int status = 0;

    status = gsl_flat_hamming_create_search_session(gsl_ctx_, &session_hdl_, &hamming_desc_);
    if (status != GSI_SUCCESS) {
        KNOWHERE_THROW_MSG("Apu failed to create search session. error code : " + std::to_string(status));
    }

    status = gsl_search_in_focus(session_hdl_);
    if (status != GSI_SUCCESS) {
        KNOWHERE_THROW_MSG("Apu failed to focus search session. error code : " + std::to_string(status));
    }
}

void
ApuInterface::loadTanimotoSessionToApu() {
    session_hdl_ = NULL;
    int status = 0;

    status = gsl_flat_tanimoto_create_search_session(gsl_ctx_, &session_hdl_, &tanimoto_desc_);
    if (status != GSI_SUCCESS) {
        KNOWHERE_THROW_MSG("Apu failed to create search session. error code : " + std::to_string(status));
    }
    status = gsl_search_in_focus(session_hdl_);
    if (status != GSI_SUCCESS) {
        KNOWHERE_THROW_MSG("Apu failed to focus search session. error code : " + std::to_string(status));
    }
}

void
ApuInterface::Query(gsl_matrix_u32& indices, gsl_matrix_f32& distances, gsl_matrix_u1& queries, APU_METRIC_TYPE type) {
    int status;
    switch (type) {
        case APU_METRIC_TYPE::TANIMOTO: {
            auto start_t = std::chrono::steady_clock::now();
            status = gsl_flat_tanimoto_search_u1(session_hdl_, &indices, &distances, &queries);
            auto end_t = std::chrono::steady_clock::now();
            LOG_ENGINE_DEBUG_ << "Apu search time in microseconds: "
                              << std::chrono::duration_cast<std::chrono::microseconds>(end_t - start_t).count()
                              << " µs";
            break;
        }
        case APU_METRIC_TYPE::HAMMING:
            auto start_h = std::chrono::steady_clock::now();
            status = gsl_flat_hamming_search_u1(session_hdl_, &indices, &distances, &queries);
            auto end_h = std::chrono::steady_clock::now();
            LOG_ENGINE_DEBUG_ << "Apu search time in microseconds: "
                              << std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count()
                              << " µs";
            break;
    }
    if (status != GSI_SUCCESS) {
        KNOWHERE_THROW_MSG("Apu query Failed. error code : " + std::to_string(status));
    }
}

bool
ApuInterface::isLoadNeeded(std::string cur_location) {
    if (location_ == "") {
        return true;
    } else if (cur_location != location_) {
        cleanApuResources(APU_CLEAN_TYPE::NEW_DB);
        return true;
    }
    return false;
}

ApuInterface::~ApuInterface() {
    cleanApuResources(APU_CLEAN_TYPE::FULL);
}

void
ApuInterface::cleanApuResources(APU_CLEAN_TYPE type) {
    switch (type) {
        case APU_CLEAN_TYPE::FULL:
            if (session_hdl_)
                gsl_search_session_destroy(session_hdl_);
            if (bdbh_)
                gsl_destroy_bdb(bdbh_);
            gdl_exit();
            break;
        case APU_CLEAN_TYPE::SESSION_HDL:
            if (session_hdl_)
                gsl_search_session_destroy(session_hdl_);
            break;
        case APU_CLEAN_TYPE::BDBH:
            gsl_destroy_bdb(bdbh_);
            break;
        case APU_CLEAN_TYPE::NEW_DB:
            if (session_hdl_)
                gsl_search_session_destroy(session_hdl_);
            if (bdbh_)
                gsl_destroy_bdb(bdbh_);
            break;
    }
}

void
ApuInterface::loadSeesionToApu(APU_METRIC_TYPE type) {
    switch (type) {
        case APU_METRIC_TYPE::TANIMOTO: {
            tanimoto_desc_ = {.typical_num_queries = NUM_QUERIES,
                              .max_num_queries = NUM_QUERIES,
                              .tanimoto_bdbh = bdbh_,
                              .max_k = topK_};
            loadTanimotoSessionToApu();
        }
        case APU_METRIC_TYPE::HAMMING: {
            hamming_desc_ = {.typical_num_queries = NUM_QUERIES,
                             .max_num_queries = NUM_QUERIES,
                             .encoding = NULL,
                             .hamming_bdbh = bdbh_,
                             .max_k = topK_,
                             .rerank = NULL};
            loadHammingSessionToApu();
        }
    }
}

uint32_t
ApuInterface::getTopK() const {
    return topK_;
}

void
ApuInterface::setTopK(uint32_t topK) {
    topK_ = topK;
}
}  // namespace Fpga
