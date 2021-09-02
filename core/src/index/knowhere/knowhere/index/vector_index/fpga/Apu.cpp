//
// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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

#include <db/Utils.h>
#include <db/engine/ExecutionEngine.h>
#include <segment/DeletedDocs.h>
#include <segment/SegmentReader.h>
#include "GsiBaseIndex.h"
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
ApuInterface::load(uint32_t dimention, uint32_t row_count, const std::string location, APU_METRIC_TYPE type,
                   const std::string& collection_name) {
    std::lock_guard<std::mutex> apu_lock(operation_mutex_);
    cleanApuResources(Fpga::APU_CLEAN_TYPE::NEW_DB);
    PopulateApuParams(dimention, row_count, location);
    createBdb();
    loadSessionToApu(type);
    removeDeleteDocsFromApu();
    num_records_ -= delete_docs_.size();
    loaded_collection_name_ = collection_name;
}

void
ApuInterface::Query(gsl_matrix_u32& indices, gsl_matrix_f32& distances, gsl_matrix_u1& queries, APU_METRIC_TYPE type,
                    uint32_t topK) {
    std::lock_guard<std::mutex> apu_lock(operation_mutex_);
    if (tanimoto_reject_ind) {
        KNOWHERE_THROW_MSG("Apu search failed. Add/Remove is not permitted for Tanimoto collection");
    }

    int status;
    topK_ = topK;
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

milvus::Status
ApuInterface::insertVectors(milvus::engine::VectorsData& vectors, std::string collection_name) {
    std::lock_guard<std::mutex> apu_lock(operation_mutex_);
    auto binary_data = vectors.binary_data_;
    if (loaded_collection_name_ != collection_name || binary_data.empty()) {
        return milvus::Status().OK();
    }

    int vectors_count = binary_data.size() / num_bytes_in_rec_;
    activities_counter_ += vectors_count;
    auto gsi_index = std::static_pointer_cast<milvus::knowhere::GsiBaseIndex>(index_);
    if (gsi_index->getMetricType() == milvus::engine::MetricType::TANIMOTO) {
        tanimoto_reject_ind = true;
        return milvus::Status(-1, "Add/Remove action from loaded Tanimoto collection is not supported");
    } else if (activities_counter_ > MAX_ACTIVITIES_BEFORE_RELOAD) {
        cleanApuResources(APU_CLEAN_TYPE::NEW_DB);
        return milvus::Status().OK();
    }

    gsl_matrix_u1 records_to_add = {.row_size = num_bfeatures_,
                                    .row_stride = num_bytes_in_rec_,
                                    .num_rows = (uint32_t)vectors_count,
                                    .rows_u1 = records_to_add.rows_u1 = (void*)binary_data.data()};

    int status = gsl_flat_hamming_append_recs_u1(session_hdl_, &records_to_add);
    if (status != 0) {
        return milvus::Status(status, "Error occuered while adding records to APU");
    }

    auto ids = index_->GetUids();
    ids->insert(ids->end(), vectors.id_array_.begin(), vectors.id_array_.end());
    return milvus::Status().OK();
}

void
ApuInterface::PopulateApuParams(uint32_t dimention, uint32_t row_count, const std::string location) {
    num_bfeatures_ = dimention;
    location_ = location;
    getDeletedDocs(location_, delete_docs_);
    num_records_ = row_count + delete_docs_.size();
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

    int status = gsl_create_bdb(gsl_ctx_, &bdbh_, &bdb_);
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

ApuInterface::~ApuInterface() {
    cleanApuResources(APU_CLEAN_TYPE::FULL);
}

void
ApuInterface::cleanApuResources(APU_CLEAN_TYPE type) {
    std::lock_guard<std::mutex> apu_lock(cleanup_mutex_);
    switch (type) {
        case APU_CLEAN_TYPE::FULL:
            if (session_hdl_)
                gsl_search_session_destroy(session_hdl_);
            if (bdbh_)
                gsl_destroy_bdb(bdbh_);
            gdl_exit();
            loaded_collection_name_ = "";
            break;
        case APU_CLEAN_TYPE::SESSION_HDL:
            if (session_hdl_)
                gsl_search_session_destroy(session_hdl_);
            break;
        case APU_CLEAN_TYPE::BDBH:
            gsl_destroy_bdb(bdbh_);
            break;
        case APU_CLEAN_TYPE::NEW_DB:
            if (session_hdl_) {
                gsl_search_session_destroy(session_hdl_);
                session_hdl_ = NULL;
            }
            if (bdbh_) {
                gsl_destroy_bdb(bdbh_);
                bdbh_ = NULL;
            }
            loaded_collection_name_ = "";
            tanimoto_reject_ind = false;
            activities_counter_ = 0;
            break;
    }
}

void
ApuInterface::loadSessionToApu(APU_METRIC_TYPE type) {
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

bool
ApuInterface::getApuLoadStatus(std::string collection_name) {
    std::lock_guard<std::mutex> apu_lock(operation_mutex_);
    bool is_loaded = true;
    if (loaded_collection_name_ != collection_name) {
        is_loaded = false;
    }
    return is_loaded;
}

void
ApuInterface::setIndex(const milvus::knowhere::VecIndexPtr& index) {
    std::lock_guard<std::mutex> apu_lock(operation_mutex_);
    index_ = index;
}

void
ApuInterface::removeDeleteDocsFromApu() {
    uint32_t num_of_offsets = delete_docs_.size();
    auto gsi_index = std::static_pointer_cast<milvus::knowhere::GsiBaseIndex>(index_);
    if (num_of_offsets == 0) {
        return;
    } else if (num_of_offsets > 0 && gsi_index->getMetricType() == milvus::engine::MetricType::TANIMOTO) {
        tanimoto_reject_ind = true;
        KNOWHERE_THROW_MSG("Apu load failed. Remove from Tanimoto collection is not permitted");
    }

    activities_counter_ += num_of_offsets;
    if (activities_counter_ > MAX_ACTIVITIES_BEFORE_RELOAD) {
        cleanApuResources(APU_CLEAN_TYPE::NEW_DB);
        return;
    }
    gsl_matrix_u32 offsets_to_remove;
    gsl_matrix_u32 moved_items;
    getDeleteObjects(num_of_offsets, offsets_to_remove, moved_items, (uint32_t*)delete_docs_.data());
    // delete records from apu according to offsets
    int status = gsl_flat_hamming_remove_recs(session_hdl_, &moved_items, &offsets_to_remove);

    if (status == 0) {
        alignApuUids(moved_items, delete_docs_.size());
    }
}

void
ApuInterface::alignApuUids(gsl_matrix_u32& moved_items, int record_count) const {
    if (record_count == 0) {
        return;
    }

    uint32_t* tmpPtr = NULL;
    uint32_t old_pos;
    uint32_t new_pos;

    auto uids = index_->GetUids();
    for (int i = 0; i < record_count; ++i) {
        tmpPtr = (uint32_t*)((char*)moved_items.rows_u32 + moved_items.row_stride * i);
        old_pos = *tmpPtr;
        new_pos = *(++tmpPtr);
        if (new_pos != INDEX_SENTINEL_VALUE && old_pos != INDEX_SENTINEL_VALUE) {
            uids->at(new_pos) = uids->at(old_pos);
            uids->erase(uids->begin() + old_pos);
        } else {
            uids->pop_back();
        }
    }

    free((void*)moved_items.rows_u32);
}

milvus::Status
ApuInterface::deleteVectors(milvus::engine::IDNumbers vector_ids, std::string collection_name) {
    std::lock_guard<std::mutex> apu_lock(operation_mutex_);

    uint32_t num_of_records = vector_ids.size();
    if (loaded_collection_name_ != collection_name || num_of_records == 0) {
        return milvus::Status().OK();
    }
    std::vector<uint32_t> offsets;
    offsets.resize(num_of_records);
    int records_found = getOffsetByValue(index_->GetUids(), vector_ids, offsets);

    activities_counter_ += records_found;
    auto gsi_index = std::static_pointer_cast<milvus::knowhere::GsiBaseIndex>(index_);
    if (gsi_index->getMetricType() == milvus::engine::MetricType::TANIMOTO) {
        tanimoto_reject_ind = true;
        return milvus::Status(-1, "Add/Remove action from loaded Tanimoto collection is not supported");
    } else if (activities_counter_ > MAX_ACTIVITIES_BEFORE_RELOAD) {
        cleanApuResources(APU_CLEAN_TYPE::NEW_DB);
        return milvus::Status().OK();
    } else if (records_found == 0) {
        return milvus::Status().OK();
    }

    gsl_matrix_u32 offsets_to_remove;
    gsl_matrix_u32 moved_items;
    getDeleteObjects(records_found, offsets_to_remove, moved_items, offsets.data());

    int status = gsl_flat_hamming_remove_recs(session_hdl_, &moved_items, &offsets_to_remove);

    if (status == 0) {
        alignApuUids(moved_items, records_found);
    }
    return milvus::Status::OK();
}

void
ApuInterface::getDeleteObjects(uint32_t num_of_offsets, gsl_matrix_u32& offsets_to_remove, gsl_matrix_u32& moved_items,
                               uint32_t* data) const {
    offsets_to_remove = {
        .row_size = num_of_offsets, .row_stride = num_of_offsets * sizeof(uint32_t), .num_rows = 1, .rows_u32 = data};
    moved_items = {.row_size = 2,
                   .row_stride = 2 * sizeof(uint32_t),
                   .num_rows = num_of_offsets,
                   .rows_u32 = (uint32_t*)calloc(num_of_offsets * 2, sizeof(uint32_t))};
}

const milvus::knowhere::VecIndexPtr&
ApuInterface::getIndex() const {
    return index_;
}

milvus::Status
ApuInterface::dropCollection(std::string collection_name) {
    std::lock_guard<std::mutex> apu_lock(operation_mutex_);
    if (collection_name == loaded_collection_name_) {
        loaded_collection_name_ = "";
        tanimoto_reject_ind = false;
    }
    return milvus::Status::OK();
}

}  // namespace Fpga
