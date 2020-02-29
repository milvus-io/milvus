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

#include "wrapper/BinVecImpl.h"

#include "WrapperException.h"
#include "index/knowhere/knowhere/index/vector_index/IndexBinaryIVF.h"
#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexBinaryIDMAP.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

Status
BinVecImpl::BuildAll(const int64_t& nb, const uint8_t* xb, const int64_t* ids, const Config& cfg, const int64_t& nt,
                     const uint8_t* xt) {
    try {
        dim = cfg->d;

        auto ret_ds = std::make_shared<knowhere::Dataset>();
        ret_ds->Set(knowhere::meta::ROWS, nb);
        ret_ds->Set(knowhere::meta::DIM, dim);
        ret_ds->Set(knowhere::meta::TENSOR, xb);
        ret_ds->Set(knowhere::meta::IDS, ids);

        index_->Train(ret_ds, cfg);
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

Status
BinVecImpl::Search(const int64_t& nq, const uint8_t* xq, float* dist, int64_t* ids, const Config& cfg) {
    try {
        auto k = cfg->k;
        auto ret_ds = std::make_shared<knowhere::Dataset>();
        ret_ds->Set(knowhere::meta::ROWS, nq);
        ret_ds->Set(knowhere::meta::DIM, dim);
        ret_ds->Set(knowhere::meta::TENSOR, xq);

        Config search_cfg = cfg;

        auto res = index_->Search(ret_ds, search_cfg);
        //{
        //    auto& ids = ids_array;
        //    auto& dists = dis_array;
        //    std::stringstream ss_id;
        //    std::stringstream ss_dist;
        //    for (auto i = 0; i < 10; i++) {
        //        for (auto j = 0; j < k; ++j) {
        //            ss_id << *(ids->data()->GetValues<int64_t>(1, i * k + j)) << " ";
        //            ss_dist << *(dists->data()->GetValues<float>(1, i * k + j)) << " ";
        //        }
        //        ss_id << std::endl;
        //        ss_dist << std::endl;
        //    }
        //    std::cout << "id\n" << ss_id.str() << std::endl;
        //    std::cout << "dist\n" << ss_dist.str() << std::endl;
        //}

        //        auto p_ids = ids_array->data()->GetValues<int64_t>(1, 0);
        //        auto p_dist = dis_array->data()->GetValues<float>(1, 0);

        // TODO(linxj): avoid copy here.
        auto res_ids = res->Get<int64_t*>(knowhere::meta::IDS);
        auto res_dist = res->Get<float*>(knowhere::meta::DISTANCE);
        memcpy(ids, res_ids, sizeof(int64_t) * nq * k);
        memcpy(dist, res_dist, sizeof(float) * nq * k);
        free(res_ids);
        free(res_dist);
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

Status
BinVecImpl::Add(const int64_t& nb, const uint8_t* xb, const int64_t* ids, const Config& cfg) {
    try {
        auto ret_ds = std::make_shared<knowhere::Dataset>();
        ret_ds->Set(knowhere::meta::ROWS, nb);
        ret_ds->Set(knowhere::meta::DIM, dim);
        ret_ds->Set(knowhere::meta::TENSOR, xb);
        ret_ds->Set(knowhere::meta::IDS, ids);

        index_->Add(ret_ds, cfg);
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

VecIndexPtr
BinVecImpl::CopyToGpu(const int64_t& device_id, const Config& cfg) {
    const char* errmsg = "Binary Index not support CopyToGpu";
    WRAPPER_LOG_ERROR << errmsg;
    throw WrapperException(errmsg);
}

VecIndexPtr
BinVecImpl::CopyToCpu(const Config& cfg) {
    const char* errmsg = "Binary Index not support CopyToCpu";
    WRAPPER_LOG_ERROR << errmsg;
    throw WrapperException(errmsg);
}

Status
BinVecImpl::SetBlacklist(faiss::ConcurrentBitsetPtr list) {
    if (auto raw_index = std::dynamic_pointer_cast<knowhere::BinaryIVF>(index_)) {
        raw_index->SetBlacklist(list);
    } else if (auto raw_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(index_)) {
        raw_index->SetBlacklist(list);
    }
    return Status::OK();
}

Status
BinVecImpl::GetVectorById(const int64_t n, const int64_t* ids, uint8_t* x, const Config& cfg) {
    if (auto raw_index = std::dynamic_pointer_cast<knowhere::BinaryIVF>(index_)) {
    } else if (auto raw_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(index_)) {
    } else {
        throw WrapperException("not support");
    }
    try {
        auto ret_ds = std::make_shared<knowhere::Dataset>();
        ret_ds->Set(knowhere::meta::ROWS, n);
        ret_ds->Set(knowhere::meta::DIM, dim);
        ret_ds->Set(knowhere::meta::IDS, ids);

        Config search_cfg = cfg;

        auto res = index_->GetVectorById(ret_ds, search_cfg);

        // TODO(linxj): avoid copy here.
        auto res_x = res->Get<uint8_t*>(knowhere::meta::TENSOR);
        memcpy(x, res_x, sizeof(uint8_t) * n * dim);
        free(res_x);
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

Status
BinVecImpl::SearchById(const int64_t& nq, const int64_t* xq, float* dist, int64_t* ids, const Config& cfg) {
    if (auto raw_index = std::dynamic_pointer_cast<knowhere::BinaryIVF>(index_)) {
    } else if (auto raw_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(index_)) {
    } else {
        throw WrapperException("not support");
    }
    try {
        auto k = cfg->k;
        auto ret_ds = std::make_shared<knowhere::Dataset>();
        ret_ds->Set(knowhere::meta::ROWS, nq);
        ret_ds->Set(knowhere::meta::DIM, dim);
        ret_ds->Set(knowhere::meta::IDS, xq);

        Config search_cfg = cfg;

        auto res = index_->SearchById(ret_ds, search_cfg);
        //{
        //    auto& ids = ids_array;
        //    auto& dists = dis_array;
        //    std::stringstream ss_id;
        //    std::stringstream ss_dist;
        //    for (auto i = 0; i < 10; i++) {
        //        for (auto j = 0; j < k; ++j) {
        //            ss_id << *(ids->data()->GetValues<int64_t>(1, i * k + j)) << " ";
        //            ss_dist << *(dists->data()->GetValues<float>(1, i * k + j)) << " ";
        //        }
        //        ss_id << std::endl;
        //        ss_dist << std::endl;
        //    }
        //    std::cout << "id\n" << ss_id.str() << std::endl;
        //    std::cout << "dist\n" << ss_dist.str() << std::endl;
        //}

        //        auto p_ids = ids_array->data()->GetValues<int64_t>(1, 0);
        //        auto p_dist = dis_array->data()->GetValues<float>(1, 0);

        // TODO(linxj): avoid copy here.
        auto res_ids = res->Get<int64_t*>(knowhere::meta::IDS);
        auto res_dist = res->Get<float*>(knowhere::meta::DISTANCE);
        memcpy(ids, res_ids, sizeof(int64_t) * nq * k);
        memcpy(dist, res_dist, sizeof(float) * nq * k);
        free(res_ids);
        free(res_dist);
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

Status
BinVecImpl::GetBlacklist(faiss::ConcurrentBitsetPtr& list) {
    if (auto raw_index = std::dynamic_pointer_cast<knowhere::BinaryIVF>(index_)) {
        raw_index->GetBlacklist(list);
    } else if (auto raw_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(index_)) {
        raw_index->GetBlacklist(list);
    }
    return Status::OK();
}

ErrorCode
BinBFIndex::Build(const Config& cfg) {
    try {
        dim = cfg->d;
        std::static_pointer_cast<knowhere::BinaryIDMAP>(index_)->Train(cfg);
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return KNOWHERE_UNEXPECTED_ERROR;
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return KNOWHERE_ERROR;
    }
    return KNOWHERE_SUCCESS;
}

Status
BinBFIndex::BuildAll(const int64_t& nb, const uint8_t* xb, const int64_t* ids, const Config& cfg, const int64_t& nt,
                     const uint8_t* xt) {
    try {
        dim = cfg->d;
        auto ret_ds = std::make_shared<knowhere::Dataset>();
        ret_ds->Set(knowhere::meta::ROWS, nb);
        ret_ds->Set(knowhere::meta::DIM, dim);
        ret_ds->Set(knowhere::meta::TENSOR, xb);
        ret_ds->Set(knowhere::meta::IDS, ids);

        std::static_pointer_cast<knowhere::BinaryIDMAP>(index_)->Train(cfg);
        index_->Add(ret_ds, cfg);
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

const uint8_t*
BinBFIndex::GetRawVectors() {
    auto raw_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(index_);
    if (raw_index) {
        return raw_index->GetRawVectors();
    }
    return nullptr;
}

const int64_t*
BinBFIndex::GetRawIds() {
    return std::static_pointer_cast<knowhere::BinaryIDMAP>(index_)->GetRawIds();
}

Status
BinBFIndex::AddWithoutIds(const int64_t& nb, const uint8_t* xb, const Config& cfg) {
    auto ret_ds = std::make_shared<knowhere::Dataset>();
    ret_ds->Set(knowhere::meta::ROWS, nb);
    ret_ds->Set(knowhere::meta::DIM, dim);
    ret_ds->Set(knowhere::meta::TENSOR, xb);
    std::static_pointer_cast<knowhere::BinaryIDMAP>(index_)->AddWithoutId(ret_ds, cfg);
    return Status::OK();
}
}  // namespace engine
}  // namespace milvus
