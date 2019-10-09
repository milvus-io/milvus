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


#include "wrapper/VecImpl.h"
#include "utils/Log.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "DataTransfer.h"

/*
 * no parameter check in this layer.
 * only responible for index combination
 */

namespace zilliz {
namespace milvus {
namespace engine {

Status
VecIndexImpl::BuildAll(const int64_t &nb,
                       const float *xb,
                       const int64_t *ids,
                       const Config &cfg,
                       const int64_t &nt,
                       const float *xt) {
    try {
        dim = cfg->d;
        auto dataset = GenDatasetWithIds(nb, dim, xb, ids);

        auto preprocessor = index_->BuildPreprocessor(dataset, cfg);
        index_->set_preprocessor(preprocessor);
        auto model = index_->Train(dataset, cfg);
        index_->set_index_model(model);
        index_->Add(dataset, cfg);
    } catch (knowhere::KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

Status
VecIndexImpl::Add(const int64_t &nb, const float *xb, const int64_t *ids, const Config &cfg) {
    try {
        auto dataset = GenDatasetWithIds(nb, dim, xb, ids);

        index_->Add(dataset, cfg);
    } catch (knowhere::KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

Status
VecIndexImpl::Search(const int64_t &nq, const float *xq, float *dist, int64_t *ids, const Config &cfg) {
    try {
        auto k = cfg->k;
        auto dataset = GenDataset(nq, dim, xq);

        Config search_cfg = cfg;

        auto res = index_->Search(dataset, search_cfg);
        auto ids_array = res->array()[0];
        auto dis_array = res->array()[1];

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

        auto p_ids = ids_array->data()->GetValues<int64_t>(1, 0);
        auto p_dist = dis_array->data()->GetValues<float>(1, 0);

        // TODO(linxj): avoid copy here.
        memcpy(ids, p_ids, sizeof(int64_t) * nq * k);
        memcpy(dist, p_dist, sizeof(float) * nq * k);
    } catch (knowhere::KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

zilliz::knowhere::BinarySet
VecIndexImpl::Serialize() {
    type = ConvertToCpuIndexType(type);
    return index_->Serialize();
}

Status
VecIndexImpl::Load(const zilliz::knowhere::BinarySet &index_binary) {
    index_->Load(index_binary);
    dim = Dimension();
    return Status::OK();
}

int64_t
VecIndexImpl::Dimension() {
    return index_->Dimension();
}

int64_t
VecIndexImpl::Count() {
    return index_->Count();
}

IndexType
VecIndexImpl::GetType() {
    return type;
}

VecIndexPtr
VecIndexImpl::CopyToGpu(const int64_t &device_id, const Config &cfg) {
    // TODO(linxj): exception handle
    auto gpu_index = zilliz::knowhere::cloner::CopyCpuToGpu(index_, device_id, cfg);
    auto new_index = std::make_shared<VecIndexImpl>(gpu_index, ConvertToGpuIndexType(type));
    new_index->dim = dim;
    return new_index;
}

VecIndexPtr
VecIndexImpl::CopyToCpu(const Config &cfg) {
    // TODO(linxj): exception handle
    auto cpu_index = zilliz::knowhere::cloner::CopyGpuToCpu(index_, cfg);
    auto new_index = std::make_shared<VecIndexImpl>(cpu_index, ConvertToCpuIndexType(type));
    new_index->dim = dim;
    return new_index;
}

VecIndexPtr
VecIndexImpl::Clone() {
    // TODO(linxj): exception handle
    auto clone_index = std::make_shared<VecIndexImpl>(index_->Clone(), type);
    clone_index->dim = dim;
    return clone_index;
}

int64_t
VecIndexImpl::GetDeviceId() {
    if (auto device_idx = std::dynamic_pointer_cast<knowhere::GPUIndex>(index_)) {
        return device_idx->GetGpuDevice();
    }
    // else
    return -1; // -1 == cpu
}

float *
BFIndex::GetRawVectors() {
    auto raw_index = std::dynamic_pointer_cast<knowhere::IDMAP>(index_);
    if (raw_index) { return raw_index->GetRawVectors(); }
    return nullptr;
}

int64_t *
BFIndex::GetRawIds() {
    return std::static_pointer_cast<knowhere::IDMAP>(index_)->GetRawIds();
}

ErrorCode
BFIndex::Build(const Config &cfg) {
    try {
        dim = cfg->d;
        std::static_pointer_cast<knowhere::IDMAP>(index_)->Train(cfg);
    } catch (knowhere::KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return KNOWHERE_UNEXPECTED_ERROR;
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return KNOWHERE_ERROR;
    }
    return KNOWHERE_SUCCESS;
}

Status
BFIndex::BuildAll(const int64_t &nb,
                  const float *xb,
                  const int64_t *ids,
                  const Config &cfg,
                  const int64_t &nt,
                  const float *xt) {
    try {
        dim = cfg->d;
        auto dataset = GenDatasetWithIds(nb, dim, xb, ids);

        std::static_pointer_cast<knowhere::IDMAP>(index_)->Train(cfg);
        index_->Add(dataset, cfg);
    } catch (knowhere::KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

// TODO(linxj): add lock here.
Status
IVFMixIndex::BuildAll(const int64_t &nb,
                      const float *xb,
                      const int64_t *ids,
                      const Config &cfg,
                      const int64_t &nt,
                      const float *xt) {
    try {
        dim = cfg->d;
        auto dataset = GenDatasetWithIds(nb, dim, xb, ids);

        auto preprocessor = index_->BuildPreprocessor(dataset, cfg);
        index_->set_preprocessor(preprocessor);
        auto model = index_->Train(dataset, cfg);
        index_->set_index_model(model);
        index_->Add(dataset, cfg);

        if (auto device_index = std::dynamic_pointer_cast<knowhere::GPUIndex>(index_)) {
            auto host_index = device_index->CopyGpuToCpu(Config());
            index_ = host_index;
            type = ConvertToCpuIndexType(type);
        } else {
            WRAPPER_LOG_ERROR << "Build IVFMIXIndex Failed";
            return Status(KNOWHERE_ERROR, "Build IVFMIXIndex Failed");
        }
    } catch (knowhere::KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
    return Status::OK();
}

Status
IVFMixIndex::Load(const zilliz::knowhere::BinarySet &index_binary) {
    index_->Load(index_binary);
    dim = Dimension();
    return Status::OK();
}

knowhere::QuantizerPtr
IVFHybridIndex::LoadQuantizer(const Config& conf) {
    // TODO(linxj): Hardcode here
    if (auto new_idx = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_)){
        return new_idx->LoadQuantizer(conf);
    } else {
        WRAPPER_LOG_ERROR << "Hybrid mode not support for index type: " << int(type);
    }
}

Status
IVFHybridIndex::SetQuantizer(const knowhere::QuantizerPtr& q) {
    try {
        // TODO(linxj): Hardcode here
        if (auto new_idx = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_)) {
            new_idx->SetQuantizer(q);
        } else {
            WRAPPER_LOG_ERROR << "Hybrid mode not support for index type: " << int(type);
            return Status(KNOWHERE_ERROR, "not support");
        }
    } catch (knowhere::KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
}

Status
IVFHybridIndex::UnsetQuantizer() {
    try {
        // TODO(linxj): Hardcode here
        if (auto new_idx = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_)) {
            new_idx->UnsetQuantizer();
        } else {
            WRAPPER_LOG_ERROR << "Hybrid mode not support for index type: " << int(type);
            return Status(KNOWHERE_ERROR, "not support");
        }
    } catch (knowhere::KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return Status(KNOWHERE_ERROR, e.what());
    }
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
