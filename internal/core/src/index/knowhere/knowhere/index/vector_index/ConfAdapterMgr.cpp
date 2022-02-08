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

#include "knowhere/index/vector_index/ConfAdapterMgr.h"

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"

namespace milvus {
namespace knowhere {

ConfAdapterPtr
AdapterMgr::GetAdapter(const IndexType type) {
    auto register_wrapper = [&, this]() { RegisterAdapter(); };
    std::call_once(once_, register_wrapper);

    try {
        return collection_.at(type)();
    } catch (...) {
        KNOWHERE_THROW_MSG("Can not find confadapter: " + type);
    }
}

#define REGISTER_CONF_ADAPTER(T, TYPE, NAME) static AdapterMgr::register_t<T> reg_##NAME##_(TYPE)

void
AdapterMgr::RegisterAdapter() {
    REGISTER_CONF_ADAPTER(ConfAdapter, IndexEnum::INDEX_FAISS_IDMAP, idmap_adapter);
    REGISTER_CONF_ADAPTER(IVFConfAdapter, IndexEnum::INDEX_FAISS_IVFFLAT, ivf_adapter);
    REGISTER_CONF_ADAPTER(IVFPQConfAdapter, IndexEnum::INDEX_FAISS_IVFPQ, ivfpq_adapter);
    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexEnum::INDEX_FAISS_IVFSQ8, ivfsq8_adapter);
    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexEnum::INDEX_FAISS_IVFSQ8H, ivfsq8h_adapter);
    REGISTER_CONF_ADAPTER(IVFHNSWConfAdapter, IndexEnum::INDEX_FAISS_IVFHNSW, ivfhnsw_adapter);
    REGISTER_CONF_ADAPTER(BinIDMAPConfAdapter, IndexEnum::INDEX_FAISS_BIN_IDMAP, idmap_bin_adapter);
    REGISTER_CONF_ADAPTER(BinIVFConfAdapter, IndexEnum::INDEX_FAISS_BIN_IVFFLAT, ivf_bin_adapter);
    REGISTER_CONF_ADAPTER(NSGConfAdapter, IndexEnum::INDEX_NSG, nsg_adapter);
#ifdef MILVUS_SUPPORT_SPTAG
    REGISTER_CONF_ADAPTER(ConfAdapter, IndexEnum::INDEX_SPTAG_KDT_RNT, sptag_kdt_adapter);
    REGISTER_CONF_ADAPTER(ConfAdapter, IndexEnum::INDEX_SPTAG_BKT_RNT, sptag_bkt_adapter);
#endif
    REGISTER_CONF_ADAPTER(HNSWConfAdapter, IndexEnum::INDEX_HNSW, hnsw_adapter);
    REGISTER_CONF_ADAPTER(ANNOYConfAdapter, IndexEnum::INDEX_ANNOY, annoy_adapter);
    REGISTER_CONF_ADAPTER(RHNSWFlatConfAdapter, IndexEnum::INDEX_RHNSWFlat, rhnswflat_adapter);
    REGISTER_CONF_ADAPTER(RHNSWPQConfAdapter, IndexEnum::INDEX_RHNSWPQ, rhnswpq_adapter);
    REGISTER_CONF_ADAPTER(RHNSWSQConfAdapter, IndexEnum::INDEX_RHNSWSQ, rhnswsq_adapter);
    REGISTER_CONF_ADAPTER(NGTPANNGConfAdapter, IndexEnum::INDEX_NGTPANNG, ngtpanng_adapter);
    REGISTER_CONF_ADAPTER(NGTONNGConfAdapter, IndexEnum::INDEX_NGTONNG, ngtonng_adapter);

    // though init_ won't be used later, it's better to set `init_` to true after registration was done.
    init_ = true;
}

}  // namespace knowhere
}  // namespace milvus
