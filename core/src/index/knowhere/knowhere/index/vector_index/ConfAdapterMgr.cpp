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
AdapterMgr::GetAdapter(const IndexType& indexType) {
    if (!init_)
        RegisterAdapter();

    auto it = table_.find(indexType);
    if (it != table_.end()) {
        return it->second();
    } else {
        KNOWHERE_THROW_MSG("Can not find this type of confadapter");
    }
}

#define REGISTER_CONF_ADAPTER(T, KEY, NAME) static AdapterMgr::register_t<T> reg_##NAME##_(KEY)

void
AdapterMgr::RegisterAdapter() {
    init_ = true;

    REGISTER_CONF_ADAPTER(ConfAdapter, IndexType::INDEX_FAISS_IDMAP, idmap);
    REGISTER_CONF_ADAPTER(BinIDMAPConfAdapter, IndexType::INDEX_FAISS_BIN_IDMAP, idmap_bin);

    REGISTER_CONF_ADAPTER(IVFConfAdapter, IndexType::INDEX_FAISS_IVFFLAT, ivf);
    REGISTER_CONF_ADAPTER(BinIVFConfAdapter, IndexType::INDEX_FAISS_BIN_IVFFLAT, ivf_bin);

    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexType::INDEX_FAISS_IVFSQ8, ivfsq8);
    REGISTER_CONF_ADAPTER(IVFSQConfAdapter, IndexType::INDEX_FAISS_IVFSQ8H, ivfsq8_hybrid);

    REGISTER_CONF_ADAPTER(IVFPQConfAdapter, IndexType::INDEX_FAISS_IVFPQ, ivfpq);

    REGISTER_CONF_ADAPTER(NSGConfAdapter, IndexType::INDEX_NSG, nsg_mix);

    REGISTER_CONF_ADAPTER(ConfAdapter, IndexType::INDEX_SPTAG_BKT_RNT, sptag_bkt);
    REGISTER_CONF_ADAPTER(ConfAdapter, IndexType::INDEX_SPTAG_KDT_RNT, sptag_kdt);

    REGISTER_CONF_ADAPTER(HNSWConfAdapter, IndexType::INDEX_HNSW, hnsw);
}

}  // namespace knowhere
}  // namespace milvus
