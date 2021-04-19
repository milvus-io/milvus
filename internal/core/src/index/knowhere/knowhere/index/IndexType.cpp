// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <unordered_map>

#include "knowhere/common/Exception.h"
#include "knowhere/index/IndexType.h"

namespace milvus {
namespace knowhere {

/* used in 0.8.0 */
namespace IndexEnum {
const char* INVALID = "";
const char* INDEX_FAISS_IDMAP = "FLAT";
const char* INDEX_FAISS_IVFFLAT = "IVF_FLAT";
const char* INDEX_FAISS_IVFPQ = "IVF_PQ";
const char* INDEX_FAISS_IVFSQ8 = "IVF_SQ8";
const char* INDEX_FAISS_IVFSQ8H = "IVF_SQ8_HYBRID";
const char* INDEX_FAISS_IVFHNSW = "IVF_HNSW";
const char* INDEX_FAISS_BIN_IDMAP = "BIN_FLAT";
const char* INDEX_FAISS_BIN_IVFFLAT = "BIN_IVF_FLAT";
const char* INDEX_NSG = "NSG";
#ifdef MILVUS_SUPPORT_SPTAG
const char* INDEX_SPTAG_KDT_RNT = "SPTAG_KDT_RNT";
const char* INDEX_SPTAG_BKT_RNT = "SPTAG_BKT_RNT";
#endif
const char* INDEX_HNSW = "HNSW";
const char* INDEX_RHNSWFlat = "RHNSW_FLAT";
const char* INDEX_RHNSWPQ = "RHNSW_PQ";
const char* INDEX_RHNSWSQ = "RHNSW_SQ";
const char* INDEX_ANNOY = "ANNOY";
const char* INDEX_NGTPANNG = "NGT_PANNG";
const char* INDEX_NGTONNG = "NGT_ONNG";
}  // namespace IndexEnum

}  // namespace knowhere
}  // namespace milvus
