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

#include "server/web_impl/Constants.h"

namespace milvus {
namespace server {
namespace web {

const char* NAME_ENGINE_TYPE_FLAT = "FLAT";
const char* NAME_ENGINE_TYPE_IVFFLAT = "IVF_FLAT";
const char* NAME_ENGINE_TYPE_IVFSQ8 = "IVF_SQ8";
const char* NAME_ENGINE_TYPE_IVFSQ8H = "IVF_SQ8H";
const char* NAME_ENGINE_TYPE_RNSG = "RNSG";
const char* NAME_ENGINE_TYPE_IVFPQ = "IVF_PQ";
const char* NAME_ENGINE_TYPE_HNSW = "HNSW";
const char* NAME_ENGINE_TYPE_ANNOY = "ANNOY";
const char* NAME_ENGINE_TYPE_RHNSWFLAT = "RHNSW_FLAT";
const char* NAME_ENGINE_TYPE_RHNSWPQ = "RHNSW_PQ";
const char* NAME_ENGINE_TYPE_RHNSWSQ8 = "RHNSW_SQ8";
const char* NAME_ENGINE_TYPE_NGTPANNG = "NGTPANNG";
const char* NAME_ENGINE_TYPE_NGTONNG = "NGTONNG";

const char* NAME_METRIC_TYPE_L2 = "L2";
const char* NAME_METRIC_TYPE_IP = "IP";
const char* NAME_METRIC_TYPE_HAMMING = "HAMMING";
const char* NAME_METRIC_TYPE_JACCARD = "JACCARD";
const char* NAME_METRIC_TYPE_TANIMOTO = "TANIMOTO";
const char* NAME_METRIC_TYPE_SUBSTRUCTURE = "SUBSTRUCTURE";
const char* NAME_METRIC_TYPE_SUPERSTRUCTURE = "SUPERSTRUCTURE";

////////////////////////////////////////////////////
const int64_t VALUE_COLLECTION_INDEX_FILE_SIZE_DEFAULT = 1024;
const char* VALUE_COLLECTION_METRIC_TYPE_DEFAULT = "L2";

const char* VALUE_PARTITION_TAG_DEFAULT = "";

const char* VALUE_INDEX_INDEX_TYPE_DEFAULT = NAME_ENGINE_TYPE_FLAT;
const int64_t VALUE_INDEX_NLIST_DEFAULT = 16384;

const int64_t VALUE_CONFIG_CPU_CACHE_CAPACITY_DEFAULT = 4;
const bool VALUE_CONFIG_CACHE_INSERT_DATA_DEFAULT = false;

const char* NAME_ID = "__id";

/////////////////////////////////////////////////////

}  // namespace web
}  // namespace server
}  // namespace milvus
