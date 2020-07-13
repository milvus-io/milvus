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
#include "knowhere/index/IndexType.h"

namespace milvus {
namespace server {
namespace web {

const char* NAME_ENGINE_TYPE_FLAT = knowhere::IndexEnum::INDEX_FAISS_IDMAP;
const char* NAME_ENGINE_TYPE_IVFFLAT = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
const char* NAME_ENGINE_TYPE_IVFSQ8 = knowhere::IndexEnum::INDEX_FAISS_IVFSQ8;
const char* NAME_ENGINE_TYPE_IVFSQ8NR = knowhere::IndexEnum::INDEX_FAISS_IVFSQ8NR;
const char* NAME_ENGINE_TYPE_IVFSQ8H = knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H;
const char* NAME_ENGINE_TYPE_RNSG = knowhere::IndexEnum::INDEX_NSG;
const char* NAME_ENGINE_TYPE_IVFPQ = knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
const char* NAME_ENGINE_TYPE_HNSW = knowhere::IndexEnum::INDEX_HNSW;
const char* NAME_ENGINE_TYPE_ANNOY = knowhere::IndexEnum::INDEX_ANNOY;
const char* NAME_ENGINE_TYPE_HNSWSQ8NR = knowhere::IndexEnum::INDEX_HNSW_SQ8NR;

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

/////////////////////////////////////////////////////
const std::unordered_map<engine::meta::EngineType, std::string> IndexMap = {
    {engine::meta::EngineType::FAISS_IDMAP, NAME_ENGINE_TYPE_FLAT},
    {engine::meta::EngineType::FAISS_IVFFLAT, NAME_ENGINE_TYPE_IVFFLAT},
    {engine::meta::EngineType::FAISS_IVFSQ8, NAME_ENGINE_TYPE_IVFSQ8},
    {engine::meta::EngineType::FAISS_IVFSQ8H, NAME_ENGINE_TYPE_IVFSQ8H},
    {engine::meta::EngineType::NSG_MIX, NAME_ENGINE_TYPE_RNSG},
    {engine::meta::EngineType::FAISS_PQ, NAME_ENGINE_TYPE_IVFPQ},
    {engine::meta::EngineType::HNSW, NAME_ENGINE_TYPE_HNSW},
    {engine::meta::EngineType::ANNOY, NAME_ENGINE_TYPE_ANNOY},
    {engine::meta::EngineType::FAISS_IVFSQ8NR, NAME_ENGINE_TYPE_IVFSQ8NR},
    {engine::meta::EngineType::HNSW_SQ8NR, NAME_ENGINE_TYPE_HNSWSQ8NR}};

const std::unordered_map<std::string, engine::meta::EngineType> IndexNameMap = {
    {NAME_ENGINE_TYPE_FLAT, engine::meta::EngineType::FAISS_IDMAP},
    {NAME_ENGINE_TYPE_IVFFLAT, engine::meta::EngineType::FAISS_IVFFLAT},
    {NAME_ENGINE_TYPE_IVFSQ8, engine::meta::EngineType::FAISS_IVFSQ8},
    {NAME_ENGINE_TYPE_IVFSQ8H, engine::meta::EngineType::FAISS_IVFSQ8H},
    {NAME_ENGINE_TYPE_RNSG, engine::meta::EngineType::NSG_MIX},
    {NAME_ENGINE_TYPE_IVFPQ, engine::meta::EngineType::FAISS_PQ},
    {NAME_ENGINE_TYPE_HNSW, engine::meta::EngineType::HNSW},
    {NAME_ENGINE_TYPE_ANNOY, engine::meta::EngineType::ANNOY},
    {NAME_ENGINE_TYPE_IVFSQ8NR, engine::meta::EngineType::FAISS_IVFSQ8NR},
    {NAME_ENGINE_TYPE_HNSWSQ8NR, engine::meta::EngineType::HNSW_SQ8NR}};

const std::unordered_map<engine::meta::MetricType, std::string> MetricMap = {
    {engine::meta::MetricType::L2, NAME_METRIC_TYPE_L2},
    {engine::meta::MetricType::IP, NAME_METRIC_TYPE_IP},
    {engine::meta::MetricType::HAMMING, NAME_METRIC_TYPE_HAMMING},
    {engine::meta::MetricType::JACCARD, NAME_METRIC_TYPE_JACCARD},
    {engine::meta::MetricType::TANIMOTO, NAME_METRIC_TYPE_TANIMOTO},
    {engine::meta::MetricType::SUBSTRUCTURE, NAME_METRIC_TYPE_SUBSTRUCTURE},
    {engine::meta::MetricType::SUPERSTRUCTURE, NAME_METRIC_TYPE_SUPERSTRUCTURE},
};

const std::unordered_map<std::string, engine::meta::MetricType> MetricNameMap = {
    {NAME_METRIC_TYPE_L2, engine::meta::MetricType::L2},
    {NAME_METRIC_TYPE_IP, engine::meta::MetricType::IP},
    {NAME_METRIC_TYPE_HAMMING, engine::meta::MetricType::HAMMING},
    {NAME_METRIC_TYPE_JACCARD, engine::meta::MetricType::JACCARD},
    {NAME_METRIC_TYPE_TANIMOTO, engine::meta::MetricType::TANIMOTO},
    {NAME_METRIC_TYPE_SUBSTRUCTURE, engine::meta::MetricType::SUBSTRUCTURE},
    {NAME_METRIC_TYPE_SUPERSTRUCTURE, engine::meta::MetricType::SUPERSTRUCTURE},
};
}  // namespace web
}  // namespace server
}  // namespace milvus
