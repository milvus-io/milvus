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
const char* NAME_ENGINE_TYPE_IVFFLAT = "IVFFLAT";
const char* NAME_ENGINE_TYPE_IVFSQ8 = "IVFSQ8";
const char* NAME_ENGINE_TYPE_IVFSQ8H = "IVFSQ8H";
const char* NAME_ENGINE_TYPE_RNSG = "RNSG";
const char* NAME_ENGINE_TYPE_IVFPQ = "IVFPQ";
const char* NAME_ENGINE_TYPE_HNSW = "HNSW";
const char* NAME_ENGINE_TYPE_ANNOY = "ANNOY";

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
const std::unordered_map<engine::EngineType, std::string> IndexMap = {
    {engine::EngineType::FAISS_IDMAP, NAME_ENGINE_TYPE_FLAT},
    {engine::EngineType::FAISS_IVFFLAT, NAME_ENGINE_TYPE_IVFFLAT},
    {engine::EngineType::FAISS_IVFSQ8, NAME_ENGINE_TYPE_IVFSQ8},
    {engine::EngineType::FAISS_IVFSQ8H, NAME_ENGINE_TYPE_IVFSQ8H},
    {engine::EngineType::NSG_MIX, NAME_ENGINE_TYPE_RNSG},
    {engine::EngineType::FAISS_PQ, NAME_ENGINE_TYPE_IVFPQ},
    {engine::EngineType::HNSW, NAME_ENGINE_TYPE_HNSW},
    {engine::EngineType::ANNOY, NAME_ENGINE_TYPE_ANNOY},
};

const std::unordered_map<std::string, engine::EngineType> IndexNameMap = {
    {NAME_ENGINE_TYPE_FLAT, engine::EngineType::FAISS_IDMAP},
    {NAME_ENGINE_TYPE_IVFFLAT, engine::EngineType::FAISS_IVFFLAT},
    {NAME_ENGINE_TYPE_IVFSQ8, engine::EngineType::FAISS_IVFSQ8},
    {NAME_ENGINE_TYPE_IVFSQ8H, engine::EngineType::FAISS_IVFSQ8H},
    {NAME_ENGINE_TYPE_RNSG, engine::EngineType::NSG_MIX},
    {NAME_ENGINE_TYPE_IVFPQ, engine::EngineType::FAISS_PQ},
    {NAME_ENGINE_TYPE_HNSW, engine::EngineType::HNSW},
    {NAME_ENGINE_TYPE_ANNOY, engine::EngineType::ANNOY},
};

const std::unordered_map<engine::MetricType, std::string> MetricMap = {
    {engine::MetricType::L2, NAME_METRIC_TYPE_L2},
    {engine::MetricType::IP, NAME_METRIC_TYPE_IP},
    {engine::MetricType::HAMMING, NAME_METRIC_TYPE_HAMMING},
    {engine::MetricType::JACCARD, NAME_METRIC_TYPE_JACCARD},
    {engine::MetricType::TANIMOTO, NAME_METRIC_TYPE_TANIMOTO},
    {engine::MetricType::SUBSTRUCTURE, NAME_METRIC_TYPE_SUBSTRUCTURE},
    {engine::MetricType::SUPERSTRUCTURE, NAME_METRIC_TYPE_SUPERSTRUCTURE},
};

const std::unordered_map<std::string, engine::MetricType> MetricNameMap = {
    {NAME_METRIC_TYPE_L2, engine::MetricType::L2},
    {NAME_METRIC_TYPE_IP, engine::MetricType::IP},
    {NAME_METRIC_TYPE_HAMMING, engine::MetricType::HAMMING},
    {NAME_METRIC_TYPE_JACCARD, engine::MetricType::JACCARD},
    {NAME_METRIC_TYPE_TANIMOTO, engine::MetricType::TANIMOTO},
    {NAME_METRIC_TYPE_SUBSTRUCTURE, engine::MetricType::SUBSTRUCTURE},
    {NAME_METRIC_TYPE_SUPERSTRUCTURE, engine::MetricType::SUPERSTRUCTURE},
};
}  // namespace web
}  // namespace server
}  // namespace milvus
