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

#pragma once

namespace milvus {
namespace server {
namespace web {

////////////////////////////////////////////////////
static const char* WEB_LOG_PREFIX = "[Web] ";

////////////////////////////////////////////////////

static const char* CORS_KEY_METHODS = "Access-Control-Allow-Methods";
static const char* CORS_KEY_ORIGIN = "Access-Control-Allow-Origin";
static const char* CORS_KEY_HEADERS = "Access-Control-Allow-Headers";
static const char* CORS_KEY_AGE = "Access-Control-Max-Age";

static const char* CORS_VALUE_METHODS = "GET, POST, PUT, OPTIONS, DELETE";
static const char* CORS_VALUE_ORIGIN = "*";
static const char* CORS_VALUE_HEADERS =
    "DNT, User-Agent, X-Requested-With, If-Modified-Since, Cache-Control, Content-Type, Range, Authorization";
static const char* CORS_VALUE_AGE = "1728000";

////////////////////////////////////////////////////

static const char* NAME_ENGINE_TYPE_FLAT = "FLAT";
static const char* NAME_ENGINE_TYPE_IVFFLAT = "IVFFLAT";
static const char* NAME_ENGINE_TYPE_IVFSQ8 = "IVFSQ8";
static const char* NAME_ENGINE_TYPE_IVFSQ8H = "IVFSQ8H";
static const char* NAME_ENGINE_TYPE_RNSG = "RNSG";
static const char* NAME_ENGINE_TYPE_IVFPQ = "IVFPQ";

static const char* NAME_METRIC_TYPE_L2 = "L2";
static const char* NAME_METRIC_TYPE_IP = "IP";
static const char* NAME_METRIC_TYPE_HAMMING = "HAMMING";
static const char* NAME_METRIC_TYPE_JACCARD = "JACCARD";
static const char* NAME_METRIC_TYPE_TANIMOTO = "TANIMOTO";

////////////////////////////////////////////////////

static const char* KEY_TABLE_TABLE_NAME = "table_name";
static const char* KEY_TABLE_DIMENSION = "dimension";
static const char* KEY_TABLE_INDEX_FILE_SIZE = "index_file_size";
static const char* KEY_TABLE_INDEX_METRIC_TYPE = "metric_type";
static const char* KEY_TABLE_COUNT = "count";

static const char* KEY_INDEX_INDEX_TYPE = "index_type";
static const char* KEY_INDEX_NLIST = "nlist";

static const char* KEY_PARTITION_NAME = "partition_name";
static const char* KEY_PARTITION_TAG = "partition_tag";

////////////////////////////////////////////////////

static const int64_t VALUE_TABLE_INDEX_FILE_SIZE_DEFAULT = 1024;
static const char* VALUE_TABLE_METRIC_TYPE_DEFAULT = "L2";

static const char* VALUE_PARTITION_TAG_DEFAULT = "";

static const char* VALUE_INDEX_INDEX_TYPE_DEFAULT = NAME_ENGINE_TYPE_FLAT;
static const int64_t VALUE_INDEX_NLIST_DEFAULT = 16384;

static const int64_t VALUE_CONFIG_CPU_CACHE_CAPACITY_DEFAULT = 4;
static const bool VALUE_CONFIG_CACHE_INSERT_DATA_DEFAULT = false;

}  // namespace web
}  // namespace server
}  // namespace milvus
