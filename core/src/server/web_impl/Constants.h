
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

#pragma once

namespace milvus {
namespace server {
namespace web {
////////////////////////////////////////////////////

static const char * NAME_ENGINE_TYPE_FLAT = "FLAT";
static const char * NAME_ENGINE_TYPE_IVFFLAT = "IVFFLAT";
static const char * NAME_ENGINE_TYPE_IVFSQ8 = "IVFSQ8";
static const char * NAME_ENGINE_TYPE_IVFSQ8H = "IVFSQ8H";
static const char * NAME_ENGINE_TYPE_RNSG = "RNSG";
static const char * NAME_ENGINE_TYPE_IVFPQ = "IVFPQ";

static const char * NAME_METRIC_TYPE_L2 = "L2";
static const char * NAME_METRIC_TYPE_IP = "IP";

////////////////////////////////////////////////////

static const char * KEY_TABLE_TABLE_NAME = "table_name";
static const char * KEY_TABLE_DIMENSION = "dimension";
static const char * KEY_TABLE_INDEX_FILE_SIZE = "index_file_size";
static const char * KEY_TABLE_INDEX_METRIC_TYPE = "metric_type";
static const char * KEY_TABLE_COUNT = "count";

static const char * KEY_INDEX_INDEX_TYPE = "index_type";
static const char * KEY_INDEX_NLIST = "nlist";

static const char * KEY_PARTITION_NAME = "partition_name";
static const char * KEY_PARTITION_TAG = "tag";

////////////////////////////////////////////////////

static const int64_t VALUE_TABLE_INDEX_FILE_SIZE_DEFAULT = 1024;
static const int64_t VALUE_TABLE_METRIC_TYPE_DEFAULT = 1;

static const char * VALUE_PARTITION_TAG_DEFAULT = "";

static const char * VALUE_INDEX_INDEX_TYPE_DEFAULT = NAME_ENGINE_TYPE_FLAT;
static const int64_t VALUE_INDEX_NLIST_DEFAULT = 16384;

}  // namespace web
}  // namespace server
}  // namespace milvus
