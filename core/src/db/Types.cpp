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

#include "db/Types.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace engine {

const char* DEFAULT_UID_NAME = "_id";
const char* PARAM_UID_AUTOGEN = "auto_id";

const char* DEFAULT_RAW_DATA_NAME = "_raw";
const char* DEFAULT_BLOOM_FILTER_NAME = "_blf";
const char* DEFAULT_DELETED_DOCS_NAME = "_del";
const char* DEFAULT_INDEX_COMPRESS_NAME = "_compress";

const char* DEFAULT_STRUCTURED_INDEX_NAME = "SORTED";  // this string should be defined in knowhere::IndexEnum

const char* PARAM_DIMENSION = knowhere::meta::DIM;
const char* PARAM_INDEX_TYPE = "index_type";
const char* PARAM_INDEX_METRIC_TYPE = knowhere::Metric::TYPE;
const char* PARAM_INDEX_EXTRA_PARAMS = "params";
const char* PARAM_SEGMENT_ROW_COUNT = "segment_row_count";

}  // namespace engine
}  // namespace milvus
