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

const char* FIELD_UID = "_id";

const char* ELEMENT_RAW_DATA = "_raw";
const char* ELEMENT_BLOOM_FILTER = "_blf";
const char* ELEMENT_DELETED_DOCS = "_del";
const char* ELEMENT_INDEX_COMPRESS = "_compress";

const char* PARAM_UID_AUTOGEN = "auto_id";
const char* PARAM_DIMENSION = knowhere::meta::DIM;
const char* PARAM_INDEX_TYPE = "index_type";
const char* PARAM_INDEX_METRIC_TYPE = knowhere::Metric::TYPE;
const char* PARAM_INDEX_EXTRA_PARAMS = "params";
const char* PARAM_SEGMENT_ROW_COUNT = "segment_row_limit";

const char* DEFAULT_STRUCTURED_INDEX = "SORTED";  // this string should be defined in knowhere::IndexEnum
const char* DEFAULT_PARTITON_TAG = "_default";

}  // namespace engine
}  // namespace milvus
