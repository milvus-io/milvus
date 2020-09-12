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

#include <string>
#include <unordered_map>

#include <oatpp/core/data/mapping/type/Object.hpp>
#include <oatpp/web/protocol/http/Http.hpp>

#include "server/web_impl/Constants.h"

namespace milvus {
namespace server {
namespace web {

using OString = oatpp::data::mapping::type::String;
using OInt8 = oatpp::data::mapping::type::Int8;
using OInt16 = oatpp::data::mapping::type::Int16;
using OInt64 = oatpp::data::mapping::type::Int64;
using OFloat32 = oatpp::data::mapping::type::Float32;
template <class T>
using OList = oatpp::data::mapping::type::List<T>;

using OQueryParams = oatpp::web::protocol::http::QueryParams;

enum StatusCode : int {
    SUCCESS = 0,
    UNEXPECTED_ERROR = 1,
    CONNECT_FAILED = 2,  // reserved.
    PERMISSION_DENIED = 3,
    COLLECTION_NOT_EXISTS = 4,  // DB_NOT_FOUND || COLLECTION_NOT_EXISTS
    ILLEGAL_ARGUMENT = 5,
    ILLEGAL_RANGE = 6,
    ILLEGAL_DIMENSION = 7,
    ILLEGAL_INDEX_TYPE = 8,
    ILLEGAL_COLLECTION_NAME = 9,
    ILLEGAL_TOPK = 10,
    ILLEGAL_ROWRECORD = 11,
    ILLEGAL_VECTOR_ID = 12,
    ILLEGAL_SEARCH_RESULT = 13,
    FILE_NOT_FOUND = 14,
    META_FAILED = 15,
    CACHE_FAILED = 16,
    CANNOT_CREATE_FOLDER = 17,
    CANNOT_CREATE_FILE = 18,
    CANNOT_DELETE_FOLDER = 19,
    CANNOT_DELETE_FILE = 20,
    BUILD_INDEX_ERROR = 21,
    ILLEGAL_NLIST = 22,
    ILLEGAL_METRIC_TYPE = 23,
    OUT_OF_MEMORY = 24,

    // HTTP error code
    PATH_PARAM_LOSS = 31,
    UNKNOWN_PATH = 32,
    QUERY_PARAM_LOSS = 33,
    BODY_FIELD_LOSS = 34,
    ILLEGAL_BODY = 35,
    BODY_PARSE_FAIL = 36,
    ILLEGAL_QUERY_PARAM = 37,

    MAX = ILLEGAL_QUERY_PARAM
};

}  // namespace web
}  // namespace server
}  // namespace milvus
