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

#include <cstdint>
#include <exception>
#include <string>

namespace milvus {

using ErrorCode = int32_t;

constexpr ErrorCode SERVER_SUCCESS = 0;
constexpr ErrorCode SERVER_ERROR_CODE_BASE = 30000;

constexpr ErrorCode
ToServerErrorCode(const ErrorCode error_code) {
    return SERVER_ERROR_CODE_BASE + error_code;
}

constexpr ErrorCode DB_SUCCESS = 0;
constexpr ErrorCode DB_ERROR_CODE_BASE = 40000;

constexpr ErrorCode
ToDbErrorCode(const ErrorCode error_code) {
    return DB_ERROR_CODE_BASE + error_code;
}

constexpr ErrorCode KNOWHERE_SUCCESS = 0;
constexpr ErrorCode KNOWHERE_ERROR_CODE_BASE = 50000;

constexpr ErrorCode
ToKnowhereErrorCode(const ErrorCode error_code) {
    return KNOWHERE_ERROR_CODE_BASE + error_code;
}

constexpr ErrorCode WAL_SUCCESS = 0;
constexpr ErrorCode WAL_ERROR_CODE_BASE = 60000;

constexpr ErrorCode
ToWalErrorCode(const ErrorCode error_code) {
    return WAL_ERROR_CODE_BASE + error_code;
}

constexpr ErrorCode SS_SUCCESS = 0;
constexpr ErrorCode SS_ERROR_CODE_BASE = 70000;

constexpr ErrorCode
ToSSErrorCode(const ErrorCode error_code) {
    return SS_ERROR_CODE_BASE + error_code;
}

// server error code
constexpr ErrorCode SERVER_UNEXPECTED_ERROR = ToServerErrorCode(1);
constexpr ErrorCode SERVER_UNSUPPORTED_ERROR = ToServerErrorCode(2);
constexpr ErrorCode SERVER_NULL_POINTER = ToServerErrorCode(3);
constexpr ErrorCode SERVER_INVALID_ARGUMENT = ToServerErrorCode(4);
constexpr ErrorCode SERVER_FILE_NOT_FOUND = ToServerErrorCode(5);
constexpr ErrorCode SERVER_NOT_IMPLEMENT = ToServerErrorCode(6);
constexpr ErrorCode SERVER_CANNOT_CREATE_FOLDER = ToServerErrorCode(8);
constexpr ErrorCode SERVER_CANNOT_CREATE_FILE = ToServerErrorCode(9);
constexpr ErrorCode SERVER_CANNOT_DELETE_FOLDER = ToServerErrorCode(10);
constexpr ErrorCode SERVER_CANNOT_DELETE_FILE = ToServerErrorCode(11);
constexpr ErrorCode SERVER_BUILD_INDEX_ERROR = ToServerErrorCode(12);
constexpr ErrorCode SERVER_CANNOT_OPEN_FILE = ToServerErrorCode(13);
constexpr ErrorCode SERVER_FILE_MAGIC_BYTES_ERROR = ToServerErrorCode(14);
constexpr ErrorCode SERVER_FILE_SUM_BYTES_ERROR = ToServerErrorCode(15);
constexpr ErrorCode SERVER_CANNOT_READ_FILE = ToServerErrorCode(16);

constexpr ErrorCode SERVER_COLLECTION_NOT_EXIST = ToServerErrorCode(100);
constexpr ErrorCode SERVER_INVALID_COLLECTION_NAME = ToServerErrorCode(101);
constexpr ErrorCode SERVER_INVALID_COLLECTION_DIMENSION = ToServerErrorCode(102);
constexpr ErrorCode SERVER_INVALID_VECTOR_DIMENSION = ToServerErrorCode(104);
constexpr ErrorCode SERVER_INVALID_INDEX_TYPE = ToServerErrorCode(105);
constexpr ErrorCode SERVER_INVALID_ROWRECORD = ToServerErrorCode(106);
constexpr ErrorCode SERVER_INVALID_ROWRECORD_ARRAY = ToServerErrorCode(107);
constexpr ErrorCode SERVER_INVALID_TOPK = ToServerErrorCode(108);
constexpr ErrorCode SERVER_ILLEGAL_VECTOR_ID = ToServerErrorCode(109);
constexpr ErrorCode SERVER_ILLEGAL_SEARCH_RESULT = ToServerErrorCode(110);
constexpr ErrorCode SERVER_CACHE_FULL = ToServerErrorCode(111);
constexpr ErrorCode SERVER_WRITE_ERROR = ToServerErrorCode(112);
constexpr ErrorCode SERVER_INVALID_NPROBE = ToServerErrorCode(113);
constexpr ErrorCode SERVER_INVALID_INDEX_NLIST = ToServerErrorCode(114);
constexpr ErrorCode SERVER_INVALID_INDEX_METRIC_TYPE = ToServerErrorCode(115);
constexpr ErrorCode SERVER_INVALID_SEGMENT_ROW_COUNT = ToServerErrorCode(116);
constexpr ErrorCode SERVER_OUT_OF_MEMORY = ToServerErrorCode(117);
constexpr ErrorCode SERVER_INVALID_PARTITION_TAG = ToServerErrorCode(118);
constexpr ErrorCode SERVER_INVALID_BINARY_QUERY = ToServerErrorCode(119);
constexpr ErrorCode SERVER_INVALID_DSL_PARAMETER = ToServerErrorCode(120);
constexpr ErrorCode SERVER_INVALID_FIELD_NAME = ToServerErrorCode(121);
constexpr ErrorCode SERVER_INVALID_FIELD_NUM = ToServerErrorCode(122);

// db error code
constexpr ErrorCode DB_META_TRANSACTION_FAILED = ToDbErrorCode(1);
constexpr ErrorCode DB_ERROR = ToDbErrorCode(2);
constexpr ErrorCode DB_NOT_FOUND = ToDbErrorCode(3);
constexpr ErrorCode DB_ALREADY_EXIST = ToDbErrorCode(4);
constexpr ErrorCode DB_INVALID_PATH = ToDbErrorCode(5);
constexpr ErrorCode DB_INCOMPATIB_META = ToDbErrorCode(6);
constexpr ErrorCode DB_INVALID_META_URI = ToDbErrorCode(7);
constexpr ErrorCode DB_EMPTY_COLLECTION = ToDbErrorCode(8);
constexpr ErrorCode DB_BLOOM_FILTER_ERROR = ToDbErrorCode(9);
constexpr ErrorCode DB_PARTITION_NOT_FOUND = ToDbErrorCode(10);
constexpr ErrorCode DB_OUT_OF_STORAGE = ToDbErrorCode(11);
constexpr ErrorCode DB_META_QUERY_FAILED = ToDbErrorCode(12);
constexpr ErrorCode DB_FILE_NOT_FOUND = ToDbErrorCode(13);
constexpr ErrorCode DB_PERMISSION_ERROR = ToDbErrorCode(14);

// knowhere error code
constexpr ErrorCode KNOWHERE_ERROR = ToKnowhereErrorCode(1);
constexpr ErrorCode KNOWHERE_INVALID_ARGUMENT = ToKnowhereErrorCode(2);
constexpr ErrorCode KNOWHERE_UNEXPECTED_ERROR = ToKnowhereErrorCode(3);
constexpr ErrorCode KNOWHERE_NO_SPACE = ToKnowhereErrorCode(4);

// knowhere error code
constexpr ErrorCode WAL_ERROR = ToWalErrorCode(1);
constexpr ErrorCode WAL_META_ERROR = ToWalErrorCode(2);
constexpr ErrorCode WAL_FILE_ERROR = ToWalErrorCode(3);
constexpr ErrorCode WAL_PATH_ERROR = ToWalErrorCode(4);

// Snapshot error code
constexpr ErrorCode SS_ERROR = ToSSErrorCode(1);
constexpr ErrorCode SS_STALE_ERROR = ToSSErrorCode(2);
constexpr ErrorCode SS_NOT_FOUND_ERROR = ToSSErrorCode(3);
constexpr ErrorCode SS_INVALID_CONTEX_ERROR = ToSSErrorCode(4);
constexpr ErrorCode SS_DUPLICATED_ERROR = ToSSErrorCode(5);
constexpr ErrorCode SS_NOT_ACTIVE_ERROR = ToSSErrorCode(6);
constexpr ErrorCode SS_CONSTRAINT_CHECK_ERROR = ToSSErrorCode(7);
constexpr ErrorCode SS_INVALID_ARGUMENT_ERROR = ToSSErrorCode(8);
constexpr ErrorCode SS_OPERATION_PENDING = ToSSErrorCode(9);
constexpr ErrorCode SS_TIMEOUT = ToSSErrorCode(10);
constexpr ErrorCode SS_NOT_COMMITED = ToSSErrorCode(11);
constexpr ErrorCode SS_COLLECTION_DROPPED = ToSSErrorCode(12);
constexpr ErrorCode SS_EMPTY_HOLDER = ToSSErrorCode(13);

}  // namespace milvus
