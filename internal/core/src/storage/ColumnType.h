// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
enum ColumnType : int {
    NONE = 0,
    BOOL = 1,
    INT8 = 2,
    INT16 = 3,
    INT32 = 4,
    INT64 = 5,
    UINT8 = 6,
    UINT16 = 7,
    UINT32 = 8,
    UINT64 = 9,
    FLOAT = 10,
    DOUBLE = 11,
    STRING = 20,
    VARCHAR = 21,
    VECTOR_BINARY = 100,
    VECTOR_FLOAT = 101
};

enum ErrorCode : int {
    SUCCESS = 0,
    UNEXPECTED_ERROR = 1,
    CONNECT_FAILED = 2,
    PERMISSION_DENIED = 3,
    COLLECTION_NOT_EXISTS = 4,
    ILLEGAL_ARGUMENT = 5,
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
    DD_REQUEST_RACE = 1000
};
