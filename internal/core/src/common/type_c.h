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
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum SegmentType {
    Invalid = 0,
    Growing = 1,
    Sealed = 2,
    Indexing = 3,
};

typedef enum SegmentType SegmentType;

enum ErrorCode {
    Success = 0,
    UnexpectedError = 1,
    IllegalArgument = 5,
};

// pure C don't support that we use schemapb.DataType directly.
// Note: the value of all enumerations must match the corresponding schemapb.DataType.
// TODO: what if there are increments in schemapb.DataType.
enum CDataType {
    None = 0,
    Bool = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,

    Float = 10,
    Double = 11,

    String = 20,
    VarChar = 21,

    BinaryVector = 100,
    FloatVector = 101,
};
typedef enum CDataType CDataType;

typedef struct CStatus {
    int error_code;
    const char* error_msg;
} CStatus;

typedef struct CProto {
    const void* proto_blob;
    int64_t proto_size;
} CProto;

typedef struct CLoadDeletedRecordInfo {
    void* timestamps;
    const uint8_t* primary_keys;
    const uint64_t primary_keys_size;
    int64_t row_count;
} CLoadDeletedRecordInfo;

typedef struct CStorageConfig {
    const char* address;
    const char* bucket_name;
    const char* access_key_id;
    const char* access_key_value;
    const char* root_path;
    const char* storage_type;
    const char* iam_endpoint;
    bool useSSL;
    bool useIAM;
} CStorageConfig;

typedef struct CTraceConfig {
    const char* exporter;
    int sampleFraction;
    const char* jaegerURL;
    const char* otlpEndpoint;

    int nodeID;
} CTraceConfig;

typedef struct CTraceContext {
    const uint8_t* traceID;
    const uint8_t* spanID;
    uint8_t flag;
} CTraceContext;
#ifdef __cplusplus
}
#endif
