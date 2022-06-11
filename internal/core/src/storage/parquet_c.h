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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

typedef struct CBuffer {
    char* data;
    int length;
} CBuffer;

typedef struct CStatus {
    int error_code;
    const char* error_msg;
} CStatus;

//============= payload writer ======================
typedef void* CPayloadWriter;
CPayloadWriter
NewPayloadWriter(int columnType);
CStatus
AddBooleanToPayload(CPayloadWriter payloadWriter, bool* values, int length);
CStatus
AddInt8ToPayload(CPayloadWriter payloadWriter, int8_t* values, int length);
CStatus
AddInt16ToPayload(CPayloadWriter payloadWriter, int16_t* values, int length);
CStatus
AddInt32ToPayload(CPayloadWriter payloadWriter, int32_t* values, int length);
CStatus
AddInt64ToPayload(CPayloadWriter payloadWriter, int64_t* values, int length);
CStatus
AddFloatToPayload(CPayloadWriter payloadWriter, float* values, int length);
CStatus
AddDoubleToPayload(CPayloadWriter payloadWriter, double* values, int length);
CStatus
AddOneStringToPayload(CPayloadWriter payloadWriter, char* cstr, int str_size);
CStatus
AddBinaryVectorToPayload(CPayloadWriter payloadWriter, uint8_t* values, int dimension, int length);
CStatus
AddFloatVectorToPayload(CPayloadWriter payloadWriter, float* values, int dimension, int length);

CStatus
FinishPayloadWriter(CPayloadWriter payloadWriter);
CBuffer
GetPayloadBufferFromWriter(CPayloadWriter payloadWriter);
int
GetPayloadLengthFromWriter(CPayloadWriter payloadWriter);
void
ReleasePayloadWriter(CPayloadWriter handler);

//============= payload reader ======================
typedef void* CPayloadReader;
CPayloadReader
NewPayloadReader(int columnType, uint8_t* buffer, int64_t buf_size);
CStatus
GetBoolFromPayload(CPayloadReader payloadReader, bool** values, int* length);
CStatus
GetInt8FromPayload(CPayloadReader payloadReader, int8_t** values, int* length);
CStatus
GetInt16FromPayload(CPayloadReader payloadReader, int16_t** values, int* length);
CStatus
GetInt32FromPayload(CPayloadReader payloadReader, int32_t** values, int* length);
CStatus
GetInt64FromPayload(CPayloadReader payloadReader, int64_t** values, int* length);
CStatus
GetFloatFromPayload(CPayloadReader payloadReader, float** values, int* length);
CStatus
GetDoubleFromPayload(CPayloadReader payloadReader, double** values, int* length);
CStatus
GetOneStringFromPayload(CPayloadReader payloadReader, int idx, char** cstr, int* str_size);
CStatus
GetBinaryVectorFromPayload(CPayloadReader payloadReader, uint8_t** values, int* dimension, int* length);
CStatus
GetFloatVectorFromPayload(CPayloadReader payloadReader, float** values, int* dimension, int* length);

int
GetPayloadLengthFromReader(CPayloadReader payloadReader);
void
ReleasePayloadReader(CPayloadReader payloadReader);

#ifdef __cplusplus
}
#endif
