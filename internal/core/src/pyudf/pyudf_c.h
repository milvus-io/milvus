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

#include <stdint.h>

#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CPyUDFInvocation;
typedef void* CPyUDFResult;
typedef void* CPyUDFResource;

// Function-pipeline code shared with merr.ErrFunctionFailed. This is not a
// segcore-family code; all other failures preserve their native ErrorCode.
typedef enum CPyUDFErrorCode {
    PyUDFErrorCodeFunctionFailed = 2400,
} CPyUDFErrorCode;

// Returns whether this core library was built with the embedded CPython runtime.
bool
PyUDFRuntimeBuildEnabled(void);

// Initializes the process-lifetime, isolated PyUDF interpreter and imports the
// trusted runtime package installed with Milvus. Repeated calls are idempotent.
CStatus
InitializePyUDFRuntime(void);

// Loads the serialized milvus.proto.cgo.PyUDFLoadRequest into an owned native
// resource. On failure, *resource is always set to nullptr.
CStatus
LoadPyUDFResource(const uint8_t* serialized_request,
                  uint64_t serialized_request_len,
                  CPyUDFResource* resource);

// Releases a resource. A null resource is a successful no-op.
CStatus
DeletePyUDFResource(CPyUDFResource resource);

struct ArrowArray;
struct ArrowSchema;

CStatus
NewPyUDFInvocation(int32_t num_inputs,
                   int32_t num_chunks,
                   const int64_t* chunk_sizes,
                   CPyUDFInvocation* invocation);

int32_t
PyUDFInvocationNumInputs(CPyUDFInvocation invocation);

int32_t
PyUDFInvocationNumChunks(CPyUDFInvocation invocation);

int64_t
PyUDFInvocationChunkSize(CPyUDFInvocation invocation, int32_t chunk_index);

struct ArrowArray*
PyUDFInvocationInputArray(CPyUDFInvocation invocation,
                          int32_t input_index,
                          int32_t chunk_index);

struct ArrowSchema*
PyUDFInvocationInputSchema(CPyUDFInvocation invocation,
                           int32_t input_index,
                           int32_t chunk_index);

void
DeletePyUDFInvocation(CPyUDFInvocation invocation);

// Runs one invocation through an already loaded resource. Resource and
// invocation handles remain caller-owned. On failure, *result is always null.
CStatus
RunPyUDFResource(CPyUDFResource resource,
                 CPyUDFInvocation invocation,
                 const uint8_t* serialized_params,
                 uint64_t serialized_params_len,
                 CPyUDFResult* result);

// Synchronous identity helper used only to verify Arrow C Data handles.
CStatus
RunPyUDFIdentity(CPyUDFInvocation invocation, CPyUDFResult* result);

int32_t
PyUDFResultNumOutputs(CPyUDFResult result);

int32_t
PyUDFResultNumChunks(CPyUDFResult result, int32_t output_index);

struct ArrowArray*
PyUDFResultArray(CPyUDFResult result,
                 int32_t output_index,
                 int32_t chunk_index);

struct ArrowSchema*
PyUDFResultSchema(CPyUDFResult result,
                  int32_t output_index,
                  int32_t chunk_index);

void
DeletePyUDFResult(CPyUDFResult result);

#ifdef __cplusplus
}
#endif
