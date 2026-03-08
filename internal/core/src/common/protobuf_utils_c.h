// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#ifdef __cplusplus
extern "C" {
#endif

// ProtoLayout is a common ffi type for cgo call with serialized protobuf message.
// It's always keep same memory layout with milvus::ProtoLayout at C++ side.
typedef struct ProtoLayout {
    void* blob;
    size_t size;
} ProtoLayout;

// ProtoLayoutInterface is the pointer alias for ProtoLayout.
// It should always created by CreateProtoLayout and released by ReleaseProtoLayout.
typedef struct ProtoLayout* ProtoLayoutInterface;

// CreateProtoLayout is used to create an empty ProtoLayout.
// When you want to create a ProtoLayout at go-side, and return some data from C-side.
// You should use this API.
ProtoLayoutInterface
CreateProtoLayout();

void
ReleaseProtoLayout(ProtoLayoutInterface proto);

#ifdef __cplusplus
}

#endif
