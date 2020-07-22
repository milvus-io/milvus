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

#include "AttrsFormat.h"
#include "AttrsIndexFormat.h"
#include "DeletedDocsFormat.h"
#include "IdBloomFilterFormat.h"
#include "IdIndexFormat.h"
#include "VectorCompressFormat.h"
#include "VectorIndexFormat.h"
#include "VectorsFormat.h"
#include "utils/Exception.h"

namespace milvus {
namespace codec {

class Codec {
 public:
    virtual VectorsFormatPtr
    GetVectorsFormat() {
        throw Exception(SERVER_UNSUPPORTED_ERROR, "vectors not supported");
    }

    virtual AttrsFormatPtr
    GetAttrsFormat() {
        throw Exception(SERVER_UNSUPPORTED_ERROR, "attr not supported");
    }

    virtual VectorIndexFormatPtr
    GetVectorIndexFormat() {
        throw Exception(SERVER_UNSUPPORTED_ERROR, "vectors index not supported");
    }

    virtual AttrsIndexFormatPtr
    GetAttrsIndexFormat() {
        throw Exception(SERVER_UNSUPPORTED_ERROR, "attr index not supported");
    }

    virtual DeletedDocsFormatPtr
    GetDeletedDocsFormat() {
        throw Exception(SERVER_UNSUPPORTED_ERROR, "delete doc index not supported");
    }

    virtual IdBloomFilterFormatPtr
    GetIdBloomFilterFormat() {
        throw Exception(SERVER_UNSUPPORTED_ERROR, "id bloom filter not supported");
    }

    virtual VectorCompressFormatPtr
    GetVectorCompressFormat() {
        throw Exception(SERVER_UNSUPPORTED_ERROR, "vector compress not supported");
    }
};

}  // namespace codec
}  // namespace milvus
