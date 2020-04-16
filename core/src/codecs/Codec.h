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
#include "VectorIndexFormat.h"
#include "VectorsFormat.h"

namespace milvus {
namespace codec {

class Codec {
 public:
    virtual VectorsFormatPtr
    GetVectorsFormat() = 0;

    virtual AttrsFormatPtr
    GetAttrsFormat() = 0;

    virtual VectorIndexFormatPtr
    GetVectorIndexFormat() = 0;

    virtual DeletedDocsFormatPtr
    GetDeletedDocsFormat() = 0;

    virtual IdBloomFilterFormatPtr
    GetIdBloomFilterFormat() = 0;

    // TODO(zhiru)
    /*
    virtual AttrsFormat
    GetAttrsFormat() = 0;

    virtual AttrsIndexFormat
    GetAttrsIndexFormat() = 0;

    virtual IdIndexFormat
    GetIdIndexFormat() = 0;

    */
};

}  // namespace codec
}  // namespace milvus
