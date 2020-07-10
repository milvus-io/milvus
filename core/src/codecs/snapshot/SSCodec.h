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

#include "codecs/snapshot/SSAttrsFormat.h"
#include "codecs/snapshot/SSAttrsIndexFormat.h"
#include "codecs/snapshot/SSDeletedDocsFormat.h"
#include "codecs/snapshot/SSIdBloomFilterFormat.h"
#include "codecs/snapshot/SSVectorsFormat.h"
#include "codecs/snapshot/SSVectorCompressFormat.h"
#include "codecs/snapshot/SSVectorIndexFormat.h"

namespace milvus {
namespace codec {

class SSCodec {
 public:
    static SSCodec&
    instance();

    SSVectorsFormatPtr
    GetVectorsFormat();

    SSAttrsFormatPtr
    GetAttrsFormat();

    SSVectorIndexFormatPtr
    GetVectorIndexFormat();

    SSAttrsIndexFormatPtr
    GetAttrsIndexFormat();

    SSDeletedDocsFormatPtr
    GetDeletedDocsFormat();

    SSIdBloomFilterFormatPtr
    GetIdBloomFilterFormat();

    SSVectorCompressFormatPtr
    GetVectorCompressFormat();

 private:
    SSCodec();

 private:
    SSVectorsFormatPtr vectors_format_ptr_;
    SSAttrsFormatPtr attrs_format_ptr_;
    SSVectorIndexFormatPtr vector_index_format_ptr_;
    SSAttrsIndexFormatPtr attrs_index_format_ptr_;
    SSDeletedDocsFormatPtr deleted_docs_format_ptr_;
    SSIdBloomFilterFormatPtr id_bloom_filter_format_ptr_;
    SSVectorCompressFormatPtr vector_compress_format_ptr_;
};

}  // namespace codec
}  // namespace milvus
