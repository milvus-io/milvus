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

#include "codecs/Codec.h"

namespace milvus {
namespace codec {

class DefaultCodec : public Codec {
 public:
    DefaultCodec();

    VectorsFormatPtr
    GetVectorsFormat() override;

    AttrsFormatPtr
    GetAttrsFormat() override;

    VectorIndexFormatPtr
    GetVectorIndexFormat() override;

    DeletedDocsFormatPtr
    GetDeletedDocsFormat() override;

    IdBloomFilterFormatPtr
    GetIdBloomFilterFormat() override;

 private:
    VectorsFormatPtr vectors_format_ptr_;
    AttrsFormatPtr attrs_format_ptr_;
    VectorIndexFormatPtr vector_index_format_ptr_;
    DeletedDocsFormatPtr deleted_docs_format_ptr_;
    IdBloomFilterFormatPtr id_bloom_filter_format_ptr_;
};

}  // namespace codec
}  // namespace milvus
