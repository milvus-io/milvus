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

#include "codecs/snapshot/SSCodec.h"

#include <memory>

#include "SSDeletedDocsFormat.h"
#include "SSIdBloomFilterFormat.h"
#include "SSStructuredIndexFormat.h"
#include "SSVectorIndexFormat.h"

namespace milvus {
namespace codec {

SSCodec&
SSCodec::instance() {
    static SSCodec s_instance;
    return s_instance;
}

SSCodec::SSCodec() {
    block_format_ptr_ = std::make_shared<SSBlockFormat>();
    structured_index_format_ptr_ = std::make_shared<SSStructuredIndexFormat>();
    vector_index_format_ptr_ = std::make_shared<SSVectorIndexFormat>();
    deleted_docs_format_ptr_ = std::make_shared<SSDeletedDocsFormat>();
    id_bloom_filter_format_ptr_ = std::make_shared<SSIdBloomFilterFormat>();
    vector_compress_format_ptr_ = std::make_shared<SSVectorCompressFormat>();
}

SSBlockFormatPtr
SSCodec::GetBlockFormat() {
    return block_format_ptr_;
}

SSVectorIndexFormatPtr
SSCodec::GetVectorIndexFormat() {
    return vector_index_format_ptr_;
}

SSStructuredIndexFormatPtr
SSCodec::GetStructuredIndexFormat() {
    return structured_index_format_ptr_;
}

SSDeletedDocsFormatPtr
SSCodec::GetDeletedDocsFormat() {
    return deleted_docs_format_ptr_;
}

SSIdBloomFilterFormatPtr
SSCodec::GetIdBloomFilterFormat() {
    return id_bloom_filter_format_ptr_;
}

SSVectorCompressFormatPtr
SSCodec::GetVectorCompressFormat() {
    return vector_compress_format_ptr_;
}
}  // namespace codec
}  // namespace milvus
