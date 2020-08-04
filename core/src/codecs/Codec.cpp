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

#include "codecs/Codec.h"

#include <memory>

#include "DeletedDocsFormat.h"
#include "IdBloomFilterFormat.h"
#include "StructuredIndexFormat.h"
#include "VectorIndexFormat.h"

namespace milvus {
namespace codec {

Codec&
Codec::instance() {
    static Codec s_instance;
    return s_instance;
}

Codec::Codec() {
    block_format_ptr_ = std::make_shared<BlockFormat>();
    structured_index_format_ptr_ = std::make_shared<StructuredIndexFormat>();
    suffix_set_.insert(structured_index_format_ptr_->FilePostfix());
    vector_index_format_ptr_ = std::make_shared<VectorIndexFormat>();
    suffix_set_.insert(vector_index_format_ptr_->FilePostfix());
    deleted_docs_format_ptr_ = std::make_shared<DeletedDocsFormat>();
    suffix_set_.insert(deleted_docs_format_ptr_->FilePostfix());
    id_bloom_filter_format_ptr_ = std::make_shared<IdBloomFilterFormat>();
    suffix_set_.insert(id_bloom_filter_format_ptr_->FilePostfix());
    vector_compress_format_ptr_ = std::make_shared<VectorCompressFormat>();
    suffix_set_.insert(vector_compress_format_ptr_->FilePostfix());
}

const std::set<std::string>&
Codec::GetSuffixSet() const {
    return suffix_set_;
}

BlockFormatPtr
Codec::GetBlockFormat() {
    return block_format_ptr_;
}

VectorIndexFormatPtr
Codec::GetVectorIndexFormat() {
    return vector_index_format_ptr_;
}

StructuredIndexFormatPtr
Codec::GetStructuredIndexFormat() {
    return structured_index_format_ptr_;
}

DeletedDocsFormatPtr
Codec::GetDeletedDocsFormat() {
    return deleted_docs_format_ptr_;
}

IdBloomFilterFormatPtr
Codec::GetIdBloomFilterFormat() {
    return id_bloom_filter_format_ptr_;
}

VectorCompressFormatPtr
Codec::GetVectorCompressFormat() {
    return vector_compress_format_ptr_;
}
}  // namespace codec
}  // namespace milvus
