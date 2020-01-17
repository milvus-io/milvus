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

#include "codecs/default/DefaultCodec.h"

#include <memory>

#include "DefaultDeletedDocsFormat.h"
#include "DefaultIdBloomFilterFormat.h"
#include "DefaultVectorsFormat.h"

namespace milvus {
namespace codec {

DefaultCodec::DefaultCodec() {
    vectors_format_ptr_ = std::make_shared<DefaultVectorsFormat>();
    deleted_docs_format_ptr_ = std::make_shared<DefaultDeletedDocsFormat>();
    id_bloom_filter_format_ptr_ = std::make_shared<DefaultIdBloomFilterFormat>();
}

VectorsFormatPtr
DefaultCodec::GetVectorsFormat() {
    return vectors_format_ptr_;
}

DeletedDocsFormatPtr
DefaultCodec::GetDeletedDocsFormat() {
    return deleted_docs_format_ptr_;
}

IdBloomFilterFormatPtr
DefaultCodec::GetIdBloomFilterFormat() {
    return id_bloom_filter_format_ptr_;
}

}  // namespace codec
}  // namespace milvus
