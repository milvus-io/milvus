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

#include <memory>

#include "segment/Attrs.h"
#include "segment/DeletedDocs.h"
#include "segment/IdBloomFilter.h"
#include "segment/VectorIndex.h"
#include "segment/Vectors.h"

namespace milvus {
namespace segment {

typedef int64_t doc_id_t;

struct Segment {
    VectorsPtr vectors_ptr_ = std::make_shared<Vectors>();
    AttrsPtr attrs_ptr_ = std::make_shared<Attrs>();
    VectorIndexPtr vector_index_ptr_ = std::make_shared<VectorIndex>();
    DeletedDocsPtr deleted_docs_ptr_ = nullptr;
    IdBloomFilterPtr id_bloom_filter_ptr_ = nullptr;
};

using SegmentPtr = std::shared_ptr<Segment>;

}  // namespace segment
}  // namespace milvus
