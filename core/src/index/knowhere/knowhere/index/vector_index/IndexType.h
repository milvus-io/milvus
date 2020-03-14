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

#include <string>

namespace milvus {
namespace knowhere {

// todo: enum => string
enum class IndexType {
    INVALID = 0,
    INDEX_FAISS_IDMAP = 1,
    INDEX_FAISS_IVFFLAT = 2,
    INDEX_FAISS_IVFPQ = 3,
    INDEX_FAISS_IVFSQ8 = 4,
    INDEX_FAISS_IVFSQ8H = 5,  // only support build on gpu.
    INDEX_FAISS_BIN_IDMAP = 6,
    INDEX_FAISS_BIN_IVFFLAT = 7,
    INDEX_NSG = 100,
    INDEX_SPTAG_KDT_RNT = 101,
    INDEX_SPTAG_BKT_RNT = 102,
    INDEX_HNSW = 103,
};

enum class IndexMode { MODE_CPU = 0, MODE_GPU = 1 };

extern std::string
IndexTypeToStr(const IndexType type);

}  // namespace knowhere
}  // namespace milvus
