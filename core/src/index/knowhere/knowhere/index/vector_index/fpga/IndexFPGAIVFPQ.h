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

#ifndef INDEX_FPGA_IVFPQ_H
#define INDEX_FPGA_IVFPQ_H

#include <memory>
#include <utility>

#include "knowhere/index/vector_index/IndexIVFPQ.h"
#ifdef MILVUS_FPGA_VERSION
#include "knowhere/index/vector_index/fpga/FpgaInst.h"
#endif
namespace milvus {
namespace knowhere {

class FPGAIVFPQ : public IVFPQ {
 public:
    FPGAIVFPQ() : IVFPQ() {
        index_type_ = IndexEnum::INDEX_FAISS_IVFPQ;
    }
    explicit FPGAIVFPQ(std::shared_ptr<faiss::Index> index) : IVFPQ(std::move(index)) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFPQ;
    }

    void
    Train(const DatasetPtr&, const Config&) override;

    void
    Add(const DatasetPtr&, const Config&) override;

    void
    CopyIndexToFpga();

    void
    QueryImpl(int64_t, const float*, int64_t, float*, int64_t*, const Config&,
              const faiss::ConcurrentBitsetPtr&) override;
};

using FPGAIVFPQPtr = std::shared_ptr<FPGAIVFPQ>;
}  // namespace knowhere
}  // namespace milvus
#endif
