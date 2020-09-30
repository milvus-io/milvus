#ifndef INDEX_FPGA_IVFPQ_H
#define INDEX_FPGA_IVFPQ_H

#include <memory>
#include <utility>

#include "knowhere/index/vector_index/IndexIVFPQ.h"
#ifdef MILVUS_FPGA_VERSION
#include"knowhere/index/vector_index/fpga/FpgaInst.h"
#endif
namespace milvus {
namespace knowhere {

class FPGAIVFPQ : public IVFPQ {
 public:
    FPGAIVFPQ() :IVFPQ() {
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
    QueryImpl(int64_t, const float*, int64_t, float*, int64_t*, const Config&) override;

};

using FPGAIVFPQPtr = std::shared_ptr<FPGAIVFPQ>;

}  // namespace knowhere
}  // namespace milvus
#endif