//
// Created by ezeharia on 5/2/21.
//
#ifdef MILVUS_APU_VERSION
#pragma once

#include "GsiBaseIndex.h"
#include "Apu.h"



namespace milvus {
namespace knowhere {

class GsiHammingIndex : public GsiBaseIndex {

public :

    GsiHammingIndex(uint32_t dim) : GsiBaseIndex(dim) {
    index_type_ = IndexEnum::INDEX_FAISS_BIN_IDMAP;
    }

	void CopyIndexToFpga( uint32_t row_count , const std::string& location ) override ;

    DatasetPtr Query(const DatasetPtr &dataset, const Config &config , faiss::ConcurrentBitsetPtr blacklist) override ;


};
}//knowhere
}//milvus
#endif

