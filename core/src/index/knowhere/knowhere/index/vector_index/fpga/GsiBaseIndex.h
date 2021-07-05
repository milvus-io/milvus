//
// Created by ezeharia on 5/2/21.
//
#ifdef MILVUS_APU_VERSION
#pragma once

#include "index/knowhere/knowhere/index/vector_index/VecIndex.h"
#include "scheduler/task/Task.h"
#include <knowhere/index/vector_index/helpers/IndexParameter.h>


extern "C"
{
#include <gsi/libgsl.h> // added by eitan
#include <gsi/libgsl_matrix.h>
#include <gsi/libgsl_flat_tanimoto.h>
#include <gsi/libgdl.h>
}

#define NUM_QUERIES (100)
#define INIT_K (50)
#define MAX_HEAP_ALLOCATIONS (4)
#define O_RDONLY	     00

using namespace milvus::engine::meta;
using namespace milvus::scheduler;


namespace milvus {
    namespace knowhere {

class GsiBaseIndex : public VecIndex {

public :


    uint32_t topK_ =INIT_K;

    uint32_t num_queries_ =0;

    uint32_t num_bfeatures_ = 0;

    uint32_t num_bytes_in_rec_=0;

    int64_t index_size_ = 0;

    void *heap_allocations[MAX_HEAP_ALLOCATIONS] = { 0 };

    unsigned int counter_heap_allocations = 0;



    virtual int64_t Size();


    BinarySet Serialize(const Config &config) override;

    void Load(const BinarySet &set) override;

    void Train(const DatasetPtr &dataset, const Config &config) override;

    void AddWithoutIds(const DatasetPtr &dataset, const Config &config) override;

    DatasetPtr Query(const DatasetPtr &dataset, const Config &config) override;

    int64_t Dim() override;

    int64_t Count() override;

   GsiBaseIndex(uint32_t dim );

   ~GsiBaseIndex();

   virtual void CopyIndexToFpga( uint32_t row_count  , const std::string& location) =0;



protected :

    void setResultDistancesStruct();

    struct gsl_matrix_u32 indices_;

    struct gsl_matrix_f32 distances_;

    struct gsl_matrix_u1 queries_;

    void AllocateMemory(const DatasetPtr &dataset, const Config &config);

    void setQueriesInfo(const DatasetPtr &dataset, const Config &config);

    void setResultIndicesStruct();

    int64_t *convertToInt64_t(gsl_matrix_u32 *indices ,  int64_t* ids_int64);

    void freeAllocatedMem();

};
}//knowhere
}//milvus
#endif