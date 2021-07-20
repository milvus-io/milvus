// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#ifdef MILVUS_APU_VERSION
#pragma once

#include "index/knowhere/knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "scheduler/task/Task.h"

#include <string>

extern "C" {
#include <gsi/libgdl.h>
#include <gsi/libgsl.h>
#include <gsi/libgsl_flat_tanimoto.h>
#include <gsi/libgsl_matrix.h>
}

#define NUM_QUERIES (100)
#define INIT_K (50)
#define MAX_HEAP_ALLOCATIONS (4)
#define O_RDONLY 00

namespace milvus {
namespace knowhere {

class GsiBaseIndex : public VecIndex {
 public:
    uint32_t topK_ = INIT_K;

    uint32_t num_queries_;

    uint32_t num_bfeatures_;

    uint32_t num_bytes_in_rec_;

    int64_t index_size_;

    void* heap_allocations[MAX_HEAP_ALLOCATIONS] = {0};

    unsigned int counter_heap_allocations;

    virtual int64_t
    Size();

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& set) override;

    void
    Train(const DatasetPtr& dataset, const Config& config) override;

    void
    AddWithoutIds(const DatasetPtr& dataset, const Config& config) override;

    int64_t
    Dim() override;

    int64_t
    Count() override;

    explicit GsiBaseIndex(uint32_t dim);

    ~GsiBaseIndex();

    virtual void
    CopyIndexToFpga(uint32_t row_count, const std::string& location) = 0;

    virtual DatasetPtr
    Query(const DatasetPtr& dataset, const Config& config, faiss::ConcurrentBitsetPtr blacklist) = 0;

 protected:
    void
    setResultDistancesStruct();

    struct gsl_matrix_u32 indices_;

    struct gsl_matrix_f32 distances_;

    struct gsl_matrix_u1 queries_;

    void
    AllocateMemory(const DatasetPtr& dataset, const Config& config);

    void
    setQueriesInfo(const DatasetPtr& dataset, const Config& config);

    void
    setResultIndicesStruct();

    int64_t*
    convertToInt64_t(gsl_matrix_u32* indices, int64_t* ids_int64);

    void
    freeAllocatedMem();
};
}  // namespace knowhere
}  // namespace milvus
#endif
