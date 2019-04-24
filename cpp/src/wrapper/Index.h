////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <fstream>

#include "faiss/AutoTune.h"
#include "faiss/AuxIndexStructures.h"
#include "faiss/gpu/GpuAutoTune.h"
#include "faiss/index_io.h"

#include "Operand.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class Index;
using Index_ptr = std::shared_ptr<Index>;

class Index {
    typedef long idx_t;

public:
    int dim;         ///< std::vector dimension
    idx_t ntotal;    ///< total nb of indexed std::vectors
    bool store_on_gpu;

    explicit Index(const std::shared_ptr<faiss::Index> &raw_index);

    virtual bool reset();

    /**
       * @brief Same as add, but stores xids instead of sequential ids.
       *
       * @param data input matrix, size n * d
       * @param if ids is not empty ids for the std::vectors
       */
    virtual bool add_with_ids(idx_t n, const float *xdata, const long *xids);

    /**
    * @brief for each query std::vector, find its k nearest neighbors in the database
    *
    * @param n queries size
    * @param data query std::vectors
    * @param k top k nearest neighbors
    * @param distances top k nearest distances
    * @param labels neighbors of the queries
    */
    virtual bool search(idx_t n, const float *data, idx_t k, float *distances, long *labels) const;

//    virtual bool remove_ids(const faiss::IDSelector &sel, long &nremove, long &location);

//    virtual bool remove_ids_range(const faiss::IDSelector &sel, long &nremove);

//    virtual bool index_display();
//
    virtual std::shared_ptr<faiss::Index> data() { return index_; }
    virtual const std::shared_ptr<faiss::Index>& data() const { return index_; }

private:
    friend void write_index(const Index_ptr &index, const std::string &file_name);
    std::shared_ptr<faiss::Index> index_ = nullptr;
//    std::vector<faiss::gpu::GpuResources *> res_;
//    std::vector<int> devs_;
//    bool usegpu = true;
//    int ngpus = 0;
//    faiss::gpu::GpuMultipleClonerOptions *options = new faiss::gpu::GpuMultipleClonerOptions();
};


void write_index(const Index_ptr &index, const std::string &file_name);

extern Index_ptr read_index(const std::string &file_name);


}
}
}
