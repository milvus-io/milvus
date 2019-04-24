////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "faiss/AutoTune.h"
#include "faiss/AuxIndexStructures.h"
#include "faiss/gpu/GpuAutoTune.h"
#include "faiss/index_io.h"

#include <memory>

namespace zilliz {
namespace vecwise {
namespace cache {

class DataObj {
public:
    DataObj(const std::shared_ptr<faiss::Index>& index)
            : index_(index)
    {}

    std::shared_ptr<faiss::Index> data() { return index_; }
    const std::shared_ptr<faiss::Index>& data() const { return index_; }

    int64_t size() const {
        if(index_ == nullptr) {
            return 0;
        }

        return index_->ntotal*(index_->d*4 + sizeof(faiss::Index::idx_t));
    }

private:
    std::shared_ptr<faiss::Index> index_ = nullptr;
    int64_t size_ = 0;
};

using DataObjPtr = std::shared_ptr<DataObj>;

}
}
}