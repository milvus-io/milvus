////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "wrapper/knowhere/vec_index.h"

#include <memory>

namespace zilliz {
namespace milvus {
namespace cache {

class DataObj {
public:
    DataObj(const engine::VecIndexPtr& index)
            : index_(index)
    {}

    DataObj(const engine::VecIndexPtr& index, int64_t size)
            : index_(index),
              size_(size)
    {}

    engine::VecIndexPtr data() { return index_; }
    const engine::VecIndexPtr& data() const { return index_; }

    int64_t size() const {
        if(index_ == nullptr) {
            return 0;
        }

        if(size_ > 0) {
            return size_;
        }

        return index_->Count() * index_->Dimension() * sizeof(float);
    }

private:
    engine::VecIndexPtr index_ = nullptr;
    int64_t size_ = 0;
};

using DataObjPtr = std::shared_ptr<DataObj>;

}
}
}