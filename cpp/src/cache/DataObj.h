////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "wrapper/Index.h"

#include <memory>

namespace zilliz {
namespace milvus {
namespace cache {

class DataObj {
public:
    DataObj(const engine::Index_ptr& index)
            : index_(index)
    {}

    engine::Index_ptr data() { return index_; }
    const engine::Index_ptr& data() const { return index_; }

    int64_t size() const {
        if(index_ == nullptr) {
            return 0;
        }

        return index_->ntotal*(index_->dim*4);
    }

private:
    engine::Index_ptr index_ = nullptr;
};

using DataObjPtr = std::shared_ptr<DataObj>;

}
}
}