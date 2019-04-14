////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>

namespace zilliz {
namespace vecwise {
namespace cache {

class DataObj {
public:
    DataObj(const std::shared_ptr<char>& data, int64_t size)
            : data_(data), size_(size)
    {}

    std::shared_ptr<char> data() { return data_; }
    const std::shared_ptr<char>& data() const { return data_; }

    int64_t size() const { return size_; }

private:
    std::shared_ptr<char> data_ = nullptr;
    int64_t size_ = 0;
};

using DataObjPtr = std::shared_ptr<DataObj>;

}
}
}