////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "knowhere/index/vector_index/vector_index.h"


namespace zilliz {
namespace knowhere {

namespace algo {
class NsgIndex;
}

class NSG : public VectorIndex {
 public:
    explicit NSG(const int64_t& gpu_num):gpu_(gpu_num){}
    NSG() = default;

    IndexModelPtr Train(const DatasetPtr &dataset, const Config &config) override;
    DatasetPtr Search(const DatasetPtr &dataset, const Config &config) override;
    void Add(const DatasetPtr &dataset, const Config &config) override;
    BinarySet Serialize() override;
    void Load(const BinarySet &index_binary) override;
    int64_t Count() override;
    int64_t Dimension() override;

 private:
    std::shared_ptr<algo::NsgIndex> index_;
    int64_t gpu_;
};

using NSGIndexPtr = std::shared_ptr<NSG>();

}
}
