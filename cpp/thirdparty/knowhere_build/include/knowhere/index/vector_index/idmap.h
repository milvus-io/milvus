////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "ivf.h"


namespace zilliz {
namespace knowhere {

class IDMAP : public VectorIndex, public BasicIndex {
 public:
    IDMAP() : BasicIndex(nullptr) {};
    BinarySet Serialize() override;
    void Load(const BinarySet &index_binary) override;
    void Train(const Config &config);
    DatasetPtr Search(const DatasetPtr &dataset, const Config &config) override;
    int64_t Count() override;
    int64_t Dimension() override;
    void Add(const DatasetPtr &dataset, const Config &config) override;

    float* GetRawVectors();
    int64_t* GetRawIds();

 protected:
    std::mutex mutex_;
};

using IDMAPPtr = std::shared_ptr<IDMAP>;

}
}
