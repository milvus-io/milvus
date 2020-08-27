#pragma once

#include "knowhere/index/vector_index/IndexNGT.h"

namespace milvus {
namespace knowhere {

class IndexNGTPANNG : public IndexNGT {
 public:
    IndexNGTPANNG() {
        index_type_ = IndexEnum::INDEX_NGTPANNG;
    }

    void
    Train(const DatasetPtr& dataset_ptr, const Config& config) override;
};

}  // namespace knowhere
}  // namespace milvus

