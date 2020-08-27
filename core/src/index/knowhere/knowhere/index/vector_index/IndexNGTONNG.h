#pragma once

#include "knowhere/index/vector_index/IndexNGT.h"

namespace milvus {
namespace knowhere {

class IndexNGTONNG : public IndexNGT {
 public:
    IndexNGTONNG() {
        index_type_ = IndexEnum::INDEX_NGTONNG;
    }

    void
    Train(const DatasetPtr& dataset_ptr, const Config& config) override;
};

}  // namespace knowhere
}  // namespace milvus

