#pragma once

#include <memory>
#include "knowhere/common/binary_set.h"
#include "knowhere/common/dataset.h"
#include "knowhere/index/index_type.h"
#include "knowhere/index/index_model.h"
#include "knowhere/index/preprocessor/preprocessor.h"


namespace zilliz {
namespace knowhere {


class Index {
 public:
    virtual BinarySet
    Serialize() = 0;

    virtual void
    Load(const BinarySet &index_binary) = 0;

    // @throw
    virtual DatasetPtr
    Search(const DatasetPtr &dataset, const Config &config) = 0;

 public:
    IndexType
    idx_type() const { return idx_type_; }

    void
    set_idx_type(IndexType idx_type) { idx_type_ = idx_type; }

    virtual void
    set_preprocessor(PreprocessorPtr preprocessor) {}

    virtual void
    set_index_model(IndexModelPtr model) {}

 private:
    IndexType idx_type_;
};


using IndexPtr = std::shared_ptr<Index>;


} // namespace knowhere
} // namespace zilliz
