#pragma once

#include <NGT/Command.h>
#include <NGT/Common.h>
#include <NGT/Index.h>

#include <knowhere/common/Exception.h>
#include <knowhere/index/vector_index/VecIndex.h>
#include <memory>

namespace milvus {
namespace knowhere {

class IndexNGT : public VecIndex {
 public:
    IndexNGT() {
        index_type_ = IndexEnum::INDEX_NGT;
    }

    BinarySet
    Serialize(const Config& config = Config()) override;

    void
    Load(const BinarySet& index_binary) override;

    void
    Train(const DatasetPtr& dataset_ptr, const Config& config) override;

    void
    Add(const DatasetPtr& dataset_ptr, const Config& config) override {
        KNOWHERE_THROW_MSG("NGT not support add item with ID, please invoke AddWithoutIds interface.");
    }

    void
    AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) override;

    DatasetPtr
    Query(const DatasetPtr& dataset_ptr, const Config& config) override;

    int64_t
    Count() override;

    int64_t
    Dim() override;

 private:
    std::shared_ptr<NGT::Index> index_ = nullptr;
};

}  // namespace knowhere
}  // namespace milvus
