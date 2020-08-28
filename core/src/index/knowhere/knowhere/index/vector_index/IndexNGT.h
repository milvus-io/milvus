#pragma once

#include <NGT/lib/NGT/Command.h>
#include <NGT/lib/NGT/Common.h>
#include <NGT/lib/NGT/Index.h>

#include <knowhere/common/Exception.h>
#include <knowhere/index/IndexType.h>
#include <knowhere/index/vector_index/VecIndex.h>
#include <memory>

namespace milvus {
namespace knowhere {

class IndexNGT : public VecIndex {
 public:
    IndexNGT() {
        index_type_ = IndexEnum::INVALID;
    }

    BinarySet
    Serialize(const Config& config = Config()) override;

    void
    Load(const BinarySet& index_binary) override;

    void
    BuildAll(const DatasetPtr& dataset_ptr, const Config& config) override;

    void
    Train(const DatasetPtr& dataset_ptr, const Config& config) override {
        KNOWHERE_THROW_MSG("NGT not support add item dynamically, please invoke BuildAll interface.");
    }

    void
    Add(const DatasetPtr& dataset_ptr, const Config& config) override {
        KNOWHERE_THROW_MSG("NGT not support add item dynamically, please invoke BuildAll interface.");
    }

    void
    AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) override {
        KNOWHERE_THROW_MSG("Incremental index is not supported");
    }

    DatasetPtr
    Query(const DatasetPtr& dataset_ptr, const Config& config) override;

    int64_t
    Count() override;

    int64_t
    Dim() override;

 protected:
    std::shared_ptr<NGT::Index> index_ = nullptr;
};

}  // namespace knowhere
}  // namespace milvus
