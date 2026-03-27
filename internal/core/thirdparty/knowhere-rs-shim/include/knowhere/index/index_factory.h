#pragma once

#include <memory>
#include <string>
#include <utility>

#include "knowhere/comp/index_param.h"
#include "knowhere/config.h"
#include "knowhere/index_node.h"
#include "knowhere/version.h"

namespace knowhere {

template <typename T>
class Index {
 public:
    Index() = default;

    explicit Index(std::shared_ptr<T> node) : node_(std::move(node)) {
    }

    Status
    Build(const DataSet& dataset, const Config& config) {
        return node_ == nullptr ? Status::invalid_index_error
                                : node_->Build(dataset, config);
    }

    Status
    Train(const DataSet& dataset, const Config& config) {
        return node_ == nullptr ? Status::invalid_index_error
                                : node_->Train(dataset, config);
    }

    Status
    Add(const DataSet& dataset, const Config& config) {
        return node_ == nullptr ? Status::invalid_index_error
                                : node_->Add(dataset, config);
    }

    expected<DataSetPtr>
    Search(const DataSet& dataset,
           const Config& config,
           const BitsetView& bitset) const {
        return node_ == nullptr ? expected<DataSetPtr>(Status::invalid_index_error)
                                : node_->Search(dataset, config, bitset);
    }

    expected<std::vector<IndexNode::IteratorPtr>>
    AnnIterator(const DataSet& dataset,
                const Config& config,
                const BitsetView& bitset) const {
        return node_ == nullptr
                   ? expected<std::vector<IndexNode::IteratorPtr>>(
                         Status::invalid_index_error)
                   : node_->AnnIterator(dataset, config, bitset);
    }

    Status
    Serialize(BinarySet& binary_set) const {
        return node_ == nullptr ? Status::invalid_index_error
                                : node_->Serialize(binary_set);
    }

    Status
    Deserialize(const BinarySet& binary_set, const Config& config = {}) {
        return node_ == nullptr ? Status::invalid_index_error
                                : node_->Deserialize(binary_set, config);
    }

    int64_t
    Dim() const {
        return node_ == nullptr ? 0 : node_->Dim();
    }

    int64_t
    Size() const {
        return node_ == nullptr ? 0 : node_->Size();
    }

    int64_t
    Count() const {
        return node_ == nullptr ? 0 : node_->Count();
    }

    bool
    HasRawData(const std::string& metric_type) const {
        return node_ != nullptr && node_->HasRawData(metric_type);
    }

    const std::shared_ptr<T>&
    GetPtr() const {
        return node_;
    }

 private:
    std::shared_ptr<T> node_;
};

class UnsupportedIndexNode : public IndexNode {
 public:
    explicit UnsupportedIndexNode(std::string index_type)
        : index_type_(std::move(index_type)) {
    }

    Status
    Train(const DataSet&, const Config&) override {
        return Status::not_implemented;
    }

    Status
    Add(const DataSet&, const Config&) override {
        return Status::not_implemented;
    }

    expected<DataSetPtr>
    Search(const DataSet&, const Config&, const BitsetView&) const override {
        return Status::not_implemented;
    }

    expected<DataSetPtr>
    RangeSearch(const DataSet&, const Config&, const BitsetView&) const override {
        return Status::not_implemented;
    }

    expected<DataSetPtr>
    GetVectorByIds(const DataSet&) const override {
        return Status::not_implemented;
    }

    bool
    HasRawData(const std::string&) const override {
        return false;
    }

    expected<DataSetPtr>
    GetIndexMeta(const Config&) const override {
        return Status::not_implemented;
    }

    Status
    Serialize(BinarySet&) const override {
        return Status::not_implemented;
    }

    Status
    Deserialize(const BinarySet&, const Config&) override {
        return Status::not_implemented;
    }

    Status
    DeserializeFromFile(const std::string&, const Config&) override {
        return Status::not_implemented;
    }

    std::unique_ptr<BaseConfig>
    CreateConfig() const override {
        return std::make_unique<BaseConfig>();
    }

    int64_t
    Dim() const override {
        return 0;
    }

    int64_t
    Size() const override {
        return 0;
    }

    int64_t
    Count() const override {
        return 0;
    }

    std::string
    Type() const override {
        return index_type_;
    }

 private:
    std::string index_type_;
};

class IndexFactory {
 public:
    static IndexFactory&
    Instance() {
        static IndexFactory instance;
        return instance;
    }

    template <typename T, typename... Args>
    expected<Index<IndexNode>>
    Create(const std::string& name, Args&&...) const {
        return Index<IndexNode>(
            std::make_shared<UnsupportedIndexNode>(name));
    }

    bool
    FeatureCheck(const std::string&, feature::Type) const {
        return false;
    }

 private:
    IndexFactory() = default;
};

}  // namespace knowhere
