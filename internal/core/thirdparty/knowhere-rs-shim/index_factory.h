#pragma once

#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "knowhere/comp/index_param.h"
#include "knowhere/config.h"
#include "knowhere/index_node.h"
#include "knowhere/index/raw_data_index_node.h"
#include "knowhere/version.h"

namespace milvus {
class OpContext;
}

namespace knowhere {

inline constexpr uint64_t kBinaryFlag = 1ULL << 0;
inline constexpr uint64_t kFloat32Flag = 1ULL << 1;
inline constexpr uint64_t kFloat16Flag = 1ULL << 2;
inline constexpr uint64_t kBFloat16Flag = 1ULL << 3;
inline constexpr uint64_t kSparseFloat32Flag = 1ULL << 4;
inline constexpr uint64_t kInt8Flag = 1ULL << 5;
inline constexpr uint64_t kEmbeddingListFlag = 1ULL << 15;
inline constexpr uint64_t kNoTrainFlag = 1ULL << 16;
inline constexpr uint64_t kKnnFlag = 1ULL << 17;
inline constexpr uint64_t kGpuFlag = 1ULL << 18;
inline constexpr uint64_t kMmapFlag = 1ULL << 19;
inline constexpr uint64_t kMvFlag = 1ULL << 20;
inline constexpr uint64_t kDiskFlag = 1ULL << 21;

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
    Build(std::nullptr_t, const Config&, bool = true) {
        return node_ == nullptr ? Status::invalid_index_error
                                : Status::invalid_args;
    }

    Status
    Train(const DataSet& dataset, const Config& config) {
        return node_ == nullptr ? Status::invalid_index_error
                                : node_->Train(dataset, config);
    }

    Status
    Train(const DataSetPtr& dataset,
          const Config& config,
          bool = true) {
        return node_ == nullptr ? Status::invalid_index_error
               : dataset == nullptr ? Status::invalid_args
                                    : node_->Train(*dataset, config);
    }

    Status
    Add(const DataSet& dataset, const Config& config) {
        return node_ == nullptr ? Status::invalid_index_error
                                : node_->Add(dataset, config);
    }

    Status
    Add(std::nullptr_t, const Config&, bool = true) {
        return node_ == nullptr ? Status::invalid_index_error
                                : Status::invalid_args;
    }

    Status
    Build(const DataSetPtr& dataset,
          const Config& config,
          bool = true) {
        return node_ == nullptr ? Status::invalid_index_error
               : dataset == nullptr ? Status::invalid_args
                                    : node_->Build(*dataset, config);
    }

    Status
    Add(const DataSetPtr& dataset,
        const Config& config,
        bool = true) {
        return node_ == nullptr ? Status::invalid_index_error
               : dataset == nullptr ? Status::invalid_args
                                    : node_->Add(*dataset, config);
    }

    expected<DataSetPtr>
    Search(const DataSet& dataset,
           const Config& config,
           const BitsetView& bitset) const {
        return node_ == nullptr ? expected<DataSetPtr>(Status::invalid_index_error)
                                : node_->Search(dataset, config, bitset);
    }

    expected<DataSetPtr>
    Search(const DataSetPtr& dataset,
           const Config& config,
           const BitsetView& bitset,
           milvus::OpContext* = nullptr) const {
        return node_ == nullptr ? expected<DataSetPtr>(Status::invalid_index_error)
               : dataset == nullptr ? expected<DataSetPtr>(Status::invalid_args)
                                    : node_->Search(*dataset, config, bitset);
    }

    expected<DataSetPtr>
    RangeSearch(const DataSetPtr& dataset,
                const Config& config,
                const BitsetView& bitset,
                milvus::OpContext* = nullptr) const {
        return node_ == nullptr ? expected<DataSetPtr>(Status::invalid_index_error)
               : dataset == nullptr ? expected<DataSetPtr>(Status::invalid_args)
                                    : node_->RangeSearch(*dataset, config, bitset);
    }

    expected<DataSetPtr>
    GetVectorByIds(const DataSetPtr& dataset,
                   milvus::OpContext* = nullptr) const {
        return node_ == nullptr ? expected<DataSetPtr>(Status::invalid_index_error)
               : dataset == nullptr ? expected<DataSetPtr>(Status::invalid_args)
                                    : node_->GetVectorByIds(*dataset);
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

    expected<std::vector<IndexNode::IteratorPtr>>
    AnnIterator(const DataSetPtr& dataset,
                const Config& config,
                const BitsetView& bitset,
                bool = true,
                milvus::OpContext* = nullptr) const {
        return node_ == nullptr
                   ? expected<std::vector<IndexNode::IteratorPtr>>(
                         Status::invalid_index_error)
               : dataset == nullptr
                   ? expected<std::vector<IndexNode::IteratorPtr>>(
                         Status::invalid_args)
                   : node_->AnnIterator(*dataset, config, bitset);
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

    Status
    DeserializeFromFile(const std::string& filename, const Config& config = {}) {
        return node_ == nullptr ? Status::invalid_index_error
                                : node_->DeserializeFromFile(filename, config);
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

    bool
    IsAdditionalScalarSupported(bool) const {
        return false;
    }

    expected<DataSetPtr>
    GetIndexMeta(const Config& config) const {
        return node_ == nullptr ? expected<DataSetPtr>(Status::invalid_index_error)
                                : node_->GetIndexMeta(config);
    }

    std::string
    Type() const {
        return node_ == nullptr ? std::string{} : node_->Type();
    }

    bool
    LoadIndexWithStream() const {
        return false;
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
        std::fprintf(stderr,
                     "[knowhere-rs-shim][get-vector] node=unsupported index=%s\n",
                     index_type_.c_str());
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

std::shared_ptr<IndexNode>
MakeHnswRustNode();

std::shared_ptr<IndexNode>
MakeSparseRustNode();

std::shared_ptr<IndexNode>
MakeDiskAnnRustNode();


class IndexFactory {
 public:
    using FeatureMap = std::map<std::string, uint64_t>;

    static IndexFactory&
    Instance() {
        static IndexFactory instance;
        return instance;
    }

    template <typename T, typename... Args>
    expected<Index<IndexNode>>
    Create(const std::string& name, Args&&...) const {
        if (name == IndexEnum::INDEX_HNSW ||
            name == IndexEnum::INDEX_HNSW_SQ) {
            if constexpr (std::is_same_v<T, bin1>) {
                return Index<IndexNode>(
                    MakeRawDataIndexNode(name, RawDataKind::Binary));
            }
            if constexpr (std::is_same_v<T, int8>) {
                return Index<IndexNode>(
                    MakeRawDataIndexNode(name, RawDataKind::Int8));
            }
            if constexpr (std::is_same_v<T, fp16>) {
                return Index<IndexNode>(
                    MakeRawDataIndexNode(name, RawDataKind::Float16));
            }
            if constexpr (std::is_same_v<T, bf16>) {
                return Index<IndexNode>(
                    MakeRawDataIndexNode(name, RawDataKind::BFloat16));
            }
            return Index<IndexNode>(MakeHnswRustNode());
        }
        if (name == IndexEnum::INDEX_FAISS_BIN_IDMAP ||
            name == IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
            return Index<IndexNode>(
                MakeRawDataIndexNode(name, RawDataKind::Binary));
        }
        if (name == IndexEnum::INDEX_DISKANN) {
            if constexpr (std::is_same_v<T, float>) {
                return Index<IndexNode>(MakeDiskAnnRustNode());
            }
        }
                if (name == IndexEnum::INDEX_FAISS_IDMAP ||
            name == IndexEnum::INDEX_FAISS_IVFFLAT ||
            name == IndexEnum::INDEX_FAISS_IVFFLAT_CC ||
            name == IndexEnum::INDEX_FAISS_IVFPQ ||
            name == IndexEnum::INDEX_FAISS_IVF_RABITQ ||
            name == IndexEnum::INDEX_FAISS_IVFSQ8 ||
            name == IndexEnum::INDEX_FAISS_IVFSQ ||
            name == IndexEnum::INDEX_FAISS_SCANN_DVR ||
            name == IndexEnum::INDEX_FAISS_SCANN_DVR) {
            if constexpr (std::is_same_v<T, float>) {
                return Index<IndexNode>(
                    MakeRawDataIndexNode(name, RawDataKind::Float32));
            }
            if constexpr (std::is_same_v<T, fp16>) {
                return Index<IndexNode>(
                    MakeRawDataIndexNode(name, RawDataKind::Float16));
            }
            if constexpr (std::is_same_v<T, bf16>) {
                return Index<IndexNode>(
                    MakeRawDataIndexNode(name, RawDataKind::BFloat16));
            }
        }
        if (name == IndexEnum::INDEX_SPARSE_INVERTED_INDEX) {
            if constexpr (std::is_same_v<T, float>) {
                return Index<IndexNode>(MakeSparseRustNode());
            }
        }
        return Index<IndexNode>(
            std::make_shared<UnsupportedIndexNode>(name));
    }

    bool
    FeatureCheck(const std::string&, feature::Type) const {
        return false;
    }

    static const FeatureMap&
    GetIndexFeatures() {
        static const FeatureMap features = {
            {IndexEnum::INDEX_HNSW,
             kBinaryFlag | kFloat32Flag | kFloat16Flag | kBFloat16Flag |
                 kInt8Flag | kMvFlag},
            {IndexEnum::INDEX_HNSW_SQ,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag | kInt8Flag |
                 kMvFlag},
            {IndexEnum::INDEX_FAISS_IDMAP,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag | kNoTrainFlag |
                 kKnnFlag},
            {IndexEnum::INDEX_FAISS_IVFFLAT,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag},
            {IndexEnum::INDEX_FAISS_IVFFLAT_CC,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag},
            {IndexEnum::INDEX_FAISS_IVFPQ,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag},
            {IndexEnum::INDEX_FAISS_IVF_RABITQ,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag},
            {IndexEnum::INDEX_FAISS_IVFSQ8,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag},
            {IndexEnum::INDEX_FAISS_IVFSQ,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag},
            {IndexEnum::INDEX_FAISS_SCANN_DVR,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag},
            {IndexEnum::INDEX_DISKANN,
             kFloat32Flag | kFloat16Flag | kBFloat16Flag | kDiskFlag},
            {IndexEnum::INDEX_FAISS_BIN_IDMAP, kBinaryFlag | kNoTrainFlag | kKnnFlag},
            {IndexEnum::INDEX_FAISS_BIN_IVFFLAT, kBinaryFlag},
            {IndexEnum::INDEX_SPARSE_INVERTED_INDEX, kSparseFloat32Flag},
            {"MINHASH_LSH", kBinaryFlag},
        };
        return features;
    }

 private:
    IndexFactory() = default;
};

}  // namespace knowhere
