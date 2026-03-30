#pragma once

#include <algorithm>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "knowhere/bitsetview.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/config.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "knowhere/index_node.h"

namespace knowhere {

enum class RawDataKind {
    Float32,
    Float16,
    BFloat16,
    Int8,
    Binary,
};

inline const char*
RawDataKindName(RawDataKind kind) {
    switch (kind) {
        case RawDataKind::Float32:
            return "float32";
        case RawDataKind::Float16:
            return "float16";
        case RawDataKind::BFloat16:
            return "bfloat16";
        case RawDataKind::Int8:
            return "int8";
        case RawDataKind::Binary:
            return "binary";
    }
    return "unknown";
}

inline bool
IsValidRawDataMetric(RawDataKind kind, const std::string& metric_name) {
    if (metric_name.empty()) {
        return true;
    }
    if (kind == RawDataKind::Float32 || kind == RawDataKind::Float16 ||
        kind == RawDataKind::BFloat16 || kind == RawDataKind::Int8) {
        return metric_name == metric::L2 || metric_name == metric::IP ||
               metric_name == metric::COSINE;
    }
    return metric_name == metric::HAMMING || metric_name == metric::JACCARD ||
           metric_name == metric::SUBSTRUCTURE ||
           metric_name == metric::SUPERSTRUCTURE;
}

inline size_t
RawTensorBytesPerRow(RawDataKind kind, int64_t dim) {
    if (kind == RawDataKind::Float32) {
        return static_cast<size_t>(dim) * sizeof(float);
    }
    if (kind == RawDataKind::Float16 || kind == RawDataKind::BFloat16) {
        return static_cast<size_t>(dim) * sizeof(uint16_t);
    }
    if (kind == RawDataKind::Int8) {
        return static_cast<size_t>(dim) * sizeof(int8);
    }
    return static_cast<size_t>((dim + 7) / 8);
}

class RawDataIndexNode : public IndexNode {
 public:
    RawDataIndexNode(std::string index_type, RawDataKind kind)
        : index_type_(std::move(index_type)), kind_(kind) {
    }

    Status
    Build(const DataSet& dataset, const Config& config) override {
        RETURN_IF_ERROR(ValidateConfig(config));
        return StoreDataset(dataset);
    }

    Status
    Train(const DataSet&, const Config& config) override {
        return ValidateConfig(config);
    }

    Status
    Add(const DataSet& dataset, const Config& config) override {
        RETURN_IF_ERROR(ValidateConfig(config));
        return StoreDataset(dataset);
    }

    expected<DataSetPtr>
    Search(const DataSet& dataset,
           const Config& config,
           const BitsetView& bitset) const override {
        if (tensor_ == nullptr || rows_ <= 0 || dim_ <= 0) {
            return Status::empty_index;
        }
        if (const auto validation = ValidateSearchConfig(config);
            !validation.empty()) {
            expected<DataSetPtr> result(validation.status);
            result << validation.message;
            return result;
        }

        auto base = GenDataSet(rows_, dim_, tensor_.get());
        base->SetTensorBeginId(tensor_begin_id_);
        if (!raw_ids_.empty()) {
            base->SetIds(raw_ids_.data());
        }
        auto query = GenDataSet(dataset.GetRows(), dataset.GetDim(), dataset.GetTensor());
        query->SetTensorBeginId(dataset.GetTensorBeginId());

        if (kind_ == RawDataKind::Float32) {
            return BruteForce::Search<float>(base, query, config, bitset);
        }
        if (kind_ == RawDataKind::Int8) {
            return BruteForce::Search<int8>(base, query, config, bitset);
        }
        if (kind_ == RawDataKind::Binary) {
            return BruteForce::Search<bin1>(base, query, config, bitset);
        }
        if (kind_ == RawDataKind::Float16) {
            return BruteForce::Search<fp16>(base, query, config, bitset);
        }
        return BruteForce::Search<bf16>(base, query, config, bitset);
    }

    expected<std::vector<IteratorPtr>>
    AnnIterator(const DataSet& dataset,
                const Config& config,
                const BitsetView& bitset) const override {
        if (tensor_ == nullptr || rows_ <= 0 || dim_ <= 0) {
            return Status::empty_index;
        }
        if (const auto validation = ValidateSearchConfig(config);
            !validation.empty()) {
            expected<std::vector<IteratorPtr>> result(validation.status);
            result << validation.message;
            return result;
        }

        if (index_type_ == IndexEnum::INDEX_FAISS_IDMAP && !bitset.empty()) {
            int64_t visible_rows = 0;
            int64_t first_visible = -1;
            for (int64_t row = 0; row < rows_; ++row) {
                if (row < static_cast<int64_t>(bitset.size()) && bitset.test(row)) {
                    continue;
                }
                ++visible_rows;
                if (first_visible < 0) {
                    first_visible = row;
                }
            }
            std::fprintf(stderr,
                         "[knowhere-rs-shim][raw-ann] index=%s rows=%lld bitset=%zu "
                         "visible=%lld first_visible=%lld topk=%lld\n",
                         index_type_.c_str(),
                         static_cast<long long>(rows_),
                         bitset.size(),
                         static_cast<long long>(visible_rows),
                         static_cast<long long>(first_visible),
                         static_cast<long long>(
                             config.value(meta::TOPK, int64_t{0})));
        }

        auto base = GenDataSet(rows_, dim_, tensor_.get());
        base->SetTensorBeginId(tensor_begin_id_);
        if (!raw_ids_.empty()) {
            base->SetIds(raw_ids_.data());
        }
        auto query = GenDataSet(dataset.GetRows(), dataset.GetDim(), dataset.GetTensor());
        query->SetTensorBeginId(dataset.GetTensorBeginId());

        if (kind_ == RawDataKind::Float32) {
            return BruteForce::AnnIterator<float>(base, query, config, bitset);
        }
        if (kind_ == RawDataKind::Int8) {
            return BruteForce::AnnIterator<int8>(base, query, config, bitset);
        }
        if (kind_ == RawDataKind::Binary) {
            return BruteForce::AnnIterator<bin1>(base, query, config, bitset);
        }
        if (kind_ == RawDataKind::Float16) {
            return BruteForce::AnnIterator<fp16>(base, query, config, bitset);
        }
        return BruteForce::AnnIterator<bf16>(base, query, config, bitset);
    }

    expected<DataSetPtr>
    RangeSearch(const DataSet&, const Config&, const BitsetView&) const override {
        return Status::not_implemented;
    }

    expected<DataSetPtr>
    GetVectorByIds(const DataSet& dataset) const override {
        if (tensor_ == nullptr || rows_ <= 0 || dim_ <= 0) {
            return expected<DataSetPtr>(Status::empty_index);
        }

        const auto count = dataset.GetRows();
        const auto* requested_ids = dataset.GetIds();
        if (count < 0 || requested_ids == nullptr) {
            return expected<DataSetPtr>(Status::invalid_args);
        }

        std::fprintf(stderr,
                     "[knowhere-rs-shim][get-vector] node=raw index=%s kind=%s "
                     "rows=%lld dim=%lld req=%lld\n",
                     index_type_.c_str(),
                     RawDataKindName(kind_),
                     static_cast<long long>(rows_),
                     static_cast<long long>(dim_),
                     static_cast<long long>(count));

        auto result = std::make_shared<DataSet>();
        result->SetRows(count);
        result->SetDim(dim_);
        if (count == 0) {
            result->SetIsOwner(true);
            return result;
        }

        const auto row_bytes = RawTensorBytesPerRow(kind_, dim_);
        const auto total_bytes =
            static_cast<size_t>(count) * static_cast<size_t>(row_bytes);
        auto* copied_ids = new int64_t[static_cast<size_t>(count)];
        auto* copied_tensor = new char[total_bytes];

        for (int64_t i = 0; i < count; ++i) {
            const auto row_offset = FindRowOffsetById(requested_ids[i]);
            if (row_offset < 0) {
                delete[] copied_ids;
                delete[] copied_tensor;
                return expected<DataSetPtr>(Status::invalid_args);
            }
            std::memcpy(copied_tensor + static_cast<size_t>(i) * row_bytes,
                        tensor_.get() +
                            static_cast<size_t>(row_offset) * row_bytes,
                        row_bytes);
            copied_ids[static_cast<size_t>(i)] = requested_ids[i];
        }

        result->SetIds(copied_ids);
        result->SetTensor(copied_tensor);
        result->SetTensorBeginId(copied_ids[0]);
        result->SetIsOwner(true);
        return result;
    }

    bool
    HasRawData(const std::string&) const override {
        return true;
    }

    expected<DataSetPtr>
    GetIndexMeta(const Config&) const override {
        return Status::not_implemented;
    }

    Status
    Serialize(BinarySet& binary_set) const override {
        if (tensor_ == nullptr || tensor_bytes_ == 0) {
            return Status::empty_index;
        }

        auto owned = std::shared_ptr<uint8_t[]>(new uint8_t[tensor_bytes_]);
        std::memcpy(owned.get(), tensor_.get(), tensor_bytes_);
        binary_set.Append("index_data", owned, static_cast<int64_t>(tensor_bytes_));

        Json meta_json = {
            {"rows", rows_},
            {"dim", dim_},
            {"tensor_begin_id", tensor_begin_id_},
            {"tensor_bytes", tensor_bytes_},
            {"kind", RawDataKindName(kind_)},
            {"index_type", index_type_},
        };
        const auto meta_text = meta_json.dump();
        auto meta_owned = std::shared_ptr<uint8_t[]>(new uint8_t[meta_text.size()]);
        std::memcpy(meta_owned.get(), meta_text.data(), meta_text.size());
        binary_set.Append(
            "index_meta", meta_owned, static_cast<int64_t>(meta_text.size()));
        if (!raw_ids_.empty()) {
            const auto ids_bytes = raw_ids_.size() * sizeof(int64_t);
            auto ids_owned = std::shared_ptr<uint8_t[]>(new uint8_t[ids_bytes]);
            std::memcpy(ids_owned.get(), raw_ids_.data(), ids_bytes);
            binary_set.Append(
                "index_ids", std::move(ids_owned), static_cast<int64_t>(ids_bytes));
        }
        return Status::success;
    }

    Status
    Deserialize(const BinarySet& binary_set, const Config&) override {
        auto data = binary_set.GetByNames({"index_data"});
        auto meta = binary_set.GetByNames({"index_meta"});
        if (data == nullptr || data->data == nullptr || data->size <= 0 ||
            meta == nullptr || meta->data == nullptr || meta->size <= 0) {
            return Status::invalid_binary_set;
        }

        Json meta_json = Json::parse(
            reinterpret_cast<const char*>(meta->data.get()),
            reinterpret_cast<const char*>(meta->data.get()) + meta->size);

        rows_ = meta_json.value("rows", int64_t{0});
        dim_ = meta_json.value("dim", int64_t{0});
        tensor_begin_id_ = meta_json.value("tensor_begin_id", int64_t{0});
        tensor_bytes_ = meta_json.value("tensor_bytes", size_t{0});
        index_type_ = meta_json.value("index_type", index_type_);
        const auto kind_name = meta_json.value("kind", std::string{});
        if (kind_name == "binary") {
            kind_ = RawDataKind::Binary;
        } else if (kind_name == "float16") {
            kind_ = RawDataKind::Float16;
        } else if (kind_name == "bfloat16") {
            kind_ = RawDataKind::BFloat16;
        } else if (kind_name == "int8") {
            kind_ = RawDataKind::Int8;
        } else {
            kind_ = RawDataKind::Float32;
        }

        if (rows_ <= 0 || dim_ <= 0 || tensor_bytes_ == 0 ||
            data->size != static_cast<int64_t>(tensor_bytes_)) {
            return Status::invalid_binary_set;
        }

        tensor_ = std::shared_ptr<uint8_t[]>(new uint8_t[tensor_bytes_]);
        std::memcpy(tensor_.get(), data->data.get(), tensor_bytes_);
        raw_ids_.clear();
        auto ids = binary_set.GetByName("index_ids");
        if (ids != nullptr && ids->data != nullptr && ids->size > 0) {
            if (ids->size != rows_ * static_cast<int64_t>(sizeof(int64_t))) {
                return Status::invalid_binary_set;
            }
            raw_ids_.resize(static_cast<size_t>(rows_));
            std::memcpy(raw_ids_.data(), ids->data.get(), static_cast<size_t>(ids->size));
        } else {
            raw_ids_.reserve(static_cast<size_t>(rows_));
            for (int64_t row = 0; row < rows_; ++row) {
                raw_ids_.push_back(tensor_begin_id_ + row);
            }
        }
        return Status::success;
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
        return dim_;
    }

    int64_t
    Size() const override {
        return static_cast<int64_t>(tensor_bytes_);
    }

    int64_t
    Count() const override {
        return rows_;
    }

    std::string
    Type() const override {
        return index_type_;
    }

 private:
    struct ValidationResult {
        Status status = Status::success;
        std::string message;

        bool
        empty() const {
            return status == Status::success;
        }
    };

    ValidationResult
    ValidateSearchConfig(const Config& config) const {
        ValidationResult result;
        if (index_type_ == IndexEnum::INDEX_HNSW) {
            result.status = ValidateHnswSearchConfig(config, result.message);
        } else if (index_type_ == IndexEnum::INDEX_FAISS_IVF_RABITQ) {
            result.status = ValidateIvfRabitqSearchConfig(config, result.message);
        }
        return result;
    }

    Status
    ValidateConfig(const Config& config) const {
        const auto metric_name =
            config.value(meta::METRIC_TYPE, std::string{});
        if (!IsValidRawDataMetric(kind_, metric_name)) {
            return Status::invalid_metric_type;
        }
        return Status::success;
    }

    Status
    StoreDataset(const DataSet& dataset) {
        const auto* tensor = reinterpret_cast<const uint8_t*>(dataset.GetTensor());
        const auto rows = dataset.GetRows();
        const auto dim = dataset.GetDim();
        if (tensor == nullptr || rows <= 0 || dim <= 0) {
            return Status::invalid_args;
        }

        rows_ = rows;
        dim_ = dim;
        tensor_begin_id_ = dataset.GetTensorBeginId();
        tensor_bytes_ =
            static_cast<size_t>(rows_) * RawTensorBytesPerRow(kind_, dim_);
        tensor_ = std::shared_ptr<uint8_t[]>(new uint8_t[tensor_bytes_]);
        std::memcpy(tensor_.get(), tensor, tensor_bytes_);
        raw_ids_.clear();
        raw_ids_.reserve(static_cast<size_t>(rows_));
        if (const auto* ids = dataset.GetIds(); ids != nullptr) {
            raw_ids_.insert(raw_ids_.end(), ids, ids + rows_);
        } else {
            for (int64_t row = 0; row < rows_; ++row) {
                raw_ids_.push_back(tensor_begin_id_ + row);
            }
        }
        return Status::success;
    }

    int64_t
    FindRowOffsetById(int64_t id) const {
        if (!raw_ids_.empty()) {
            const auto it = std::find(raw_ids_.begin(), raw_ids_.end(), id);
            if (it == raw_ids_.end()) {
                return -1;
            }
            return static_cast<int64_t>(std::distance(raw_ids_.begin(), it));
        }

        const auto row_offset = id - tensor_begin_id_;
        return row_offset >= 0 && row_offset < rows_ ? row_offset : -1;
    }

    std::string index_type_;
    RawDataKind kind_;
    std::shared_ptr<uint8_t[]> tensor_;
    std::vector<int64_t> raw_ids_;
    size_t tensor_bytes_ = 0;
    int64_t rows_ = 0;
    int64_t dim_ = 0;
    int64_t tensor_begin_id_ = 0;
};

inline std::shared_ptr<IndexNode>
MakeRawDataIndexNode(const std::string& index_type, RawDataKind kind) {
    return std::make_shared<RawDataIndexNode>(index_type, kind);
}

}  // namespace knowhere
