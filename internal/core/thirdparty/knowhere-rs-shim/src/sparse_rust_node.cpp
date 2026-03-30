#include "knowhere/index/index_factory.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cabi_bridge.hpp"
#include "status.hpp"

namespace knowhere {
namespace {

using SparseValue = sparse_u32_f32::ValueType;
using SparseRow = sparse::SparseRow<SparseValue>;

std::optional<size_t>
GetOptionalSizeT(const Config& config, const char* key) {
    if (!config.contains(key)) {
        return std::nullopt;
    }
    const auto& value = config.at(key);
    if (value.is_number_unsigned()) {
        return value.get<size_t>();
    }
    if (value.is_number_integer()) {
        const auto v = value.get<int64_t>();
        if (v >= 0) {
            return static_cast<size_t>(v);
        }
    }
    if (value.is_string()) {
        const auto& str = value.get_ref<const std::string&>();
        if (!str.empty()) {
            return static_cast<size_t>(std::stoull(str));
        }
    }
    return std::nullopt;
}

std::string
GetString(const Config& config, const char* key, std::string fallback) {
    if (!config.contains(key)) {
        return fallback;
    }
    const auto& value = config.at(key);
    if (value.is_string()) {
        return value.get<std::string>();
    }
    return fallback;
}

int64_t
ResolveSparseDim(const DataSet& dataset, int64_t fallback_dim) {
    int64_t resolved_dim = std::max<int64_t>(dataset.GetDim(), fallback_dim);
    if (!dataset.IsSparse() || dataset.GetRows() <= 0 || dataset.GetTensor() == nullptr) {
        return resolved_dim;
    }

    const auto* sparse_rows = static_cast<const SparseRow*>(dataset.GetTensor());
    for (int64_t row_idx = 0; row_idx < dataset.GetRows(); ++row_idx) {
        const auto& row = sparse_rows[row_idx];
        for (size_t elem_idx = 0; elem_idx < row.size(); ++elem_idx) {
            const auto elem_dim = static_cast<int64_t>(row[elem_idx].id) + 1;
            if (elem_dim > resolved_dim) {
                resolved_dim = elem_dim;
            }
        }
    }
    return resolved_dim;
}

expected<std::vector<float>>
DenseFromSparseDataset(const DataSet& dataset, int64_t fallback_dim = 0) {
    if (!dataset.IsSparse()) {
        return ErrorExpected<std::vector<float>>(Status::invalid_args,
                                                 "sparse rust node expects sparse dataset");
    }
    const auto resolved_dim = ResolveSparseDim(dataset, fallback_dim);
    if (dataset.GetRows() <= 0 || resolved_dim <= 0 || dataset.GetTensor() == nullptr) {
        return ErrorExpected<std::vector<float>>(Status::invalid_args,
                                                 "sparse dataset is invalid");
    }

    const auto rows = static_cast<size_t>(dataset.GetRows());
    const auto dim = static_cast<size_t>(resolved_dim);
    const auto* sparse_rows = static_cast<const SparseRow*>(dataset.GetTensor());

    std::vector<float> dense(rows * dim, 0.0f);
    for (size_t row_idx = 0; row_idx < rows; ++row_idx) {
        const auto& row = sparse_rows[row_idx];
        for (size_t elem_idx = 0; elem_idx < row.size(); ++elem_idx) {
            const auto& elem = row[elem_idx];
            if (elem.id >= dim) {
                return ErrorExpected<std::vector<float>>(Status::invalid_args,
                                                         "sparse row element exceeds dataset dim");
            }
            dense[row_idx * dim + static_cast<size_t>(elem.id)] = elem.val;
        }
    }
    return dense;
}

class SparseRustNode : public IndexNode {
 public:
    SparseRustNode(std::string index_name, CIndexType ffi_index_type)
        : index_name_(std::move(index_name)), ffi_index_type_(ffi_index_type) {
    }

    ~SparseRustNode() override {
        Reset();
    }

    Status
    Train(const DataSet& dataset, const Config& config) override {
        RETURN_IF_ERROR(EnsureIndex(dataset.GetDim(), config));
        auto dense = DenseFromSparseDataset(dataset);
        if (!dense.has_value()) {
            return dense.error();
        }
        return ToStatus(knowhere_train_index(handle_,
                                             dense.value().data(),
                                             static_cast<size_t>(dataset.GetRows()),
                                             static_cast<size_t>(dataset.GetDim())));
    }

    Status
    Add(const DataSet& dataset, const Config& config) override {
        RETURN_IF_ERROR(EnsureIndex(dataset.GetDim(), config));
        auto dense = DenseFromSparseDataset(dataset);
        if (!dense.has_value()) {
            return dense.error();
        }
        const auto status = ToStatus(knowhere_add_index(handle_,
                                                        dense.value().data(),
                                                        dataset.GetIds(),
                                                        static_cast<size_t>(dataset.GetRows()),
                                                        static_cast<size_t>(dataset.GetDim())));
        if (status != Status::success) {
            return status;
        }
        RefreshStats();
        return Status::success;
    }

    expected<DataSetPtr>
    Search(const DataSet& dataset,
           const Config& config,
           const BitsetView& bitset) const override {
        if (handle_ == nullptr) {
            return ErrorExpected<DataSetPtr>(Status::empty_index,
                                             "sparse inverted index is not initialized");
        }

        auto dense = DenseFromSparseDataset(dataset, dim_);
        if (!dense.has_value()) {
            return ErrorExpected<DataSetPtr>(dense.error(), dense.what());
        }
        const auto query_dim = ResolveSparseDim(dataset, dim_);

        const auto topk = GetOptionalSizeT(config, meta::TOPK).value_or(10);
        if (topk == 0) {
            return ErrorExpected<DataSetPtr>(Status::invalid_args,
                                             "topk must be positive for sparse search");
        }

        std::vector<uint64_t> bitset_words;
        CBitset cbitset{};
        CSearchResult* raw = nullptr;
        if (bitset.empty()) {
            raw = knowhere_search(handle_,
                                  dense.value().data(),
                                  static_cast<size_t>(dataset.GetRows()),
                                  topk,
                                  static_cast<size_t>(query_dim));
        } else {
            bitset_words.resize((bitset.size() + 63) / 64);
            std::memcpy(bitset_words.data(), bitset.data(), bitset.byte_size());
            cbitset.data = bitset_words.data();
            cbitset.len = bitset.size();
            cbitset.cap_words = bitset_words.size();
            raw = knowhere_search_with_bitset(handle_,
                                              dense.value().data(),
                                              static_cast<size_t>(dataset.GetRows()),
                                              topk,
                                              static_cast<size_t>(query_dim),
                                              &cbitset);
        }

        if (raw == nullptr) {
            return ErrorExpected<DataSetPtr>(Status::invalid_index_error,
                                             "rust ffi sparse search returned null");
        }

        std::unique_ptr<CSearchResult, void (*)(CSearchResult*)> result(
            raw, knowhere_free_result);
        return BuildSearchDataSet(dataset.GetRows(), topk, *result);
    }

    expected<DataSetPtr>
    RangeSearch(const DataSet&, const Config&, const BitsetView&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented,
                                         "sparse range search is out of stage 1 scope");
    }

    expected<DataSetPtr>
    GetVectorByIds(const DataSet&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented,
                                         "sparse get_vector_by_ids is out of stage 1 scope");
    }

    expected<std::vector<IteratorPtr>>
    AnnIterator(const DataSet&, const Config&, const BitsetView&) const override {
        return ErrorExpected<std::vector<IteratorPtr>>(
            Status::not_implemented, "sparse iterator is out of stage 1 scope");
    }

    bool
    HasRawData(const std::string&) const override {
        return handle_ != nullptr && knowhere_has_raw_data(handle_) == 1;
    }

    expected<DataSetPtr>
    GetIndexMeta(const Config&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented,
                                         "sparse index meta is out of stage 1 scope");
    }

    Status
    Serialize(BinarySet& binary_set) const override {
        if (handle_ == nullptr) {
            return Status::empty_index;
        }
        std::unique_ptr<CBinarySet, void (*)(CBinarySet*)> ffi_set(
            knowhere_serialize_index(handle_), knowhere_free_binary_set);
        if (!ffi_set || ffi_set->count == 0 || ffi_set->values == nullptr) {
            return Status::invalid_binary_set;
        }

        for (size_t i = 0; i < ffi_set->count; ++i) {
            const auto& binary = ffi_set->values[i];
            const auto* key = ffi_set->keys != nullptr && ffi_set->keys[i] != nullptr
                                  ? ffi_set->keys[i]
                                  : "index_data";
            auto owned = std::shared_ptr<uint8_t[]>(new uint8_t[binary.size]);
            std::memcpy(owned.get(), binary.data, static_cast<size_t>(binary.size));
            binary_set.Append(key, owned, binary.size);
        }
        return Status::success;
    }

    Status
    Deserialize(const BinarySet& binary_set, const Config& config) override {
        RETURN_IF_ERROR(EnsureIndex(dim_ > 0 ? dim_ : 0, config));

        auto binary = binary_set.GetByNames({"index_data"});
        if (binary == nullptr || binary->data == nullptr || binary->size <= 0) {
            return Status::invalid_binary_set;
        }

        CBinary ffi_binary{
            .data = binary->data.get(),
            .size = binary->size,
        };
        const char* key = "index_data";
        char* ffi_key = const_cast<char*>(key);
        CBinarySet ffi_set{
            .keys = &ffi_key,
            .values = &ffi_binary,
            .count = 1,
        };
        const auto status = ToStatus(knowhere_deserialize_index(handle_, &ffi_set));
        if (status == Status::success) {
            RefreshStats();
        }
        return status;
    }

    Status
    DeserializeFromFile(const std::string& filename, const Config& config) override {
        RETURN_IF_ERROR(EnsureIndex(dim_ > 0 ? dim_ : 0, config));
        const auto status = ToStatus(knowhere_load_index(handle_, filename.c_str()));
        if (status == Status::success) {
            RefreshStats();
        }
        return status;
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
        return size_;
    }

    int64_t
    Count() const override {
        return count_;
    }

    std::string
    Type() const override {
        return index_name_;
    }

 private:
    Status
    EnsureIndex(int64_t dataset_dim, const Config& config) {
        if (handle_ != nullptr) {
            return Status::success;
        }

        auto configured_dim =
            GetOptionalSizeT(config, meta::DIM).value_or(static_cast<size_t>(dataset_dim));
        // Sparse file-load in Milvus does not carry a schema dim in the load config.
        // The Rust index restores its real column count from persisted bytes, so a
        // non-zero bootstrap dim is sufficient to construct the FFI handle.
        if (configured_dim == 0) {
            configured_dim = 1;
        }

        metric_type_ = GetString(config, meta::METRIC_TYPE, metric::IP);
        if (metric_type_ != metric::IP) {
            return Status::invalid_args;
        }

        dim_ = static_cast<int64_t>(configured_dim);

        CIndexConfig ffi_config{};
        ffi_config.index_type = ffi_index_type_;
        ffi_config.metric_type = CMetricType::Ip;
        ffi_config.dim = configured_dim;
        ffi_config.data_type = 104;

        handle_ = knowhere_create_index(ffi_config);
        return handle_ == nullptr ? Status::invalid_args : Status::success;
    }

    expected<DataSetPtr>
    BuildSearchDataSet(int64_t nq, size_t topk, const CSearchResult& result) const {
        const auto expected_total = static_cast<size_t>(nq) * topk;
        if (result.ids == nullptr || result.distances == nullptr) {
            return ErrorExpected<DataSetPtr>(Status::invalid_index_error,
                                             "rust ffi sparse search returned null buffers");
        }

        auto* ids = new int64_t[expected_total];
        auto* distances = new float[expected_total];
        std::fill(ids, ids + expected_total, -1);
        std::fill(distances,
                  distances + expected_total,
                  -std::numeric_limits<float>::max());
        const auto available = std::min(result.num_results, expected_total);
        std::copy(result.ids, result.ids + available, ids);
        std::copy(result.distances, result.distances + available, distances);

        auto dataset_result = std::make_shared<DataSet>();
        dataset_result->SetRows(nq);
        dataset_result->SetDim(static_cast<int64_t>(topk));
        dataset_result->SetIds(ids);
        dataset_result->SetDistance(distances);
        dataset_result->SetIsOwner(true);
        return dataset_result;
    }

    void
    RefreshStats() {
        count_ = static_cast<int64_t>(knowhere_get_index_count(handle_));
        dim_ = static_cast<int64_t>(knowhere_get_index_dim(handle_));
        size_ = static_cast<int64_t>(knowhere_get_index_size(handle_));
    }

    void
    Reset() {
        if (handle_ != nullptr) {
            knowhere_free_index(handle_);
            handle_ = nullptr;
        }
        count_ = 0;
        dim_ = 0;
        size_ = 0;
        metric_type_.clear();
    }

    void* handle_ = nullptr;
    int64_t count_ = 0;
    int64_t dim_ = 0;
    int64_t size_ = 0;
    std::string metric_type_;
    std::string index_name_;
    CIndexType ffi_index_type_ = CIndexType::SparseInverted;
};

}  // namespace

std::shared_ptr<IndexNode>
MakeSparseRustNode(const std::string& name) {
    if (name == IndexEnum::INDEX_SPARSE_WAND) {
        return std::make_shared<SparseRustNode>(name, CIndexType::SparseWand);
    }
    return std::make_shared<SparseRustNode>(name, CIndexType::SparseInverted);
}

}  // namespace knowhere
