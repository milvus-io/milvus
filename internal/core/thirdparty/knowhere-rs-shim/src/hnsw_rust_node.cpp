#include "knowhere/index/index_factory.h"

#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cabi_bridge.hpp"
#include "status.hpp"

namespace knowhere {
namespace {

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
        auto v = value.get<int64_t>();
        if (v >= 0) {
            return static_cast<size_t>(v);
        }
        return std::nullopt;
    }
    if (value.is_string()) {
        const auto& str = value.get_ref<const std::string&>();
        if (str.empty()) {
            return std::nullopt;
        }
        return static_cast<size_t>(std::stoull(str));
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

CMetricType
MetricToCMetric(const std::string& metric_type) {
    if (metric_type == metric::IP) {
        return CMetricType::Ip;
    }
    if (metric_type == metric::COSINE) {
        return CMetricType::Cosine;
    }
    return CMetricType::L2;
}

class HnswRustNode : public IndexNode {
 public:
    HnswRustNode() = default;

    ~HnswRustNode() override {
        Reset();
    }

    Status
    Train(const DataSet& dataset, const Config& config) override {
        RETURN_IF_ERROR(EnsureIndex(dataset.GetDim(), config));

        const auto* vectors = static_cast<const float*>(dataset.GetTensor());
        if (vectors == nullptr || dataset.GetRows() <= 0 || dataset.GetDim() <= 0) {
            return Status::invalid_args;
        }

        return ToStatus(knowhere_train_index(handle_,
                                             vectors,
                                             static_cast<size_t>(dataset.GetRows()),
                                             static_cast<size_t>(dataset.GetDim())));
    }

    Status
    Add(const DataSet& dataset, const Config& config) override {
        RETURN_IF_ERROR(EnsureIndex(dataset.GetDim(), config));

        const auto* vectors = static_cast<const float*>(dataset.GetTensor());
        if (vectors == nullptr || dataset.GetRows() <= 0 || dataset.GetDim() <= 0) {
            return Status::invalid_args;
        }

        const int32_t code = knowhere_add_index(handle_,
                                                vectors,
                                                dataset.GetIds(),
                                                static_cast<size_t>(dataset.GetRows()),
                                                static_cast<size_t>(dataset.GetDim()));
        if (const auto status = ToStatus(code); status != Status::success) {
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
                                             "hnsw index is not initialized");
        }

        const auto* query = static_cast<const float*>(dataset.GetTensor());
        const auto topk = GetOptionalSizeT(config, meta::TOPK).value_or(10);
        if (query == nullptr || dataset.GetRows() <= 0 || dataset.GetDim() <= 0 ||
            topk == 0) {
            return ErrorExpected<DataSetPtr>(Status::invalid_args,
                                             "invalid search dataset");
        }

        std::vector<uint64_t> bitset_words;
        CBitset cbitset{};
        CSearchResult* raw = nullptr;
        if (bitset.empty()) {
            raw = knowhere_search(handle_,
                                  query,
                                  static_cast<size_t>(dataset.GetRows()),
                                  topk,
                                  static_cast<size_t>(dataset.GetDim()));
        } else {
            bitset_words.resize((bitset.size() + 63) / 64);
            std::memcpy(bitset_words.data(), bitset.data(), bitset.byte_size());
            cbitset.data = bitset_words.data();
            cbitset.len = bitset.size();
            cbitset.cap_words = bitset_words.size();
            raw = knowhere_search_with_bitset(handle_,
                                              query,
                                              static_cast<size_t>(dataset.GetRows()),
                                              topk,
                                              static_cast<size_t>(dataset.GetDim()),
                                              &cbitset);
        }

        if (raw == nullptr) {
            return ErrorExpected<DataSetPtr>(Status::invalid_index_error,
                                             "rust ffi search returned null");
        }

        std::unique_ptr<CSearchResult, void (*)(CSearchResult*)> result(
            raw, knowhere_free_result);

        const auto total = result->num_results;
        auto* ids = new int64_t[total];
        auto* distances = new float[total];
        std::copy(result->ids, result->ids + total, ids);
        std::copy(result->distances, result->distances + total, distances);

        auto dataset_result = std::make_shared<DataSet>();
        dataset_result->SetRows(dataset.GetRows());
        dataset_result->SetDim(static_cast<int64_t>(topk));
        dataset_result->SetIds(ids);
        dataset_result->SetDistance(distances);
        dataset_result->SetIsOwner(true);
        return dataset_result;
    }

    expected<DataSetPtr>
    RangeSearch(const DataSet&, const Config&, const BitsetView&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented,
                                         "range search is out of stage 1 scope");
    }

    expected<DataSetPtr>
    GetVectorByIds(const DataSet&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented,
                                         "get_vector_by_ids is out of stage 1 scope");
    }

    bool
    HasRawData(const std::string&) const override {
        return handle_ != nullptr && knowhere_has_raw_data(handle_) == 1;
    }

    expected<DataSetPtr>
    GetIndexMeta(const Config&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented,
                                         "index meta is out of stage 1 scope");
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
        return IndexEnum::INDEX_HNSW;
    }

 private:
    Status
    EnsureIndex(int64_t dataset_dim, const Config& config) {
        if (handle_ != nullptr) {
            return Status::success;
        }

        const auto configured_dim =
            GetOptionalSizeT(config, meta::DIM).value_or(static_cast<size_t>(dataset_dim));
        if (configured_dim == 0) {
            return Status::invalid_args;
        }

        metric_type_ = GetString(config, meta::METRIC_TYPE, metric::L2);
        dim_ = static_cast<int64_t>(configured_dim);

        CIndexConfig ffi_config{};
        ffi_config.index_type = CIndexType::Hnsw;
        ffi_config.metric_type = MetricToCMetric(metric_type_);
        ffi_config.dim = configured_dim;
        ffi_config.ef_construction =
            GetOptionalSizeT(config, indexparam::EFCONSTRUCTION).value_or(200);
        ffi_config.ef_search =
            GetOptionalSizeT(config, indexparam::EF).value_or(64);
        ffi_config.data_type = 101;

        handle_ = knowhere_create_index(ffi_config);
        return handle_ == nullptr ? Status::invalid_args : Status::success;
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
    }

    void* handle_ = nullptr;
    int64_t dim_ = 0;
    int64_t size_ = 0;
    int64_t count_ = 0;
    std::string metric_type_ = metric::L2;
};

}  // namespace

std::shared_ptr<IndexNode>
MakeHnswRustNode() {
    return std::make_shared<HnswRustNode>();
}

}  // namespace knowhere
