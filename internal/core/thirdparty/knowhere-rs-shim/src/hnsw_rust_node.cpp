#include "knowhere/index/index_factory.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cabi_bridge.hpp"
#include "knowhere/comp/brute_force.h"
#include "knowhere/config.h"
#include "status.hpp"

namespace knowhere {
namespace {

constexpr const char* kShimRawVectorsKey = "shim_raw_vectors";
constexpr const char* kShimRawIdsKey = "shim_raw_ids";
constexpr const char* kShimRawMetaKey = "shim_raw_meta";

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

        std::vector<float> normalized_vectors;
        const auto* ffi_vectors = MaybeNormalizeVectors(vectors,
                                                        static_cast<size_t>(dataset.GetRows()),
                                                        static_cast<size_t>(dataset.GetDim()),
                                                        normalized_vectors);

        return ToStatus(knowhere_train_index(handle_,
                                             ffi_vectors,
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

        std::vector<float> normalized_vectors;
        const auto* ffi_vectors = MaybeNormalizeVectors(vectors,
                                                        static_cast<size_t>(dataset.GetRows()),
                                                        static_cast<size_t>(dataset.GetDim()),
                                                        normalized_vectors);

        const int32_t code = knowhere_add_index(handle_,
                                                ffi_vectors,
                                                dataset.GetIds(),
                                                static_cast<size_t>(dataset.GetRows()),
                                                static_cast<size_t>(dataset.GetDim()));
        if (const auto status = ToStatus(code); status != Status::success) {
            return status;
        }

        RETURN_IF_ERROR(StoreRawDataset(dataset));
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

        std::fprintf(stderr,
                     "[knowhere-rs-shim][hnsw-validate] mode=search top_level_ef=%d "
                     "nested_params=%d config=%s\n",
                     config.contains(indexparam::EF) ? 1 : 0,
                     config.contains("params") ? 1 : 0,
                     config.dump().c_str());

        std::string validation_message;
        if (const auto status =
                ValidateHnswSearchConfig(config, validation_message);
            status != Status::success) {
            return ErrorExpected<DataSetPtr>(status, validation_message);
        }

        const auto config_metric = GetString(config, meta::METRIC_TYPE, "<missing>");

        const auto* query = static_cast<const float*>(dataset.GetTensor());
        const auto topk = GetOptionalSizeT(config, meta::TOPK).value_or(10);
        if (query == nullptr || dataset.GetRows() <= 0 || dataset.GetDim() <= 0 ||
            topk == 0) {
            return ErrorExpected<DataSetPtr>(Status::invalid_args,
                                             "invalid search dataset");
        }

        std::vector<float> normalized_query;
        const auto* ffi_query = MaybeNormalizeVectors(query,
                                                      static_cast<size_t>(dataset.GetRows()),
                                                      static_cast<size_t>(dataset.GetDim()),
                                                      normalized_query);

        std::vector<uint64_t> bitset_words;
        CBitset cbitset{};
        CSearchResult* raw = nullptr;
        if (bitset.empty()) {
            raw = knowhere_search(handle_,
                                  ffi_query,
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
                                              ffi_query,
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

        std::fprintf(stderr,
                     "[knowhere-rs-shim][hnsw-search] config_metric=%s node_metric=%s "
                     "topk=%zu nq=%lld dim=%lld raw_rows=%lld results=%zu bitset=%zu\n",
                     config_metric.c_str(),
                     metric_type_.c_str(),
                     topk,
                     static_cast<long long>(dataset.GetRows()),
                     static_cast<long long>(dataset.GetDim()),
                     static_cast<long long>(raw_rows_),
                     static_cast<size_t>(result->num_results),
                     static_cast<size_t>(bitset.size()));

        if (HasRawDataset() &&
            SearchResultIsShort(*result, dataset.GetRows(), topk, bitset)) {
            return FallbackSearch(dataset, config, bitset);
        }

        return BuildSearchDataSet(dataset.GetRows(), topk, *result);
    }

    expected<std::vector<IteratorPtr>>
    AnnIterator(const DataSet& dataset,
                const Config& config,
                const BitsetView& bitset) const override {
        std::fprintf(stderr,
                     "[knowhere-rs-shim][hnsw-validate] mode=iterator top_level_ef=%d "
                     "nested_params=%d config=%s\n",
                     config.contains(indexparam::EF) ? 1 : 0,
                     config.contains("params") ? 1 : 0,
                     config.dump().c_str());

        std::string validation_message;
        if (const auto status =
                ValidateHnswSearchConfig(config, validation_message);
            status != Status::success) {
            return ErrorExpected<std::vector<IteratorPtr>>(status,
                                                           validation_message);
        }

        if (HasRawDataset()) {
            return FallbackIterator(dataset, config, bitset);
        }

        if (handle_ == nullptr) {
            return ErrorExpected<std::vector<IteratorPtr>>(
                Status::empty_index, "hnsw index is not initialized");
        }
        if (count_ <= 0) {
            return ErrorExpected<std::vector<IteratorPtr>>(
                Status::empty_index, "hnsw index has no indexed vectors");
        }

        Config iterator_config = config;
        iterator_config[meta::TOPK] = count_;
        auto result = Search(dataset, iterator_config, bitset);
        if (!result.has_value()) {
            return ErrorExpected<std::vector<IteratorPtr>>(result.error(),
                                                           result.what());
        }
        return detail::BuildIteratorsFromSearchResult(result.value());
    }

    expected<DataSetPtr>
    RangeSearch(const DataSet&, const Config&, const BitsetView&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented,
                                         "range search is out of stage 1 scope");
    }

    expected<DataSetPtr>
    GetVectorByIds(const DataSet& dataset) const override {
        if (!HasRawDataset()) {
            return ErrorExpected<DataSetPtr>(
                Status::empty_index, "raw vectors are not available for hnsw index");
        }

        const auto count = dataset.GetRows();
        const auto* requested_ids = dataset.GetIds();
        if (count < 0 || requested_ids == nullptr) {
            return ErrorExpected<DataSetPtr>(Status::invalid_args,
                                             "vector ids dataset is invalid");
        }

        std::fprintf(stderr,
                     "[knowhere-rs-shim][get-vector] node=hnsw index=%s rows=%lld "
                     "dim=%lld req=%lld\n",
                     Type().c_str(),
                     static_cast<long long>(raw_rows_),
                     static_cast<long long>(dim_),
                     static_cast<long long>(count));

        auto result = std::make_shared<DataSet>();
        result->SetRows(count);
        result->SetDim(dim_);
        if (count == 0) {
            result->SetIsOwner(true);
            return result;
        }

        const auto row_bytes =
            static_cast<size_t>(dim_) * sizeof(raw_vectors_.front());
        const auto total_bytes = static_cast<size_t>(count) * row_bytes;
        auto* copied_ids = new int64_t[static_cast<size_t>(count)];
        auto* copied_tensor = new char[total_bytes];

        for (int64_t i = 0; i < count; ++i) {
            const auto it =
                std::find(raw_ids_.begin(), raw_ids_.end(), requested_ids[i]);
            if (it == raw_ids_.end()) {
                delete[] copied_ids;
                delete[] copied_tensor;
                return ErrorExpected<DataSetPtr>(
                    Status::invalid_args,
                    "requested id is not available in hnsw raw dataset");
            }
            const auto row_offset =
                static_cast<size_t>(std::distance(raw_ids_.begin(), it));
            std::memcpy(copied_tensor + static_cast<size_t>(i) * row_bytes,
                        raw_vectors_.data() + row_offset * static_cast<size_t>(dim_),
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
        return HasRawDataset() ||
               (handle_ != nullptr && knowhere_has_raw_data(handle_) == 1);
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

        if (HasRawDataset()) {
            const auto raw_bytes =
                static_cast<size_t>(raw_rows_) * static_cast<size_t>(dim_) * sizeof(float);
            auto raw_owned = std::shared_ptr<uint8_t[]>(new uint8_t[raw_bytes]);
            std::memcpy(raw_owned.get(), raw_vectors_.data(), raw_bytes);
            binary_set.Append(
                kShimRawVectorsKey, std::move(raw_owned), static_cast<int64_t>(raw_bytes));

            const auto ids_bytes = raw_ids_.size() * sizeof(int64_t);
            auto ids_owned = std::shared_ptr<uint8_t[]>(new uint8_t[ids_bytes]);
            std::memcpy(ids_owned.get(), raw_ids_.data(), ids_bytes);
            binary_set.Append(
                kShimRawIdsKey, std::move(ids_owned), static_cast<int64_t>(ids_bytes));

            Json meta_json = {
                {"rows", raw_rows_},
                {"dim", dim_},
                {"metric_type", metric_type_},
            };
            const auto meta_text = meta_json.dump();
            auto meta_owned = std::shared_ptr<uint8_t[]>(new uint8_t[meta_text.size()]);
            std::memcpy(meta_owned.get(), meta_text.data(), meta_text.size());
            binary_set.Append(
                kShimRawMetaKey,
                std::move(meta_owned),
                static_cast<int64_t>(meta_text.size()));
        }
        return Status::success;
    }

    Status
    Deserialize(const BinarySet& binary_set, const Config& config) override {
        Config deserialize_config = config;
        RETURN_IF_ERROR(MergeSerializedMetaIntoConfig(binary_set, deserialize_config));
        RETURN_IF_ERROR(EnsureIndex(dim_ > 0 ? dim_ : 0, deserialize_config));

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
            RETURN_IF_ERROR(RestoreRawDataset(binary_set));
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

        std::fprintf(stderr,
                     "[knowhere-rs-shim][hnsw-ensure] config_metric=%s "
                     "resolved_metric=%s dim=%zu existing_handle=%d\n",
                     GetString(config, meta::METRIC_TYPE, "<missing>").c_str(),
                     metric_type_.c_str(),
                     configured_dim,
                     handle_ != nullptr ? 1 : 0);

        CIndexConfig ffi_config{};
        ffi_config.index_type = CIndexType::Hnsw;
        ffi_config.metric_type = MetricToCMetric(metric_type_);
        ffi_config.dim = configured_dim;
        ffi_config.ef_construction =
            GetOptionalSizeT(config, indexparam::EFCONSTRUCTION).value_or(200);
        ffi_config.ef_search = GetOptionalSizeT(config, indexparam::EF).value_or(64);
        ffi_config.data_type = 101;

        handle_ = knowhere_create_index(ffi_config);
        return handle_ == nullptr ? Status::invalid_args : Status::success;
    }

    expected<DataSetPtr>
    BuildSearchDataSet(int64_t nq, size_t topk, const CSearchResult& result) const {
        const auto expected_total = static_cast<size_t>(nq) * topk;
        if (result.ids == nullptr || result.distances == nullptr ||
            result.num_results < expected_total) {
            return ErrorExpected<DataSetPtr>(Status::invalid_index_error,
                                             "rust ffi search returned short buffers");
        }

        auto* ids = new int64_t[expected_total];
        auto* distances = new float[expected_total];
        std::copy(result.ids, result.ids + expected_total, ids);
        std::copy(result.distances, result.distances + expected_total, distances);
        if (metric_type_ == metric::COSINE) {
            for (size_t idx = 0; idx < expected_total; ++idx) {
                distances[idx] = 1.0f - distances[idx];
            }
        }

        auto dataset_result = std::make_shared<DataSet>();
        dataset_result->SetRows(nq);
        dataset_result->SetDim(static_cast<int64_t>(topk));
        dataset_result->SetIds(ids);
        dataset_result->SetDistance(distances);
        dataset_result->SetIsOwner(true);
        return dataset_result;
    }

    Status
    StoreRawDataset(const DataSet& dataset) {
        const auto* vectors = static_cast<const float*>(dataset.GetTensor());
        if (vectors == nullptr || dataset.GetRows() <= 0 || dataset.GetDim() <= 0) {
            return Status::invalid_args;
        }
        if (raw_rows_ > 0 && dataset.GetDim() != dim_) {
            return Status::invalid_args;
        }

        const auto rows = dataset.GetRows();
        const auto old_rows = raw_rows_;
        const auto old_count = static_cast<size_t>(old_rows) * static_cast<size_t>(dim_);
        const auto append_count =
            static_cast<size_t>(rows) * static_cast<size_t>(dataset.GetDim());
        raw_vectors_.resize(old_count + append_count);
        std::copy(vectors,
                  vectors + append_count,
                  raw_vectors_.begin() + static_cast<std::ptrdiff_t>(old_count));

        const auto* dataset_ids = dataset.GetIds();
        raw_ids_.reserve(static_cast<size_t>(old_rows + rows));
        if (dataset_ids != nullptr) {
            raw_ids_.insert(raw_ids_.end(), dataset_ids, dataset_ids + rows);
        } else {
            int64_t next_id = dataset.GetTensorBeginId();
            if (old_rows > 0 && next_id == 0 && !raw_ids_.empty()) {
                next_id = raw_ids_.back() + 1;
            }
            for (int64_t offset = 0; offset < rows; ++offset) {
                raw_ids_.push_back(next_id + offset);
            }
        }

        raw_rows_ += rows;
        return Status::success;
    }

    bool
    HasRawDataset() const {
        return raw_rows_ > 0 && dim_ > 0 &&
               raw_vectors_.size() ==
                   static_cast<size_t>(raw_rows_) * static_cast<size_t>(dim_) &&
               raw_ids_.size() == static_cast<size_t>(raw_rows_);
    }

    size_t
    CountAvailable(const BitsetView& bitset) const {
        if (!HasRawDataset()) {
            return 0;
        }
        if (bitset.empty()) {
            return static_cast<size_t>(raw_rows_);
        }

        size_t available = 0;
        for (int64_t idx = 0; idx < raw_rows_; ++idx) {
            if (idx >= static_cast<int64_t>(bitset.size()) || !bitset.test(idx)) {
                ++available;
            }
        }
        return available;
    }

    bool
    SearchResultIsShort(const CSearchResult& result,
                        int64_t nq,
                        size_t topk,
                        const BitsetView& bitset) const {
        if (result.ids == nullptr || nq <= 0 || topk == 0) {
            return true;
        }

        const auto expected_per_query = std::min(topk, CountAvailable(bitset));
        for (int64_t query_idx = 0; query_idx < nq; ++query_idx) {
            size_t valid = 0;
            const auto offset = static_cast<size_t>(query_idx) * topk;
            for (size_t rank = 0; rank < topk; ++rank) {
                const auto result_idx = offset + rank;
                if (result_idx >= result.num_results) {
                    break;
                }
                if (result.ids[result_idx] >= 0) {
                    ++valid;
                }
            }
            if (valid < expected_per_query) {
                return true;
            }
        }
        return false;
    }

    DataSetPtr
    MakeRawBaseDataset() const {
        if (!HasRawDataset()) {
            return nullptr;
        }

        auto base = GenDataSet(raw_rows_, dim_, raw_vectors_.data());
        base->SetIds(raw_ids_.data());
        base->SetTensorBeginId(raw_ids_.empty() ? 0 : raw_ids_.front());
        return base;
    }

    DataSetPtr
    MakeQueryDataset(const DataSet& dataset) const {
        auto query = GenDataSet(dataset.GetRows(), dataset.GetDim(), dataset.GetTensor());
        query->SetTensorBeginId(dataset.GetTensorBeginId());
        if (dataset.GetIds() != nullptr) {
            query->SetIds(dataset.GetIds());
        }
        return query;
    }

    const float*
    MaybeNormalizeVectors(const float* vectors,
                          size_t rows,
                          size_t dim,
                          std::vector<float>& normalized) const {
        if (metric_type_ != metric::COSINE || vectors == nullptr || rows == 0 || dim == 0) {
            return vectors;
        }

        normalized.resize(rows * dim);
        for (size_t row = 0; row < rows; ++row) {
            const auto* source = vectors + row * dim;
            auto* dest = normalized.data() + row * dim;
            float norm_sq = 0.0f;
            for (size_t col = 0; col < dim; ++col) {
                norm_sq += source[col] * source[col];
            }
            const auto norm = std::sqrt(norm_sq);
            if (norm > 0.0f) {
                const auto inv_norm = 1.0f / norm;
                for (size_t col = 0; col < dim; ++col) {
                    dest[col] = source[col] * inv_norm;
                }
            } else {
                std::fill(dest, dest + dim, 0.0f);
            }
        }
        return normalized.data();
    }

    expected<DataSetPtr>
    FallbackSearch(const DataSet& dataset,
                   const Config& config,
                   const BitsetView& bitset) const {
        auto base = MakeRawBaseDataset();
        auto query = MakeQueryDataset(dataset);
        if (base == nullptr || query == nullptr) {
            return ErrorExpected<DataSetPtr>(Status::empty_index,
                                             "raw fallback data is not available");
        }
        return BruteForce::Search<float>(base, query, config, bitset);
    }

    expected<std::vector<IteratorPtr>>
    FallbackIterator(const DataSet& dataset,
                     const Config& config,
                     const BitsetView& bitset) const {
        auto base = MakeRawBaseDataset();
        auto query = MakeQueryDataset(dataset);
        if (base == nullptr || query == nullptr) {
            return ErrorExpected<std::vector<IteratorPtr>>(
                Status::empty_index, "raw fallback data is not available");
        }
        return BruteForce::AnnIterator<float>(base, query, config, bitset);
    }

    Status
    RestoreRawDataset(const BinarySet& binary_set) {
        raw_vectors_.clear();
        raw_ids_.clear();
        raw_rows_ = 0;

        auto meta_json = LoadSerializedMeta(binary_set);
        if (!meta_json.has_value()) {
            return Status::success;
        }

        auto raw = binary_set.GetByName(kShimRawVectorsKey);
        auto ids = binary_set.GetByName(kShimRawIdsKey);
        if (raw == nullptr || ids == nullptr || raw->data == nullptr || ids->data == nullptr) {
            return Status::invalid_binary_set;
        }

        raw_rows_ = meta_json->value("rows", int64_t{0});
        const auto serialized_dim = meta_json->value("dim", int64_t{0});
        if (raw_rows_ <= 0 || serialized_dim <= 0 || serialized_dim != dim_) {
            return Status::invalid_binary_set;
        }

        const auto vector_count = static_cast<size_t>(raw_rows_) * static_cast<size_t>(dim_);
        if (raw->size != static_cast<int64_t>(vector_count * sizeof(float)) ||
            ids->size != static_cast<int64_t>(static_cast<size_t>(raw_rows_) * sizeof(int64_t))) {
            return Status::invalid_binary_set;
        }

        raw_vectors_.resize(vector_count);
        std::memcpy(raw_vectors_.data(), raw->data.get(), static_cast<size_t>(raw->size));
        raw_ids_.resize(static_cast<size_t>(raw_rows_));
        std::memcpy(raw_ids_.data(), ids->data.get(), static_cast<size_t>(ids->size));
        return Status::success;
    }

    std::optional<Json>
    LoadSerializedMeta(const BinarySet& binary_set) const {
        auto meta = binary_set.GetByName(kShimRawMetaKey);
        if (meta == nullptr || meta->data == nullptr || meta->size <= 0) {
            return std::nullopt;
        }

        const auto meta_text = std::string(
            reinterpret_cast<const char*>(meta->data.get()),
            static_cast<size_t>(meta->size));
        const auto meta_json = Json::parse(meta_text, nullptr, false);
        if (meta_json.is_discarded()) {
            return std::nullopt;
        }
        return meta_json;
    }

    Status
    MergeSerializedMetaIntoConfig(const BinarySet& binary_set, Config& config) const {
        auto meta_json = LoadSerializedMeta(binary_set);
        if (!meta_json.has_value()) {
            return Status::success;
        }

        if (!config.contains(meta::DIM)) {
            const auto serialized_dim = meta_json->value("dim", int64_t{0});
            if (serialized_dim <= 0) {
                return Status::invalid_binary_set;
            }
            config[meta::DIM] = serialized_dim;
        }

        if (!config.contains(meta::METRIC_TYPE)) {
            const auto serialized_metric =
                meta_json->value("metric_type", std::string{});
            if (serialized_metric.empty()) {
                return Status::invalid_binary_set;
            }
            config[meta::METRIC_TYPE] = serialized_metric;
        }

        return Status::success;
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
        raw_vectors_.clear();
        raw_ids_.clear();
        raw_rows_ = 0;
    }

    void* handle_ = nullptr;
    int64_t dim_ = 0;
    int64_t size_ = 0;
    int64_t count_ = 0;
    int64_t raw_rows_ = 0;
    std::string metric_type_ = metric::L2;
    std::vector<float> raw_vectors_;
    std::vector<int64_t> raw_ids_;
};

}  // namespace

std::shared_ptr<IndexNode>
MakeHnswRustNode() {
    return std::make_shared<HnswRustNode>();
}

}  // namespace knowhere
