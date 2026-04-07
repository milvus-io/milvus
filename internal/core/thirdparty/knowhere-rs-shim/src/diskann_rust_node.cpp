#include "knowhere/index/index_factory.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <sys/stat.h>
#include <utility>
#include <vector>

#include "cabi_bridge.hpp"
#include "knowhere/config.h"
#include "status.hpp"

namespace knowhere {
namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

std::optional<size_t>
GetSizeT(const Config& config, const char* key) {
    if (!config.contains(key)) {
        return std::nullopt;
    }
    const auto& v = config.at(key);
    if (v.is_number_unsigned()) return v.get<size_t>();
    if (v.is_number_integer()) {
        const auto i = v.get<int64_t>();
        if (i >= 0) return static_cast<size_t>(i);
        return std::nullopt;
    }
    if (v.is_string()) {
        const auto& s = v.get_ref<const std::string&>();
        if (!s.empty()) return static_cast<size_t>(std::stoull(s));
    }
    return std::nullopt;
}

std::string
GetStr(const Config& config, const char* key, std::string fallback) {
    if (!config.contains(key)) return fallback;
    const auto& v = config.at(key);
    if (v.is_string()) return v.get<std::string>();
    return fallback;
}

CMetricType
ToCMetric(const std::string& m) {
    if (m == metric::IP) return CMetricType::Ip;
    if (m == metric::COSINE) return CMetricType::Cosine;
    return CMetricType::L2;
}

// Create a unique temp directory; returns "" on failure.
static std::string
MakeTempDir() {
    char tmpl[] = "/tmp/diskann_rs_XXXXXX";
    if (mkdtemp(tmpl) == nullptr) return "";
    return std::string(tmpl);
}

// Remove a directory and all its contents.
static void
RemoveAll(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
}

// Pack every regular file inside dir_path into binary_set.
// Returns Status::success on success.
static Status
PackDirIntoBinarySet(const std::string& dir_path, BinarySet& binary_set) {
    std::error_code ec;
    for (auto& entry : std::filesystem::directory_iterator(dir_path, ec)) {
        if (!entry.is_regular_file()) continue;
        const auto& fpath = entry.path();
        std::ifstream ifs(fpath, std::ios::binary | std::ios::ate);
        if (!ifs) return Status::invalid_index_error;
        const auto size = ifs.tellg();
        if (size <= 0) continue;
        ifs.seekg(0);
        auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[static_cast<size_t>(size)]);
        if (!ifs.read(reinterpret_cast<char*>(buf.get()), size)) {
            return Status::invalid_index_error;
        }
        binary_set.Append(fpath.filename().string(), buf, size);
    }
    return ec ? Status::invalid_index_error : Status::success;
}

// Write each entry in binary_set as a file under dir_path.
static Status
UnpackBinarySetIntoDir(const BinarySet& binary_set, const std::string& dir_path) {
    for (const auto& kv : binary_set.binary_map_) {
        if (!kv.second || kv.second->size <= 0) continue;
        const auto fpath = dir_path + "/" + kv.first;
        std::ofstream ofs(fpath, std::ios::binary);
        if (!ofs) return Status::invalid_index_error;
        if (!ofs.write(reinterpret_cast<const char*>(kv.second->data.get()),
                       kv.second->size)) {
            return Status::invalid_index_error;
        }
    }
    return Status::success;
}

// ---------------------------------------------------------------------------
// DiskAnnRustNode
// ---------------------------------------------------------------------------

class DiskAnnRustNode : public IndexNode {
 public:
    DiskAnnRustNode() = default;

    ~DiskAnnRustNode() override {
        if (handle_ != nullptr) {
            knowhere_free_index(handle_);
        }
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
                                             "diskann index is not initialized");
        }

        const auto* query = static_cast<const float*>(dataset.GetTensor());
        const auto topk = GetSizeT(config, meta::TOPK).value_or(10);
        if (query == nullptr || dataset.GetRows() <= 0 || dataset.GetDim() <= 0 ||
            topk == 0) {
            return ErrorExpected<DataSetPtr>(Status::invalid_args,
                                             "invalid search dataset");
        }

        // Update search_list_size if provided.
        if (const auto sl = GetSearchListSize(config); sl.has_value()) {
            knowhere_set_ef_search(handle_, sl.value());
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
                                             "diskann rust ffi search returned null");
        }
        std::unique_ptr<CSearchResult, void (*)(CSearchResult*)> result(
            raw, knowhere_free_result);
        return BuildDataSet(dataset.GetRows(), topk, *result);
    }

    expected<std::vector<IteratorPtr>>
    AnnIterator(const DataSet&, const Config&, const BitsetView&) const override {
        return ErrorExpected<std::vector<IteratorPtr>>(
            Status::not_implemented, "DiskANN iterator not supported");
    }

    expected<DataSetPtr>
    RangeSearch(const DataSet&, const Config&, const BitsetView&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented,
                                         "DiskANN range search not supported");
    }

    expected<DataSetPtr>
    GetVectorByIds(const DataSet&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented,
                                         "DiskANN GetVectorByIds not supported");
    }

    bool
    HasRawData(const std::string&) const override {
        return false;
    }

    expected<DataSetPtr>
    GetIndexMeta(const Config&) const override {
        return ErrorExpected<DataSetPtr>(Status::not_implemented, "not supported");
    }

    // Serialize: save DiskANN directory to a temp path, pack files into BinarySet.
    Status
    Serialize(BinarySet& binary_set) const override {
        if (handle_ == nullptr) return Status::empty_index;

        const auto tmpdir = MakeTempDir();
        if (tmpdir.empty()) return Status::invalid_index_error;

        const int32_t rc = knowhere_save_index(handle_, tmpdir.c_str());
        if (rc != 0) {
            RemoveAll(tmpdir);
            return Status::invalid_index_error;
        }

        const auto status = PackDirIntoBinarySet(tmpdir, binary_set);
        RemoveAll(tmpdir);
        return status;
    }

    // Deserialize: write BinarySet entries to temp dir, load from there.
    Status
    Deserialize(const BinarySet& binary_set, const Config& config) override {
        RETURN_IF_ERROR(EnsureIndex(dim_ > 0 ? dim_ : 0, config));

        const auto tmpdir = MakeTempDir();
        if (tmpdir.empty()) return Status::invalid_index_error;

        const auto unpack_status = UnpackBinarySetIntoDir(binary_set, tmpdir);
        if (unpack_status != Status::success) {
            RemoveAll(tmpdir);
            return unpack_status;
        }

        const auto rc = ToStatus(knowhere_load_index(handle_, tmpdir.c_str()));
        RemoveAll(tmpdir);
        if (rc == Status::success) RefreshStats();
        return rc;
    }

    // DeserializeFromFile: the path is already a directory on local disk.
    Status
    DeserializeFromFile(const std::string& filename, const Config& config) override {
        RETURN_IF_ERROR(EnsureIndex(dim_ > 0 ? dim_ : 0, config));
        const auto rc = ToStatus(knowhere_load_index(handle_, filename.c_str()));
        if (rc == Status::success) RefreshStats();
        return rc;
    }

    std::unique_ptr<BaseConfig>
    CreateConfig() const override {
        return std::make_unique<BaseConfig>();
    }

    int64_t Dim() const override { return dim_; }
    int64_t Size() const override { return size_; }
    int64_t Count() const override { return count_; }
    std::string Type() const override { return IndexEnum::INDEX_DISKANN; }

 private:
    Status
    EnsureIndex(int64_t dataset_dim, const Config& config) {
        if (handle_ != nullptr) return Status::success;

        const auto configured_dim =
            GetSizeT(config, meta::DIM).value_or(static_cast<size_t>(dataset_dim));
        if (configured_dim == 0) return Status::invalid_args;

        metric_type_ = GetStr(config, meta::METRIC_TYPE, metric::IP);
        dim_ = static_cast<int64_t>(configured_dim);

        CIndexConfig ffi_config{};
        ffi_config.index_type = CIndexType::DiskAnn;
        ffi_config.metric_type = ToCMetric(metric_type_);
        ffi_config.dim = configured_dim;
        // max_degree maps to ef_construction in CIndexConfig
        ffi_config.ef_construction =
            GetSizeT(config, "max_degree").value_or(56);
        // search_list_size maps to ef_search in CIndexConfig
        ffi_config.ef_search =
            GetSizeT(config, "search_list_size").value_or(100);
        ffi_config.data_type = 101;

        handle_ = knowhere_create_index(ffi_config);
        return handle_ == nullptr ? Status::invalid_args : Status::success;
    }

    std::optional<size_t>
    GetSearchListSize(const Config& config) const {
        // search_list_size may appear directly or under search_params
        if (const auto v = GetSizeT(config, "search_list_size"); v.has_value()) {
            return v;
        }
        const auto* sp = FindSearchParamValue(config, "search_list_size");
        if (sp == nullptr) return std::nullopt;
        if (sp->is_number_unsigned()) return sp->get<size_t>();
        if (sp->is_number_integer()) {
            const auto i = sp->get<int64_t>();
            if (i > 0) return static_cast<size_t>(i);
        }
        return std::nullopt;
    }

    expected<DataSetPtr>
    BuildDataSet(int64_t nq, size_t topk, const CSearchResult& result) const {
        const auto expected_total = static_cast<size_t>(nq) * topk;
        if (result.ids == nullptr || result.distances == nullptr ||
            result.num_results < expected_total) {
            return ErrorExpected<DataSetPtr>(Status::invalid_index_error,
                                             "diskann search returned short buffers");
        }
        auto* ids = new int64_t[expected_total];
        auto* distances = new float[expected_total];
        std::copy(result.ids, result.ids + expected_total, ids);
        std::copy(result.distances, result.distances + expected_total, distances);

        auto ds = std::make_shared<DataSet>();
        ds->SetRows(nq);
        ds->SetDim(static_cast<int64_t>(topk));
        ds->SetIds(ids);
        ds->SetDistance(distances);
        ds->SetIsOwner(true);
        return ds;
    }

    void
    RefreshStats() {
        count_ = static_cast<int64_t>(knowhere_get_index_count(handle_));
        dim_   = static_cast<int64_t>(knowhere_get_index_dim(handle_));
        size_  = static_cast<int64_t>(knowhere_get_index_size(handle_));
    }

    void* handle_   = nullptr;
    int64_t dim_    = 0;
    int64_t size_   = 0;
    int64_t count_  = 0;
    std::string metric_type_ = metric::IP;
};

}  // namespace

std::shared_ptr<IndexNode>
MakeDiskAnnRustNode() {
    return std::make_shared<DiskAnnRustNode>();
}

}  // namespace knowhere
