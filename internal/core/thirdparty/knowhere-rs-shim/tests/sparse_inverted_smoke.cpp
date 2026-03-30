#include <filesystem>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <vector>

#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/version.h"

namespace {

using SparseValue = knowhere::sparse_u32_f32::ValueType;
using SparseRow = knowhere::sparse::SparseRow<SparseValue>;

SparseRow
MakeRow(std::initializer_list<std::pair<uint32_t, float>> values) {
    SparseRow row(values.size());
    size_t offset = 0;
    for (const auto& [dim, value] : values) {
        row[offset].id = dim;
        row[offset].val = value;
        ++offset;
    }
    return row;
}

knowhere::DataSetPtr
MakeSparseDataSet(std::vector<SparseRow>* rows, int64_t dim, const int64_t* ids = nullptr) {
    auto dataset = knowhere::GenDataSet(static_cast<int64_t>(rows->size()), dim, rows->data());
    dataset->SetIsSparse(true);
    if (ids != nullptr) {
        dataset->SetIds(ids);
    }
    return dataset;
}

bool
CheckTop1(const knowhere::expected<knowhere::DataSetPtr>& search, int64_t expected_id) {
    return search.has_value() && search.value() != nullptr &&
           search.value()->GetIds() != nullptr &&
           search.value()->GetDistance() != nullptr &&
           search.value()->GetIds()[0] == expected_id;
}

bool
CheckPaddedTopK(const knowhere::expected<knowhere::DataSetPtr>& search,
                int64_t expected_id,
                int64_t expected_topk) {
    return CheckTop1(search, expected_id) && search.value()->GetDim() == expected_topk &&
           search.value()->GetIds()[2] == -1;
}

void
PrintSearchDebug(const char* label,
                 const knowhere::expected<knowhere::DataSetPtr>& search) {
    std::cerr << label;
    if (!search.has_value()) {
        std::cerr << " status=" << knowhere::Status2String(search.error())
                  << " message=" << search.what() << "\n";
        return;
    }
    if (search.value() == nullptr) {
        std::cerr << " result=null\n";
        return;
    }
    const auto* ids = search.value()->GetIds();
    const auto* distances = search.value()->GetDistance();
    if (ids == nullptr || distances == nullptr) {
        std::cerr << " ids_or_distances_missing\n";
        return;
    }
    std::cerr << " top1_id=" << ids[0] << " top1_distance=" << distances[0]
              << " rows=" << search.value()->GetRows()
              << " topk=" << search.value()->GetDim() << "\n";
}

}  // namespace

int
main() {
    std::vector<SparseRow> base_rows;
    base_rows.emplace_back(MakeRow({{1, 1.0f}, {4, 0.5f}}));
    base_rows.emplace_back(MakeRow({{2, 1.0f}, {5, 0.5f}}));
    base_rows.emplace_back(MakeRow({{1, 0.2f}, {3, 0.8f}}));
    const std::vector<int64_t> base_ids = {101, 102, 103};

    std::vector<SparseRow> query_rows;
    query_rows.emplace_back(MakeRow({{1, 1.0f}, {4, 0.5f}}));

    knowhere::Config build_cfg;
    build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::IP;
    build_cfg[knowhere::meta::DIM] = 8;

    knowhere::Config search_cfg;
    search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::IP;
    search_cfg[knowhere::meta::TOPK] = 2;

    knowhere::Config padded_search_cfg = search_cfg;
    padded_search_cfg[knowhere::meta::TOPK] = 4;

    auto base_ds = MakeSparseDataSet(&base_rows, 8, base_ids.data());
    auto query_ds = MakeSparseDataSet(&query_rows, 8);
    auto dimless_query_ds = MakeSparseDataSet(&query_rows, 0);
    const uint8_t zero_bitset_bytes[] = {0x00};
    auto zero_bitset = knowhere::BitsetView(zero_bitset_bytes, base_rows.size());

    auto created = knowhere::IndexFactory::Instance().Create<knowhere::sparse_u32_f32>(
        knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create sparse inverted index\n";
        return 1;
    }

    auto index = created.value();
    const auto build_status = index.Build(*base_ds, build_cfg);
    if (build_status != knowhere::Status::success) {
        std::cerr << "failed to build sparse inverted index: "
                  << knowhere::Status2String(build_status) << "\n";
        return 2;
    }

    auto search = index.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!search.has_value() || search.value() == nullptr ||
        search.value()->GetIds() == nullptr || search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search sparse inverted index: status="
                  << (search.has_value() ? "success"
                                         : knowhere::Status2String(search.error()))
                  << " message=" << (search.has_value() ? "" : search.what()) << "\n";
        return 3;
    }

    if (search.value()->GetIds()[0] != 101) {
        std::cerr << "unexpected top1 id: " << search.value()->GetIds()[0] << "\n";
        return 4;
    }

    auto dimless_search =
        index.Search(*dimless_query_ds, search_cfg, knowhere::BitsetView{});
    if (!CheckTop1(dimless_search, 101)) {
        PrintSearchDebug("dimless sparse search mismatch:", dimless_search);
        return 5;
    }

    auto dimless_bitset_search =
        index.Search(*dimless_query_ds, search_cfg, zero_bitset);
    if (!CheckTop1(dimless_bitset_search, 101)) {
        PrintSearchDebug("dimless sparse bitset search mismatch:",
                         dimless_bitset_search);
        return 6;
    }

    auto padded_search =
        index.Search(*dimless_query_ds, padded_search_cfg, zero_bitset);
    if (!CheckPaddedTopK(padded_search, 101, 4)) {
        PrintSearchDebug("dimless sparse padded search mismatch:", padded_search);
        return 7;
    }

    std::vector<SparseRow> high_dim_base_rows;
    high_dim_base_rows.emplace_back(MakeRow({{129, 1.0f}, {131, 0.5f}}));
    high_dim_base_rows.emplace_back(MakeRow({{129, 0.1f}, {141, 0.2f}}));
    const std::vector<int64_t> high_dim_ids = {201, 202};
    auto high_dim_base_ds =
        MakeSparseDataSet(&high_dim_base_rows, 256, high_dim_ids.data());

    std::vector<SparseRow> underspecified_query_rows;
    underspecified_query_rows.emplace_back(MakeRow({{129, 1.0f}, {131, 0.5f}}));
    auto underspecified_query_ds = MakeSparseDataSet(&underspecified_query_rows, 128);

    auto high_dim_created =
        knowhere::IndexFactory::Instance().Create<knowhere::sparse_u32_f32>(
            knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
            knowhere::Version::GetCurrentVersion());
    if (!high_dim_created.has_value()) {
        std::cerr << "failed to create high-dim sparse inverted index\n";
        return 8;
    }

    auto high_dim_index = high_dim_created.value();
    knowhere::Config high_dim_build_cfg;
    high_dim_build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::IP;
    high_dim_build_cfg[knowhere::meta::DIM] = 256;
    const auto high_dim_status =
        high_dim_index.Build(*high_dim_base_ds, high_dim_build_cfg);
    if (high_dim_status != knowhere::Status::success) {
        std::cerr << "failed to build high-dim sparse inverted index: "
                  << knowhere::Status2String(high_dim_status) << "\n";
        return 9;
    }

    auto underspecified_search = high_dim_index.Search(
        *underspecified_query_ds, search_cfg, knowhere::BitsetView{});
    if (!CheckTop1(underspecified_search, 201)) {
        PrintSearchDebug("underspecified sparse query dim mismatch:",
                         underspecified_search);
        return 10;
    }

    knowhere::BinarySet binary_set;
    if (index.Serialize(binary_set) != knowhere::Status::success ||
        !binary_set.Contains("index_data")) {
        std::cerr << "failed to serialize sparse inverted index\n";
        return 11;
    }

    auto restored_created = knowhere::IndexFactory::Instance().Create<knowhere::sparse_u32_f32>(
        knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
        knowhere::Version::GetCurrentVersion());
    if (!restored_created.has_value()) {
        std::cerr << "failed to create restored sparse inverted index\n";
        return 12;
    }

    auto restored = restored_created.value();
    if (restored.Deserialize(binary_set, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to deserialize sparse inverted index from binary set\n";
        return 13;
    }
    auto restored_search = restored.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!CheckTop1(restored_search, 101)) {
        std::cerr << "unexpected binary-set restored sparse top1\n";
        return 14;
    }

    const auto binary = binary_set.GetByName("index_data");
    if (binary == nullptr || binary->data == nullptr || binary->size <= 0) {
        std::cerr << "serialized sparse binary set missing index_data payload\n";
        return 15;
    }

    const auto temp_path = std::filesystem::temp_directory_path() /
                           ("knowhere_rs_sparse_inverted_roundtrip_" +
                            std::to_string(::getpid()) + ".bin");
    {
        std::ofstream out(temp_path, std::ios::binary | std::ios::trunc);
        if (!out.good()) {
            std::cerr << "failed to open sparse roundtrip temp file\n";
            return 16;
        }
        out.write(reinterpret_cast<const char*>(binary->data.get()), binary->size);
        if (!out.good()) {
            std::cerr << "failed to write sparse roundtrip temp file\n";
            return 17;
        }
    }

    knowhere::Config load_cfg;
    load_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::IP;

    auto file_restored_created =
        knowhere::IndexFactory::Instance().Create<knowhere::sparse_u32_f32>(
            knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
            knowhere::Version::GetCurrentVersion());
    if (!file_restored_created.has_value()) {
        std::cerr << "failed to create file-restored sparse inverted index\n";
        return 18;
    }

    auto file_restored = file_restored_created.value();
    const auto file_status =
        file_restored.DeserializeFromFile(temp_path.string(), load_cfg);
    std::filesystem::remove(temp_path);
    if (file_status != knowhere::Status::success) {
        std::cerr << "failed to load sparse inverted index from file: "
                  << knowhere::Status2String(file_status) << "\n";
        return 19;
    }

    auto file_search =
        file_restored.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!CheckTop1(file_search, 101)) {
        std::cerr << "unexpected file-restored sparse top1\n";
        return 20;
    }

    return 0;
}
