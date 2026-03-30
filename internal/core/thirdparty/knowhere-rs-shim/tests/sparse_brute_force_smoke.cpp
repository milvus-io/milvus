#include <cmath>
#include <cstdint>
#include <iostream>
#include <vector>

#include "knowhere/bitsetview.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"

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

    auto base_ds = MakeSparseDataSet(&base_rows, 8, base_ids.data());
    auto query_ds = MakeSparseDataSet(&query_rows, 8);

    knowhere::Config search_cfg;
    search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::IP;
    search_cfg[knowhere::meta::TOPK] = 3;

    std::vector<knowhere::sparse::label_t> ids(3, -9);
    std::vector<float> distances(3, -9.0f);
    auto search_status = knowhere::BruteForce::SearchSparseWithBuf(
        base_ds, query_ds, ids.data(), distances.data(), search_cfg, knowhere::BitsetView{});
    if (search_status != knowhere::Status::success) {
        std::cerr << "sparse brute-force search failed with status "
                  << static_cast<int>(search_status) << "\n";
        return 1;
    }

    if (ids[0] != 101 || ids[1] != 103 || ids[2] != -1) {
        std::cerr << "unexpected sparse brute-force ids: " << ids[0] << ", "
                  << ids[1] << ", " << ids[2] << "\n";
        return 2;
    }
    if (std::fabs(distances[0] - 1.25f) > 1e-5f ||
        std::fabs(distances[1] - 0.2f) > 1e-5f) {
        std::cerr << "unexpected sparse brute-force distances: " << distances[0]
                  << ", " << distances[1] << "\n";
        return 3;
    }

    knowhere::Config range_cfg;
    range_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::IP;
    range_cfg[knowhere::meta::RADIUS] = 0.1f;
    range_cfg[knowhere::meta::RANGE_FILTER] = 2.0f;
    range_cfg[knowhere::meta::RANGE_SEARCH_K] = 3;

    auto range_result = knowhere::BruteForce::RangeSearch<SparseRow>(
        base_ds, query_ds, range_cfg, knowhere::BitsetView{});
    if (!range_result.has_value()) {
        std::cerr << "sparse range search failed with status "
                  << static_cast<int>(range_result.error()) << "\n";
        return 4;
    }
    const auto* lims = range_result.value()->GetLims();
    const auto* range_ids = range_result.value()->GetIds();
    if (lims == nullptr || range_ids == nullptr || lims[1] != 2 ||
        range_ids[0] != 101 || range_ids[1] != 103) {
        std::cerr << "unexpected sparse range search result\n";
        return 5;
    }

    auto iterators = knowhere::BruteForce::AnnIterator<SparseRow>(
        base_ds, query_ds, search_cfg, knowhere::BitsetView{});
    if (!iterators.has_value() || iterators.value().size() != 1) {
        std::cerr << "failed to build sparse brute-force iterator\n";
        return 6;
    }
    auto& iterator = iterators.value()[0];
    if (iterator == nullptr || !iterator->HasNext()) {
        std::cerr << "sparse iterator is empty\n";
        return 7;
    }
    auto [first_id, first_distance] = iterator->Next();
    if (first_id != 101 || std::fabs(first_distance - 1.25f) > 1e-5f) {
        std::cerr << "unexpected sparse iterator top1\n";
        return 8;
    }
    if (!iterator->HasNext()) {
        std::cerr << "sparse iterator missing second hit\n";
        return 9;
    }
    auto [second_id, second_distance] = iterator->Next();
    if (second_id != 103 || std::fabs(second_distance - 0.2f) > 1e-5f) {
        std::cerr << "unexpected sparse iterator top2\n";
        return 10;
    }
    if (iterator->HasNext()) {
        std::cerr << "sparse iterator returned extra hits\n";
        return 11;
    }

    return 0;
}
