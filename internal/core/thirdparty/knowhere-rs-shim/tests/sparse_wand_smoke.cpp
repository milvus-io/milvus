#include <iostream>
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

    auto base_ds = MakeSparseDataSet(&base_rows, 8, base_ids.data());
    auto query_ds = MakeSparseDataSet(&query_rows, 8);

    auto created = knowhere::IndexFactory::Instance().Create<knowhere::sparse_u32_f32>(
        knowhere::IndexEnum::INDEX_SPARSE_WAND,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create sparse wand index\n";
        return 1;
    }

    auto index = created.value();
    const auto build_status = index.Build(*base_ds, build_cfg);
    if (build_status != knowhere::Status::success) {
        std::cerr << "failed to build sparse wand index: "
                  << knowhere::Status2String(build_status) << "\n";
        return 2;
    }

    auto search = index.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!search.has_value() || search.value() == nullptr ||
        search.value()->GetIds() == nullptr || search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search sparse wand index: status="
                  << (search.has_value() ? "success"
                                         : knowhere::Status2String(search.error()))
                  << " message=" << (search.has_value() ? "" : search.what()) << "\n";
        return 3;
    }

    if (search.value()->GetIds()[0] != 101) {
        std::cerr << "unexpected top1 id: " << search.value()->GetIds()[0] << "\n";
        return 4;
    }

    return 0;
}
