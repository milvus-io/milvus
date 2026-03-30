#include <iostream>
#include <vector>

#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/version.h"

namespace knowhere {
std::shared_ptr<IndexNode>
MakeHnswRustNode() {
    return nullptr;
}
}  // namespace knowhere

int
main() {
    const std::vector<uint8_t> base = {
        0xFF, 0x00,
        0xF0, 0x0F,
        0x00, 0xFF,
        0xAA, 0x55,
    };
    const std::vector<uint8_t> query = {0xFF, 0x00};

    knowhere::Config build_cfg;
    build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::JACCARD;
    build_cfg[knowhere::meta::DIM] = 16;
    build_cfg[knowhere::indexparam::NLIST] = 64;

    auto base_ds = knowhere::GenDataSet(4, 16, base.data());
    auto query_ds = knowhere::GenDataSet(1, 16, query.data());

    auto created = knowhere::IndexFactory::Instance().Create<knowhere::bin1>(
        knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create bin_ivf_flat index\n";
        return 1;
    }

    auto index = created.value();
    if (index.Build(*base_ds, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to build bin_ivf_flat index\n";
        return 2;
    }

    knowhere::Config search_cfg;
    search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::JACCARD;
    search_cfg[knowhere::meta::TOPK] = 2;

    auto search = index.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!search.has_value() || search.value() == nullptr ||
        search.value()->GetIds() == nullptr ||
        search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search bin_ivf_flat index\n";
        return 3;
    }
    if (search.value()->GetIds()[0] != 0) {
        std::cerr << "unexpected top result id: " << search.value()->GetIds()[0]
                  << "\n";
        return 4;
    }

    knowhere::BinarySet binary_set;
    if (index.Serialize(binary_set) != knowhere::Status::success ||
        !binary_set.Contains("index_data")) {
        std::cerr << "failed to serialize bin_ivf_flat index\n";
        return 5;
    }

    auto restored_created = knowhere::IndexFactory::Instance().Create<knowhere::bin1>(
        knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
        knowhere::Version::GetCurrentVersion());
    if (!restored_created.has_value()) {
        std::cerr << "failed to create restored bin_ivf_flat index\n";
        return 6;
    }

    auto restored = restored_created.value();
    if (restored.Deserialize(binary_set, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to deserialize bin_ivf_flat index\n";
        return 7;
    }

    auto restored_search =
        restored.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!restored_search.has_value() || restored_search.value() == nullptr ||
        restored_search.value()->GetIds() == nullptr ||
        restored_search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search restored bin_ivf_flat index\n";
        return 8;
    }
    if (restored_search.value()->GetIds()[0] != 0) {
        std::cerr << "unexpected restored top result id: "
                  << restored_search.value()->GetIds()[0] << "\n";
        return 9;
    }

    return 0;
}
