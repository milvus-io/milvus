#include <iostream>
#include <vector>

#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/version.h"

int
main() {
    const std::vector<float> base = {
        0.0f, 0.0f, 0.0f, 0.0f,
        1.0f, 0.0f, 0.0f, 0.0f,
        0.0f, 1.0f, 0.0f, 0.0f,
        0.0f, 0.0f, 1.0f, 0.0f,
    };
    const std::vector<float> query = {1.0f, 0.0f, 0.0f, 0.0f};

    knowhere::Config build_cfg;
    build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
    build_cfg[knowhere::meta::DIM] = 4;
    build_cfg[knowhere::indexparam::M] = 16;
    build_cfg[knowhere::indexparam::EFCONSTRUCTION] = 64;
    build_cfg[knowhere::indexparam::EF] = 32;

    auto base_ds = knowhere::GenDataSet(4, 4, base.data());
    auto query_ds = knowhere::GenDataSet(1, 4, query.data());

    auto created = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create hnsw index\n";
        return 1;
    }

    auto index = created.value();
    if (index.Build(*base_ds, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to build hnsw index\n";
        return 2;
    }

    knowhere::Config search_cfg;
    search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
    search_cfg[knowhere::meta::TOPK] = 2;
    search_cfg[knowhere::indexparam::EF] = 32;

    auto search = index.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!search.has_value() || search.value() == nullptr ||
        search.value()->GetIds() == nullptr ||
        search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search hnsw index\n";
        return 3;
    }

    knowhere::BinarySet binary_set;
    if (index.Serialize(binary_set) != knowhere::Status::success ||
        !binary_set.Contains("index_data")) {
        std::cerr << "failed to serialize hnsw index\n";
        return 4;
    }

    auto restored_created = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!restored_created.has_value()) {
        std::cerr << "failed to create restored hnsw index\n";
        return 5;
    }

    auto restored = restored_created.value();
    if (restored.Deserialize(binary_set, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to deserialize hnsw index\n";
        return 6;
    }

    auto restored_search =
        restored.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!restored_search.has_value() || restored_search.value() == nullptr ||
        restored_search.value()->GetIds() == nullptr ||
        restored_search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search restored hnsw index\n";
        return 7;
    }

    return 0;
}
