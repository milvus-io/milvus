#include <cmath>
#include <cstdint>
#include <iostream>
#include <vector>

#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/version.h"

int
main() {
    const std::vector<knowhere::int8> base = {
        1, 0, 0, 0,
        0, 1, 0, 0,
        1, 1, 0, 0,
        0, 0, 1, 0,
    };
    const std::vector<knowhere::int8> query = {1, 1, 0, 0};

    knowhere::Config build_cfg;
    build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    build_cfg[knowhere::meta::DIM] = 4;
    build_cfg[knowhere::indexparam::M] = 16;
    build_cfg[knowhere::indexparam::EFCONSTRUCTION] = 64;
    build_cfg[knowhere::indexparam::EF] = 32;

    auto base_ds = knowhere::GenDataSet(4, 4, base.data());
    auto query_ds = knowhere::GenDataSet(1, 4, query.data());

    auto created = knowhere::IndexFactory::Instance().Create<knowhere::int8>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create int8 hnsw index\n";
        return 1;
    }

    auto index = created.value();
    if (index.Build(*base_ds, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to build int8 hnsw index\n";
        return 2;
    }

    knowhere::Config search_cfg;
    search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    search_cfg[knowhere::meta::TOPK] = 2;
    search_cfg[knowhere::indexparam::EF] = 32;

    auto search = index.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!search.has_value() || search.value() == nullptr ||
        search.value()->GetIds() == nullptr ||
        search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search int8 hnsw index\n";
        return 3;
    }
    if (search.value()->GetIds()[0] != 2 ||
        std::fabs(search.value()->GetDistance()[0] - 1.0f) > 1e-4f) {
        std::cerr << "unexpected int8 hnsw top hit id="
                  << search.value()->GetIds()[0]
                  << " distance=" << search.value()->GetDistance()[0] << "\n";
        return 4;
    }

    auto iterators = index.AnnIterator(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!iterators.has_value() || iterators.value().size() != 1 ||
        iterators.value()[0] == nullptr || !iterators.value()[0]->HasNext()) {
        std::cerr << "failed to initialize int8 hnsw iterator\n";
        return 5;
    }
    auto [iterator_id, iterator_distance] = iterators.value()[0]->Next();
    if (iterator_id != 2 || std::fabs(iterator_distance - 1.0f) > 1e-4f) {
        std::cerr << "unexpected int8 hnsw iterator top result\n";
        return 6;
    }

    knowhere::BinarySet binary_set;
    if (index.Serialize(binary_set) != knowhere::Status::success ||
        !binary_set.Contains("index_data")) {
        std::cerr << "failed to serialize int8 hnsw index\n";
        return 7;
    }

    auto restored_created = knowhere::IndexFactory::Instance().Create<knowhere::int8>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!restored_created.has_value()) {
        std::cerr << "failed to create restored int8 hnsw index\n";
        return 8;
    }

    auto restored = restored_created.value();
    if (restored.Deserialize(binary_set, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to deserialize int8 hnsw index\n";
        return 9;
    }

    auto restored_search =
        restored.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!restored_search.has_value() || restored_search.value() == nullptr ||
        restored_search.value()->GetIds() == nullptr ||
        restored_search.value()->GetDistance() == nullptr ||
        restored_search.value()->GetIds()[0] != 2 ||
        std::fabs(restored_search.value()->GetDistance()[0] - 1.0f) > 1e-4f) {
        std::cerr << "failed to search restored int8 hnsw index\n";
        return 10;
    }

    return 0;
}
